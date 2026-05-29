using Cassandra;
using CassandraMigrationProcessor.CassandraDriver;
using CassandraMigrationProcessor.Infrastructure;
using CassandraMigrationProcessor.Models;

namespace CassandraMigrationProcessor.DataTransfer;

/// <summary>
/// Single owner of "what feed ranges does this table have, and which
/// of them still need work". Given a source session, the partitioner:
/// <list type="number">
///   <item>Queries the source for its current feed-range list.</item>
///   <item>Reconciles against any persisted snapshots already on
///         <see cref="TableMigration.Partitions"/> — existing snapshots
///         are kept (their progress survives resume); any source range
///         not yet stored gets a fresh snapshot.</item>
///   <item>Returns a <see cref="Result"/> with the total range count,
///         the already-bulk-completed count, and a blueprint per still
///         pending range. The caller constructs
///         <see cref="TableResources"/> from the count and materializes
///         <see cref="Partition"/> objects from the blueprints — that
///         keeps Partitioner free of <see cref="TableResources"/> /
///         <see cref="Partition"/> construction concerns.</item>
/// </list>
/// Per-table feed-range list is cached for the lifetime of this
/// partitioner instance so multi-chunk tables do not re-query the
/// source. Does not touch the <see cref="PartitionManager"/> — the
/// job-init phase collects every table's partition list into a
/// <see cref="JobPartitioning"/> and hands the flattened result to the
/// pool's constructor.
/// </summary>
internal class Partitioner
{
    private readonly MigrationLog _log;
    private readonly Dictionary<string, IReadOnlyList<string>> _feedRangeCache = new();

    public Partitioner(MigrationLog log)
    {
        _log = log;
    }

    /// <summary>
    /// Materialization blueprint for one pending partition. Carries
    /// everything <see cref="Partition"/>'s ctor needs except the
    /// per-table <see cref="TableResources"/> — that's owned by the
    /// caller, which constructs <see cref="TableResources"/> once per
    /// (table, chunk) and then wraps each blueprint into a
    /// <see cref="Partition"/>.
    /// </summary>
    public readonly record struct PartitionBlueprint(
        Partition.PartitionSnapshot Snapshot,
        byte[]? InitialPagingState,
        PartitionPhase Phase);

    /// <summary>
    /// Outcome of one <see cref="PartitionAsync"/> call.
    /// <see cref="TotalFeedRanges"/> is the union of stored snapshots
    /// and source ranges; the caller passes it to
    /// <see cref="TableResources"/>'s ctor.
    /// <see cref="AlreadyCompletedCount"/> is the number of stored
    /// snapshots whose bulk phase is already done; the caller must
    /// raise that many <see cref="TableResources.OnPartitionBulkCompleted"/>
    /// calls so the per-table drain signal trips correctly on resume.
    /// </summary>
    public readonly record struct Result(
        int TotalFeedRanges,
        int AlreadyCompletedCount,
        IReadOnlyList<PartitionBlueprint> PendingPartitions,
        bool AllRangesAlreadyComplete);

    /// <summary>
    /// Discover feed-ranges from the source, reconcile with persisted
    /// snapshots, and return blueprints for every still-pending range.
    /// <see cref="Result.AllRangesAlreadyComplete"/> is <c>true</c>
    /// when every range is already bulk-completed and there is nothing
    /// to enqueue (offline-only); the caller can short-circuit.
    /// </summary>
    public async Task<Result> PartitionAsync(
        ISession sourceSession,
        TableMigration mu,
        bool enableReplay)
    {
        var feedRanges = await DiscoverFeedRangesAsync(sourceSession, mu);

        int alreadyCompleted = mu.Partitions.Values.Count(p => p.BulkCompleted);

        var pendingRanges = feedRanges
            .Select(r => (Range: r, State: mu.Partitions[r]))
            .Where(t => enableReplay || !t.State.BulkCompleted)
            .Select(t => (
                t.Range,
                t.State,
                Phase: t.State.BulkCompleted ? PartitionPhase.Replay : PartitionPhase.Bulk))
            .ToList();

        if (pendingRanges.Count == 0)
        {
            _log.WriteLine($"All {feedRanges.Count} ranges already completed for {mu.KeyspaceName}.{mu.TableName}", LogType.Info);
            return new Result(feedRanges.Count, alreadyCompleted, Array.Empty<PartitionBlueprint>(), AllRangesAlreadyComplete: true);
        }

        int bulkCount = pendingRanges.Count(p => p.Phase == PartitionPhase.Bulk);
        int replayCount = pendingRanges.Count - bulkCount;
        _log.WriteLine(
            $"Pipeline: {bulkCount} bulk + {replayCount} replay range(s) for {mu.KeyspaceName}.{mu.TableName} " +
            $"({alreadyCompleted} previously bulk-completed)",
            LogType.Info);

        int resumedCount = 0;
        var blueprints = new List<PartitionBlueprint>(pendingRanges.Count);
        foreach (var (range, state, phase) in pendingRanges)
        {
            byte[]? pagingState = !string.IsNullOrEmpty(state.ContinuationToken)
                ? Convert.FromBase64String(state.ContinuationToken)
                : null;
            if (pagingState != null || phase == PartitionPhase.Replay)
                resumedCount++;
            blueprints.Add(new PartitionBlueprint(state, pagingState, phase));
        }
        if (resumedCount > 0)
            _log.WriteLine($"Resuming {resumedCount}/{pendingRanges.Count} ranges from checkpoint for {mu.KeyspaceName}.{mu.TableName}", LogType.Info);

        return new Result(feedRanges.Count, alreadyCompleted, blueprints, AllRangesAlreadyComplete: false);
    }

    /// <summary>
    /// Source-of-truth feed-range discovery: queries the source and
    /// reconciles against any snapshots already persisted on
    /// <paramref name="mu"/>. Result is cached per-table for the
    /// lifetime of this partitioner so multi-chunk tables don't
    /// re-query. New snapshots added here are flushed by the caller's
    /// <c>SaveMigrationUnit</c> after the chunk loop, so they survive
    /// crashes after discovery.
    /// </summary>
    /// <remarks>
    /// Existing snapshots are never dropped, even if the source no
    /// longer reports a given range — partition merges on the source
    /// side would otherwise discard already-copied progress, and the
    /// stored range is still a valid input to BuildSelectCql. Adds
    /// hold the per-table lock because <see cref="TableMigration.Partitions"/>
    /// is shared with hot-path writers via <see cref="Partition.Snapshot"/>
    /// references.
    /// </remarks>
    private async Task<IReadOnlyList<string>> DiscoverFeedRangesAsync(
        ISession sourceSession,
        TableMigration mu)
    {
        string cacheKey = string.IsNullOrEmpty(mu.Id) ? $"{mu.KeyspaceName}.{mu.TableName}" : mu.Id;
        if (_feedRangeCache.TryGetValue(cacheKey, out var cached))
            return cached;

        int storedBefore;
        lock (mu.Partitions)
            storedBefore = mu.Partitions.Count;

        var sourceRanges = await CassandraQueries.GetFeedRangesAsync(
            sourceSession, mu.KeyspaceName, mu.TableName);

        int addedFromSource = 0;
        IReadOnlyList<string> working;
        lock (mu.Partitions)
        {
            foreach (var range in sourceRanges)
            {
                if (!mu.Partitions.ContainsKey(range))
                {
                    mu.Partitions[range] = new Partition.PartitionSnapshot { FeedRange = range };
                    addedFromSource++;
                }
            }
            working = mu.Partitions.Keys.ToList();
        }

        if (storedBefore == 0)
        {
            _log.WriteLine(
                $"{mu.KeyspaceName}.{mu.TableName}: {working.Count} feed range(s) from source",
                LogType.Info);
        }
        else if (addedFromSource > 0)
        {
            _log.WriteLine(
                $"{mu.KeyspaceName}.{mu.TableName}: {working.Count} feed range(s) ({storedBefore} resumed, {addedFromSource} new from source)",
                LogType.Info);
        }
        else
        {
            _log.WriteLine(
                $"{mu.KeyspaceName}.{mu.TableName}: {working.Count} feed range(s) (resumed from snapshot, source matches)",
                LogType.Info);
        }

        _feedRangeCache[cacheKey] = working;
        return working;
    }
}
