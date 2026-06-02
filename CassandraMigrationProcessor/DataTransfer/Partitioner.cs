using System.Collections.Concurrent;
using Cassandra;
using CassandraMigrationProcessor.CassandraDriver;
using CassandraMigrationProcessor.Infrastructure;
using CassandraMigrationProcessor.Models;

namespace CassandraMigrationProcessor.DataTransfer;

/// <summary>
/// Plans the still-pending partitions for a single migration unit. Two
/// responsibilities, intentionally kept in one place because the second
/// depends on the first and nothing else calls them independently:
///   1. Reconcile the table's persisted <see cref="PartitionSnapshot"/>
///      dictionary with the source's current feed-range list — adds new
///      source ranges as fresh snapshots, never drops stored ranges so a
///      source-side partition merge does not discard already-copied
///      progress. Per-table feed-range list is cached for this
///      partitioner instance's lifetime so repeated planning passes do
///      not re-query the source.
///   2. Project the reconciled feed-range list into a <see cref="Plan"/>
///      of <see cref="PendingPartition"/> descriptors that the caller
///      materializes into runtime <see cref="Partition"/> instances.
/// </summary>
internal sealed class Partitioner
{
    private readonly MigrationLog _log;
    private readonly ConcurrentDictionary<string, IReadOnlyList<string>> _feedRangeCache = new();

    public Partitioner(MigrationLog log)
    {
        _log = log;
    }

    /// <summary>
    /// Materialization descriptor for one pending partition. Carries
    /// everything <see cref="Partition"/>'s ctor needs except the
    /// per-table <see cref="TableResources"/>.
    /// </summary>
    public readonly record struct PendingPartition(
        PartitionSnapshot Snapshot,
        byte[]? InitialPagingState,
        PartitionPhase Phase);

    /// <summary>
    /// Outcome of one <see cref="PartitionAsync"/> call.
    /// <see cref="TotalFeedRanges"/> is the union of stored snapshots
    /// and source ranges. <see cref="AlreadyCompletedCount"/> is the
    /// number of stored snapshots whose bulk phase is already done;
    /// the caller raises that many
    /// <see cref="TableResources.OnPartitionBulkCompleted"/> calls.
    /// </summary>
    public readonly record struct Plan(
        int TotalFeedRanges,
        int AlreadyCompletedCount,
        IReadOnlyList<PendingPartition> PendingPartitions,
        bool AllRangesAlreadyComplete);

    public async Task<Plan> PartitionAsync(
        ISession sourceSession,
        TableMigration mu,
        bool enableReplay)
    {
        var feedRanges = await ReconcileFeedRangesAsync(sourceSession, mu);

        int alreadyCompleted = mu.Partitions.Values.Count(p => p.BulkCompleted);

        var pending = new List<PendingPartition>(feedRanges.Count);
        int resumedCount = 0;
        foreach (var range in feedRanges)
        {
            var state = mu.Partitions[range];
            if (!enableReplay && state.BulkCompleted) continue;

            var phase = state.BulkCompleted ? PartitionPhase.Replay : PartitionPhase.Bulk;
            byte[]? pagingState = !string.IsNullOrEmpty(state.ContinuationToken)
                ? Convert.FromBase64String(state.ContinuationToken)
                : null;
            if (pagingState != null || phase == PartitionPhase.Replay)
                resumedCount++;
            pending.Add(new PendingPartition(state, pagingState, phase));
        }

        if (pending.Count == 0)
        {
            _log.WriteLine($"All {feedRanges.Count} partitions already completed for {mu.KeyspaceName}.{mu.TableName}", LogType.Info);
            return new Plan(feedRanges.Count, alreadyCompleted, Array.Empty<PendingPartition>(), AllRangesAlreadyComplete: true);
        }

        _log.WriteLine(
            $"Pipeline: {pending.Count} partition(s) for {mu.KeyspaceName}.{mu.TableName}",
            LogType.Info);

        if (resumedCount > 0)
            _log.WriteLine($"Resuming {resumedCount}/{pending.Count} partitions from snapshot for {mu.KeyspaceName}.{mu.TableName}", LogType.Info);

        return new Plan(feedRanges.Count, alreadyCompleted, pending, AllRangesAlreadyComplete: false);
    }

    private async Task<IReadOnlyList<string>> ReconcileFeedRangesAsync(ISession sourceSession, TableMigration mu)
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
                    mu.Partitions[range] = new PartitionSnapshot { FeedRange = range };
                    addedFromSource++;
                }
            }
            working = mu.Partitions.Keys.ToList();
        }

        LogReconciliation(mu, working.Count, storedBefore, addedFromSource);

        _feedRangeCache.TryAdd(cacheKey, working);
        return working;
    }

    private void LogReconciliation(TableMigration mu, int total, int storedBefore, int addedFromSource)
    {
        if (storedBefore == 0)
        {
            _log.WriteLine(
                $"{mu.KeyspaceName}.{mu.TableName}: {total} feed range(s) from source",
                LogType.Info);
        }
        else if (addedFromSource > 0)
        {
            _log.WriteLine(
                $"{mu.KeyspaceName}.{mu.TableName}: {total} feed range(s) ({storedBefore} resumed, {addedFromSource} new from source)",
                LogType.Info);
        }
        else
        {
            _log.WriteLine(
                $"{mu.KeyspaceName}.{mu.TableName}: {total} feed range(s) (resumed from snapshot, source matches)",
                LogType.Info);
        }
    }
}
