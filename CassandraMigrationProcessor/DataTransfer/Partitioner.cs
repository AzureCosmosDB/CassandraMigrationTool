using CassandraMigrationProcessor.Infrastructure;
using CassandraMigrationProcessor.Models;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace CassandraMigrationProcessor.DataTransfer;

/// <summary>
/// Slices a table's feed-range list into <see cref="Partition"/>s and
/// seeds them into the job-shared <see cref="PartitionManager"/>. Owns
/// no per-table state — the caller supplies a fully-built
/// <see cref="TableResources"/>; the Partitioner only decides which
/// ranges are still pending and what paging state to resume from.
/// </summary>
internal class Partitioner
{
    private readonly MigrationLog _log;

    public Partitioner(MigrationLog log)
    {
        _log = log;
    }

    /// <summary>
    /// Seed every still-pending range from <paramref name="feedRanges"/>
    /// into <paramref name="partitions"/>, allocating a
    /// <see cref="PartitionState"/> on <paramref name="mu"/> for any
    /// range that does not already have one. Returns <c>true</c> if
    /// every range is already bulk-completed and there is nothing to
    /// seed (offline-only) — in that case the caller can short-circuit.
    /// </summary>
    public async Task<bool> SeedAsync(
        TableResources resources,
        TableMigration mu,
        List<string> feedRanges,
        PartitionManager partitions,
        bool enableReplay)
    {
        var spec = resources.Spec;

        // Ensure every range has a persisted state entry. Lock the dict
        // because Partitioner.SeedAsync runs once per table on the
        // coordinator, but the dict object is shared with hot-path
        // writers via Partition.State references. Adds happen here only.
        lock (mu.Partitions)
        {
            foreach (var range in feedRanges)
            {
                if (!mu.Partitions.ContainsKey(range))
                    mu.Partitions[range] = new PartitionState { FeedRange = range };
            }
        }

        // Restore the bulk-completed counter on resume so PageWriter's
        // ETA reads a correct "remaining ranges" count immediately, and
        // the table-wide BulkDrainSignal trips automatically if every
        // range was already finished in a prior run. Going through the
        // single OnPartitionBulkCompleted entry point keeps signal
        // semantics consistent with runtime partition transitions.
        int alreadyCompleted = mu.Partitions.Values.Count(p => p.BulkCompleted);
        for (int i = 0; i < alreadyCompleted; i++)
            resources.OnPartitionBulkCompleted();

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
            _log.WriteLine($"All {feedRanges.Count} ranges already completed for {spec.KeyspaceName}.{spec.TableName}", LogType.Info);
            return true;
        }

        int bulkCount = pendingRanges.Count(p => p.Phase == PartitionPhase.Bulk);
        int replayCount = pendingRanges.Count - bulkCount;
        _log.WriteLine(
            $"Pipeline: {bulkCount} bulk + {replayCount} replay range(s) for {spec.KeyspaceName}.{spec.TableName} " +
            $"({alreadyCompleted} previously bulk-completed)",
            LogType.Info);

        int resumedCount = 0;
        foreach (var (range, state, phase) in pendingRanges)
        {
            byte[]? pagingState = !string.IsNullOrEmpty(state.ContinuationToken)
                ? Convert.FromBase64String(state.ContinuationToken)
                : null;
            if (pagingState != null || phase == PartitionPhase.Replay)
                resumedCount++;
            await partitions.SeedAsync(new Partition(state, pagingState, resources, phase));
        }
        if (resumedCount > 0)
            _log.WriteLine($"Resuming {resumedCount}/{pendingRanges.Count} ranges from checkpoint for {spec.KeyspaceName}.{spec.TableName}", LogType.Info);

        return false;
    }
}
