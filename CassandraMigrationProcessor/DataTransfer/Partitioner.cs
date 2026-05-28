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
    /// Seed every still-pending range from <paramref name="resources"/>
    /// into <paramref name="partitions"/>. Returns <c>true</c> if there
    /// was nothing to seed (every range already completed) — in that
    /// case the caller can short-circuit. The table's
    /// <see cref="TableResources.BulkDrainSignal"/> is tripped here
    /// when there is no bulk work to do.
    /// </summary>
    public async Task<bool> SeedAsync(
        TableResources resources,
        TableMigration mu,
        PartitionManager partitions,
        bool enableReplay)
    {
        var spec = resources.Spec;
        var feedRanges = resources.Ranges.FeedRanges;
        var completed = mu.CompletedCopyFeedRanges;
        var checkpoints = mu.CopyFeedRangeCheckpoints;

        List<(string Range, PartitionPhase Phase)> pendingRanges;
        lock (checkpoints)
        {
            pendingRanges = feedRanges
                .Where(r => enableReplay || !completed.Contains(r))
                .Select(r => (Range: r,
                    Phase: completed.Contains(r) ? PartitionPhase.Replay : PartitionPhase.Bulk))
                .ToList();
        }

        if (pendingRanges.Count == 0)
        {
            _log.WriteLine($"All {feedRanges.Count} ranges already completed for {spec.KeyspaceName}.{spec.TableName}", LogType.Info);
            resources.BulkDrainSignal.TrySetResult();
            return true;
        }

        int bulkCount = pendingRanges.Count(p => p.Phase == PartitionPhase.Bulk);
        int replayCount = pendingRanges.Count - bulkCount;
        _log.WriteLine(
            $"Pipeline: {bulkCount} bulk + {replayCount} replay range(s) for {spec.KeyspaceName}.{spec.TableName} " +
            $"({completed.Count} previously bulk-completed)",
            LogType.Info);

        int resumedCount = 0;
        foreach (var (range, phase) in pendingRanges)
        {
            byte[]? pagingState = null;
            if (phase == PartitionPhase.Replay)
            {
                lock (mu.FeedRangeContinuationTokens)
                {
                    if (mu.FeedRangeContinuationTokens.TryGetValue(range, out var cfToken)
                        && !string.IsNullOrEmpty(cfToken))
                    {
                        pagingState = Convert.FromBase64String(cfToken);
                    }
                }
                if (pagingState == null
                    && checkpoints.TryGetValue(range, out var bulkToken)
                    && bulkToken != null)
                {
                    pagingState = Convert.FromBase64String(bulkToken);
                }
                resumedCount++;
            }
            else if (checkpoints.TryGetValue(range, out var base64Token) && base64Token != null)
            {
                pagingState = Convert.FromBase64String(base64Token);
                resumedCount++;
            }
            await partitions.SeedAsync(new Partition(range, pagingState, resources, phase));
        }
        if (resumedCount > 0)
            _log.WriteLine($"Resuming {resumedCount}/{pendingRanges.Count} ranges from checkpoint for {spec.KeyspaceName}.{spec.TableName}", LogType.Info);

        return false;
    }
}
