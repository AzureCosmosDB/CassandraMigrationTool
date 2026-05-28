using Cassandra;
using CassandraMigrationProcessor.CassandraDriver;
using CassandraMigrationProcessor.Infrastructure;
using CassandraMigrationProcessor.Models;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Channels;
using System.Threading.Tasks;

namespace CassandraMigrationProcessor.DataTransfer;

internal class Partitioner
{
    private readonly MigrationLog _log;

    public Partitioner(MigrationLog log)
    {
        _log = log;
    }

    public record SeedResult(
        TableResources Resources,
        bool AllRangesComplete);

    /// <summary>
    /// Discovers feed ranges, restores per-range checkpoints, builds the
    /// table's <see cref="TableResources"/>, and seeds the resulting
    /// partitions into the job-shared <paramref name="partitions"/>.
    /// </summary>
    public async Task<SeedResult> DiscoverAndSeedAsync(
        ISession sourceSession, TableMigration mu, TableCopySpec context,
        List<CassandraColumn> columns,
        CopyProgressTracker tracker,
        PartitionManager partitions,
        bool enableReplay)
    {
        var feedRanges = await CassandraQueries.GetFeedRangesAsync(
            sourceSession, context.KeyspaceName, context.TableName);

        _log.WriteLine($"{context.KeyspaceName}.{context.TableName}: {feedRanges.Count} feed range(s)", LogType.Info);

        var completed = mu.CompletedCopyFeedRanges;
        var checkpoints = mu.CopyFeedRangeCheckpoints;
        var ranges = new RangeState(completed, checkpoints, feedRanges);
        var resources = new TableResources(context, columns, tracker, ranges);

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
            _log.WriteLine($"All {feedRanges.Count} ranges already completed for {context.KeyspaceName}.{context.TableName}", LogType.Info);
            // Mark drain immediately so callers don't wait.
            resources.BulkDrainSignal.TrySetResult();
            return new SeedResult(resources, AllRangesComplete: true);
        }

        int bulkCount = pendingRanges.Count(p => p.Phase == PartitionPhase.Bulk);
        int replayCount = pendingRanges.Count - bulkCount;
        _log.WriteLine(
            $"Pipeline: {bulkCount} bulk + {replayCount} replay range(s) for {context.KeyspaceName}.{context.TableName} " +
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
            _log.WriteLine($"Resuming {resumedCount}/{pendingRanges.Count} ranges from checkpoint for {context.KeyspaceName}.{context.TableName}", LogType.Info);

        return new SeedResult(resources, AllRangesComplete: false);
    }
}
