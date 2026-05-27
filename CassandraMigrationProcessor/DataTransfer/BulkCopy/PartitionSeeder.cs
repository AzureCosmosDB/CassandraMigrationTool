using Cassandra;
using CassandraMigrationProcessor.CassandraDriver;
using CassandraMigrationProcessor.Infrastructure;
using CassandraMigrationProcessor.Models;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Channels;
using System.Threading.Tasks;

namespace CassandraMigrationProcessor.DataTransfer.BulkCopy;

internal class PartitionSeeder
{
    private readonly MigrationLog _log;

    public PartitionSeeder(MigrationLog log)
    {
        _log = log;
    }

    public record SeedResult(
        Channel<Partition> Pool,
        HashSet<string> Completed,
        Dictionary<string, string?> Checkpoints,
        List<string> FeedRanges,
        int PendingCount);

    /// <summary>
    /// Discovers feed ranges, restores per-range checkpoints, and seeds
    /// the partition pool. When <paramref name="enableReplay"/> is true
    /// (online jobs), ranges previously recorded in
    /// <see cref="TableMigration.CompletedCopyFeedRanges"/> are NOT
    /// skipped — they are re-seeded in <see cref="PartitionPhase.Replay"/>
    /// with the continuation token from
    /// <see cref="TableMigration.FeedRangeContinuationTokens"/>, so the
    /// merged DataCopyWorker can resume tailing the change feed from
    /// where the previous run left off.
    /// </summary>
    public async Task<(SeedResult? Result, bool AllRangesComplete)> DiscoverAndSeedAsync(
        ISession sourceSession, TableMigration mu, TableContext context, bool enableReplay)
    {
        var feedRanges = await CassandraQueries.GetFeedRangesAsync(
            sourceSession, context.KeyspaceName, context.TableName);

        _log.WriteLine($"{context.KeyspaceName}.{context.TableName}: {feedRanges.Count} feed range(s)", LogType.Info);

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
            _log.WriteLine($"All {feedRanges.Count} ranges already completed for {context.KeyspaceName}.{context.TableName}", LogType.Info);
            return (null, AllRangesComplete: true);
        }

        int bulkCount = pendingRanges.Count(p => p.Phase == PartitionPhase.Bulk);
        int replayCount = pendingRanges.Count - bulkCount;
        _log.WriteLine(
            $"Pipeline: {bulkCount} bulk + {replayCount} replay range(s) for {context.KeyspaceName}.{context.TableName} " +
            $"({completed.Count} previously bulk-completed)",
            LogType.Info);

        var pool = Channel.CreateBounded<Partition>(new BoundedChannelOptions(pendingRanges.Count)
            { FullMode = BoundedChannelFullMode.Wait });

        int resumedCount = 0;
        foreach (var (range, phase) in pendingRanges)
        {
            byte[]? pagingState = null;
            if (phase == PartitionPhase.Replay)
            {
                // Resume replay from the per-range CF token persisted on
                // the MU; falls back to whatever was last saved in the
                // bulk-copy checkpoints dict (same value at the drain
                // handoff moment).
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
            await pool.Writer.WriteAsync(new Partition(range, pagingState, phase));
        }
        if (resumedCount > 0)
            _log.WriteLine($"Resuming {resumedCount}/{pendingRanges.Count} ranges from checkpoint", LogType.Info);

        return (new SeedResult(pool, completed, checkpoints,
            feedRanges, pendingRanges.Count), AllRangesComplete: false);
    }
}
