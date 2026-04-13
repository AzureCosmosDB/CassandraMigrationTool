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
    /// Discovers feed ranges, filters completed, restores
    /// checkpoints, and seeds the partition pool channel.
    /// </summary>
    public async Task<(SeedResult? Result, bool AllRangesComplete)> DiscoverAndSeedAsync(
        ISession sourceSession, TableMigration mu, TableContext context)
    {
        var feedRanges = await CassandraQueries.GetFeedRangesAsync(
            sourceSession, context.KeyspaceName, context.TableName);

        _log.WriteLine($"{context.KeyspaceName}.{context.TableName}: {feedRanges.Count} feed range(s)", LogType.Info);

        var completed = mu.CompletedCopyFeedRanges;
        var checkpoints = mu.CopyFeedRangeCheckpoints;

        List<string> pendingRanges;
        lock (checkpoints)
        {
            pendingRanges = feedRanges.Where(r => !completed.Contains(r)).ToList();
        }

        if (pendingRanges.Count == 0)
        {
            _log.WriteLine($"All {feedRanges.Count} ranges already completed for {context.KeyspaceName}.{context.TableName}", LogType.Info);
            return (null, AllRangesComplete: true);
        }

        _log.WriteLine($"Pipeline copy: {pendingRanges.Count} ranges ({completed.Count} already done) for {context.KeyspaceName}.{context.TableName}", LogType.Info);

        var pool = Channel.CreateBounded<Partition>(new BoundedChannelOptions(pendingRanges.Count)
            { FullMode = BoundedChannelFullMode.Wait });

        int resumedCount = 0;
        foreach (var range in pendingRanges)
        {
            byte[]? pagingState = null;
            if (checkpoints.TryGetValue(range, out var base64Token) && base64Token != null)
            {
                pagingState = Convert.FromBase64String(base64Token);
                resumedCount++;
            }
            await pool.Writer.WriteAsync(new Partition(range, pagingState));
        }
        if (resumedCount > 0)
            _log.WriteLine($"Resuming {resumedCount}/{pendingRanges.Count} ranges from checkpoint", LogType.Info);

        return (new SeedResult(pool, completed, checkpoints, feedRanges, pendingRanges.Count), AllRangesComplete: false);
    }
}
