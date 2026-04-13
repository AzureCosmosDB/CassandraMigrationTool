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
        int PendingCount);

    public async Task<(SeedResult? Result, bool AllRangesComplete)> SeedAsync(PipelineRequest request)
    {
        var mu = request.TableMigration;
        var ctx0 = request.Context;
        var completed = mu.CompletedCopyFeedRanges;
        var checkpoints = mu.CopyFeedRangeCheckpoints;

        List<string> pendingRanges;
        lock (checkpoints)
        {
            pendingRanges = request.FeedRanges.Where(r => !completed.Contains(r)).ToList();
        }

        if (pendingRanges.Count == 0)
        {
            _log.WriteLine($"All {request.FeedRanges.Count} ranges already completed for {ctx0.KeyspaceName}.{ctx0.TableName}", LogType.Info);
            return (null, AllRangesComplete: true);
        }

        _log.WriteLine($"Pipeline copy: {pendingRanges.Count} ranges ({completed.Count} already done) for {ctx0.KeyspaceName}.{ctx0.TableName}", LogType.Info);

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

        return (new SeedResult(pool, completed, checkpoints, pendingRanges.Count), AllRangesComplete: false);
    }
}
