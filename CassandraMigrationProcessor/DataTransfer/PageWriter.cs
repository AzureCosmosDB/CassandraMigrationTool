using Cassandra;
using CassandraMigrationProcessor.Infrastructure;
using CassandraMigrationProcessor.CassandraDriver;
using CassandraMigrationProcessor.Models;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace CassandraMigrationProcessor.DataTransfer;

/// <summary>
/// Writes extracted rows to the target cluster. The target session is
/// keyspace-agnostic; per-table prepared statements and write strategies
/// are lazily built and cached per table on first encounter. A single
/// worker can therefore service partitions from any table without
/// rebuilding state per page.
/// </summary>
internal sealed class PageWriter : IDisposable
{
    private readonly WorkerLog _log;
    private readonly CancellationToken _ct;
    private readonly ISession _targetSession;
    private readonly int _pageSize;
    private readonly int _maxWriteRetries;
    private readonly WorkerConfig _config;

    private readonly ConcurrentDictionary<string, Task<IRowWriteStrategy>> _strategyCache = new();
    private readonly ConcurrentDictionary<string, Task> _udtRegistrations = new();

    private PageWriter(WorkerLog log, WorkerConfig config, ISession targetSession,
        int pageSize, int maxWriteRetries, CancellationToken cancellationToken)
    {
        _log = log;
        _ct = cancellationToken;
        _config = config;
        _pageSize = pageSize;
        _maxWriteRetries = maxWriteRetries;
        _targetSession = targetSession;
    }

    public static async Task<PageWriter> CreateAsync(WorkerLog log, WorkerConfig config, int pageSize, int maxWriteRetries, CancellationToken cancellationToken)
    {
        var targetSession = await CassandraClientFactory.CreateTargetSessionAsync(log.Inner, config.Job, string.Empty);
        return new PageWriter(log, config, targetSession, pageSize, maxWriteRetries, cancellationToken);
    }

    public void Dispose() => MigrationUtilities.SafeDispose(_targetSession, "PageWriter target session");

    private Task<IRowWriteStrategy> GetStrategyAsync(TableResources resources)
    {
        return _strategyCache.GetOrAdd(resources.TableId, async _ =>
        {
            await EnsureTargetUdtsRegisteredAsync(resources);
            return await RowWriteStrategyFactory.CreateAsync(
                _log, _targetSession, resources.Columns,
                resources.Spec.TargetKeyspaceName, resources.Spec.TargetTableName,
                _maxWriteRetries);
        });
    }

    private Task EnsureTargetUdtsRegisteredAsync(TableResources resources)
    {
        return _udtRegistrations.GetOrAdd(resources.Spec.TargetKeyspaceName, async ks =>
        {
            ISession? sourceSession = null;
            try
            {
                sourceSession = CassandraClientFactory.CreateSourceSession(_log.Inner, _config.Job, string.Empty, _config.TokenRefreshManager);
                var allUdts = await SchemaManager.GetUserDefinedTypesAsync(sourceSession, resources.Spec.KeyspaceName);
                var requiredUdts = SchemaManager.FilterUdtsReferencedByTable(
                    allUdts, resources.Columns.Select(c => c.Type));
                await DynamicUdtRegistrar.RegisterAsync(_targetSession, ks, requiredUdts);
            }
            catch (Exception ex)
            {
                _log.WriteLine($"UDT mapping registration on target failed for {ks}: {ex.Message}", LogType.Warning);
            }
            finally
            {
                if (sourceSession != null)
                    MigrationUtilities.SafeDispose(sourceSession, "PageWriter UDT discovery session");
            }
        });
    }

    public async Task WriteAsync(List<object[]> rows,
        WorkChunk workChunk,
        Partition partition,
        PipelineContext ctx)
    {
        if (rows.Count == 0)
        {
            workChunk.IsCompleted = true;
            return;
        }

        var resources = partition.Resources;
        var strategy = await GetStrategyAsync(resources);

        var stopwatch = Stopwatch.StartNew();
        var counters = new WriteCounters();
        var writeTasks = new List<Task>(rows.Count);
        Action onFatal = () => Interlocked.Exchange(ref ctx.Flags.FatalErrorFlag, 1);

        for (int i = 0; i < rows.Count; i++)
        {
            if (_ct.IsCancellationRequested
                || Volatile.Read(ref ctx.Flags.FatalErrorFlag) != 0)
                break;

            writeTasks.Add(strategy.WriteRowAsync(rows[i], onFatal, counters, i));
        }

        resources.Tracker.SetPipelineState(resources.Ranges.FeedRanges.Count
                - resources.Ranges.Completed.Count,
            _pageSize);
        await Task.WhenAll(writeTasks);

        if (counters.Failed == 0) workChunk.IsCompleted = true;
        else
        {
            _log.WriteLine($"{counters.Failed}/{rows.Count} writes failed for {resources.TableId} — checkpoint NOT advanced (will retry on resume)",
                LogType.Warning);
        }

        stopwatch.Stop();
        resources.Tracker.AddWriteTime(counters.LatencySum, rows.Count);

        if (partition.Phase == PartitionPhase.Replay)
        {
            resources.Tracker.AddReplayApplied(counters.Done, counters.LatencySum);
            if (counters.Failed > 0)
                resources.Tracker.AddReplayErrors(counters.Failed);
        }
        else
        {
            resources.Tracker.AddCopied(counters.Done);
            resources.Tracker.AddFailed(counters.Failed);
        }

        long pageBytes = 0;
        foreach (var r in rows)
            foreach (var v in r)
            {
                if (v is byte[] b)
                    pageBytes += b.Length;
                else if (v is string s)
                    pageBytes += s.Length * 2;
                else if (v != null)
                    pageBytes += 8;
            }
        resources.Tracker.AddBytes(pageBytes);
    }
}
