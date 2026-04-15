using Cassandra;
using CassandraMigrationProcessor.Infrastructure;
using CassandraMigrationProcessor.CassandraDriver;
using CassandraMigrationProcessor.Models;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;

namespace CassandraMigrationProcessor.DataTransfer.BulkCopy;

/// <summary>
/// Writes extracted rows to the target Cassandra cluster
/// concurrently, tracking latency and errors.
/// </summary>
internal class PageWriter : IDisposable
{
    private readonly MigrationLog _log;
    private readonly CancellationToken _ct;
    private readonly ISession _targetSession;
    private readonly PreparedStatement _preparedInsert;
    private readonly int _workerId;
    private readonly int _pageSize;

    private const int WriteTimeoutMs = 60_000;
    private const int MaxRowRetries = 5;
    private const int RetryDelayMs = 500;

    public PageWriter(MigrationLog log, WorkerConfig config, int pageSize, int workerId, CancellationToken cancellationToken)
    {
        _log = log;
        _ct = cancellationToken;
        _workerId = workerId;
        _pageSize = pageSize;
        _targetSession = CassandraClientFactory.CreateTargetSession(log, config.TargetConnection, "");
        var (ps, _) = CassandraQueries.PrepareInsert(_targetSession, config.Context.TargetKeyspaceName, config.Context.TargetTableName, config.Columns);
        _preparedInsert = ps;
    }

    public void Dispose() => MigrationUtilities.SafeDispose(_targetSession, "PageWriter target session");

    private class WriteCounters
    {
        public int Done;
        public int Failed;
        public long LatencySum;
    }

    private async Task WriteRowAsync(BoundStatement bound, PipelineContext ctx, WriteCounters counters, int rowIndex)
    {
        for (int attempt = 1; attempt <= MaxRowRetries; attempt++)
        {
            var writeStart = Stopwatch.GetTimestamp();
            try
            {
                await _targetSession.ExecuteAsync(bound);
                long elapsed = (Stopwatch.GetTimestamp() - writeStart) * 1000 / Stopwatch.Frequency;
                Interlocked.Add(ref counters.LatencySum, elapsed);
                Interlocked.Increment(ref counters.Done);
                return; // success
            }
            catch (Exception ex)
            {
                if (ExceptionClassifier.IsFatal(ex))
                {
                    _log.WriteLine($"[W{_workerId}] FATAL row {rowIndex}: {ex.GetType().Name}: {ex.Message}",
                        LogType.Error);
                    Interlocked.Exchange(ref ctx.Counters.FatalErrorFlag, 1);
                    Interlocked.Increment(ref counters.Failed);
                    return;
                }

                if (ExceptionClassifier.IsTransient(ex) && attempt < MaxRowRetries)
                {
                    await Task.Delay(RetryDelayMs * attempt);
                    continue; // retry
                }

                // Non-transient or final retry exhausted
                Interlocked.Increment(ref counters.Failed);
                _log.WriteLine($"[W{_workerId}] Row {rowIndex} FAILED after {attempt} attempt(s): {ex.GetType().Name}: {ex.Message}",
                    LogType.Error);

                if (!ExceptionClassifier.IsTransient(ex))
                {
                    Interlocked.Exchange(ref ctx.Counters.FatalErrorFlag, 1);
                }
                return;
            }
        }
    }

    /// <summary>
    /// Writes extracted rows to the target cluster in
    /// parallel, tracking progress and handling errors.
    /// </summary>
    public async Task WriteAsync(List<object[]> rows,
        WorkChunk workChunk,
        PipelineContext ctx)
    {
        if (rows.Count == 0)
        {
            workChunk.IsCompleted = true;
            return;
        }

        var stopwatch = Stopwatch.StartNew();
        var counters = new WriteCounters();
        var writeTasks = new List<Task>(rows.Count);

        for (int i = 0; i < rows.Count; i++)
        {
            if (_ct.IsCancellationRequested
                || Volatile.Read(ref ctx.Counters.FatalErrorFlag) != 0)
                break;

            var bound = _preparedInsert.Bind(rows[i]);
            bound.SetReadTimeoutMillis(WriteTimeoutMs);
            bound.SetConsistencyLevel(ConsistencyLevel.LocalOne);

            writeTasks.Add(WriteRowAsync(bound, ctx, counters, i));
        }

        ctx.Tracker.SetPipelineState(ctx.Ranges.FeedRanges.Count
                - ctx.Ranges.Completed.Count,
            _pageSize);
        await Task.WhenAll(writeTasks);

        // Only mark chunk completed if ALL rows succeeded.
        // Failed rows mean this page must be retried on resume.
        if (counters.Failed == 0) workChunk.IsCompleted = true;
        else
        {
            _log.WriteLine($"[W{_workerId}] {counters.Failed}/{rows.Count} writes failed — checkpoint NOT advanced (will retry on resume)",
                LogType.Warning);
        }

        stopwatch.Stop();
        ctx.Tracker.AddWriteTime(counters.LatencySum, rows.Count);
        ctx.Tracker.AddCopied(counters.Done);
        ctx.Tracker.AddFailed(counters.Failed);

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
        ctx.Tracker.AddBytes(pageBytes);
    }
}
