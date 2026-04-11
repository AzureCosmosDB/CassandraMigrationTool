using Cassandra;
using CassandraMigrationProcessor.Infrastructure;
using CassandraMigrationProcessor.CassandraDriver;
using CassandraMigrationProcessor.Models;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;

namespace CassandraMigrationProcessor.DataTransfer.BulkCopy
{
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

        public PageWriter(MigrationLog log, ConnectionOptions targetConnection, List<(string Name, string Type, string Kind, string ClusteringOrder, int Position)> columns, string targetKeyspace, string targetTable, int pageSize, int workerId, CancellationToken cancellationToken)
        {
            _log = log;
            _ct = cancellationToken;
            _workerId = workerId;
            _pageSize = pageSize;
            _targetSession = CassandraClientFactory.CreateTargetSession(log, targetConnection, "");
            var (ps, _) = CassandraQueries.PrepareInsert(_targetSession, targetKeyspace, targetTable, columns);
            _preparedInsert = ps;
        }

        public void Dispose() => MigrationUtilities.SafeDispose(_targetSession, "PageWriter target session");

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
            int writeDone = 0;
            int writeFail = 0;
            long writeLatencySum = 0;
            var writeTasks = new List<Task>(rows.Count);

            foreach (var rowValues in rows)
            {
                if (_ct.IsCancellationRequested
                    || Volatile.Read(ref ctx.Counters.FatalErrorFlag) != 0)
                    break;

                var bound = _preparedInsert.Bind(rowValues);
                bound.SetReadTimeoutMillis(WriteTimeoutMs);
                bound.SetConsistencyLevel(ConsistencyLevel.LocalOne);

                var writeStart = Stopwatch.GetTimestamp();
                writeTasks.Add(_targetSession.ExecuteAsync(bound).ContinueWith(task =>
                {
                    long elapsed = (Stopwatch.GetTimestamp()
                            - writeStart)
                        * 1000
                        / Stopwatch.Frequency;
                    Interlocked.Add(ref writeLatencySum, elapsed);

                    if (task.IsFaulted)
                    {
                        var ex = task.Exception!.InnerException!;
                        Interlocked.Increment(ref writeFail);
                        _log.WriteLine($"[W{_workerId}] INSERT failed: {ex.GetType().Name}: {ex.Message}",
                            LogType.Error);

                        if (ExceptionClassifier.IsFatal(ex))
                        {
                            _log.WriteLine($"[W{_workerId}] FATAL: {ex.GetType().Name} — failing job",
                                LogType.Error);
                            Interlocked.Exchange(ref ctx.Counters.FatalErrorFlag, 1);
                        }
                        else if (!ExceptionClassifier.IsTransient(ex))
                        {
                            Interlocked.Exchange(ref ctx.Counters.FatalErrorFlag, 1);
                        }
                    }
                    else
                    {
                        Interlocked.Increment(ref writeDone);
                    }
                }, TaskContinuationOptions.ExecuteSynchronously));
            }

            ctx.Tracker.SetPipelineState(ctx.Ranges.FeedRanges.Count
                    - ctx.Ranges.Completed.Count,
                _pageSize);
            await Task.WhenAll(writeTasks);

            // Only mark chunk completed if ALL rows succeeded.
            // Failed rows mean this page must be retried on resume.
            if (writeFail == 0) workChunk.IsCompleted = true;
            else
            {
                _log.WriteLine($"[W{_workerId}] {writeFail}/{rows.Count} writes failed — checkpoint NOT advanced (will retry on resume)",
                    LogType.Warning);
            }

            stopwatch.Stop();
            ctx.Tracker.AddWriteTime(writeLatencySum, rows.Count);
            ctx.Tracker.AddCopied(writeDone);
            ctx.Tracker.AddFailed(writeFail);

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
}


