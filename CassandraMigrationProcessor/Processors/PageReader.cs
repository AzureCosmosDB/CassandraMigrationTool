using Cassandra;
using CassandraMigrationProcessor.Helpers;
using CassandraMigrationProcessor.Helpers.Cassandra;
using CassandraMigrationProcessor.Models;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;

namespace CassandraMigrationProcessor.Processors
{
    /// <summary>
    /// Reads a single page from the source Cassandra cluster,
    /// extracting row values for downstream writing.
    /// </summary>
    internal class PageReader : IDisposable
    {
        private readonly MigrationLog _log;
        private readonly CancellationTokenSource _cancellation;
        private readonly ISession _sourceSession;
        private readonly int _workerId;

        private const int ReadTimeoutMs = 60_000;
        private const int MaxReadRetries = 3;
        private const int RetryDelayMs = 5000;

        public PageReader(MigrationLog log,
            ConnectionOptions sourceConnection, string keyspace, int workerId,
            CancellationTokenSource cancellation)
        {
            _log = log;
            _cancellation = cancellation;
            _workerId = workerId;
            _sourceSession = CassandraClientFactory.CreateSourceSession(log, sourceConnection, keyspace);
        }

        public void Dispose() => MigrationHelper.SafeDispose(_sourceSession, "PageReader source session");

        /// <summary>Result of a page read attempt.</summary>
        internal class ReadResult
        {
            public List<object[]> Rows { get; init; } = new();
            public CopyProcessor.WorkChunk? WorkChunk { get; init; }
            public bool IsLastPage { get; init; }
        }

        /// <summary>
        /// Reads a single page, updates partition state and tracker.
        /// </summary>
        public async Task<ReadResult?>
            ReadAsync(CopyProcessor.Partition partition, CopyProcessor.PipelineContext ctx)
        {
            var stopwatch = Stopwatch.StartNew();
            var stmt = new SimpleStatement(BuildSelectCql(ctx.Context, partition.FeedRange));
            stmt.SetPageSize(ctx.ConfiguredPageSize);
            stmt.SetAutoPage(false);
            stmt.SetReadTimeoutMillis(ReadTimeoutMs);
            stmt.SetConsistencyLevel(ConsistencyLevel.One);

            if (partition.LastPagingState != null)
                stmt.SetPagingState(partition.LastPagingState);

            RowSet resultSet = null;
            for (int attempt = 1; attempt <= MaxReadRetries; attempt++)
            {
                try
                {
                    resultSet = await _sourceSession.ExecuteAsync(stmt);
                    break;
                }
                catch (System.Exception ex) when (attempt < MaxReadRetries
                    && ExceptionClassifier.IsTransient(ex))
                {
                    _log.WriteLine($"[W{_workerId}] Read timeout (attempt {attempt}/{MaxReadRetries})",
                        LogType.Warning);
                    await Task.Delay(attempt * RetryDelayMs, _cancellation.Token);
                }
            }

            if (resultSet == null)
            {
                ctx.WorkerErrors.Add(TaskResult.Retry);
                return null;
            }

            byte[]? nextPaging = resultSet.PagingState;
            var rows = new List<object[]>();
            int available = resultSet.GetAvailableWithoutFetching();
            int consumed = 0;
            foreach (var row in resultSet)
            {
                if (consumed >= available) break;
                consumed++;
                var rowValues = new object[ctx.ColumnNames.Count];
                for (int i = 0; i < ctx.ColumnNames.Count; i++)
                    rowValues[i] = row[ctx.ColumnNames[i]];
                rows.Add(rowValues);
            }

            stopwatch.Stop();
            bool isLastPage = rows.Count == 0 || nextPaging == null;

            // Update partition and tracker — caller doesn't need to
            partition.LastPagingState = nextPaging;
            Interlocked.Add(ref ctx.TotalRead, rows.Count);
            ctx.Tracker.AddReadTime(stopwatch.ElapsedMilliseconds);
            var workChunk = partition.AddChunkAndTrim(nextPaging);
            if (isLastPage) partition.IsExhausted = true;

            return new ReadResult { Rows = rows, WorkChunk = workChunk, IsLastPage = isLastPage };
        }

        internal static string BuildSelectCql(ProcessorContext context, string range) =>
            $"SELECT * FROM \"{context.KeyspaceName}\".\"{context.TableName}\"" +
            $" WHERE COSMOS_CHANGEFEED_FROM_START() = true AND COSMOS_FEEDRANGE() = '{range}'";
    }
}

