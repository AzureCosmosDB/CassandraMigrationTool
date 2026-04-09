using Cassandra;
using CassandraMigrationProcessor.Context;
using CassandraMigrationProcessor.Helpers;
using CassandraMigrationProcessor.Models;
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
    internal class PageReader
    {
        private readonly MigrationLog _log;
        private readonly CancellationTokenSource _cancellation;

        private const int ReadTimeoutMs = 60_000;
        private const int MaxReadRetries = 3;
        private const int RetryDelayMs = 5000;

        public PageReader(MigrationLog log, CancellationTokenSource cancellation)
        {
            _log = log;
            _cancellation = cancellation;
        }

        /// <summary>
        /// Reads a single page of rows from the source
        /// partition, retrying on transient timeouts.
        /// Returns null rows when all retries are exhausted.
        /// </summary>
        public async Task<(List<object[]>? rows, byte[]? nextPaging, bool isLastPage,
            long readTimeMs)> ReadAsync(CopyProcessor.Partition partition,
            ISession sourceSession,
            CopyProcessor.PipelineContext ctx,
            int workerId)
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
            for (int attempt = 1;
                attempt <= MaxReadRetries; attempt++)
            {
                try
                {
                    resultSet = await sourceSession.ExecuteAsync(stmt);
                    break;
                }
                catch (System.Exception ex) when (attempt < MaxReadRetries
                    && ExceptionClassifier.IsTransient(ex))
                {
                    _log.WriteLine($"[W{workerId}] Read timeout (attempt {attempt}/{MaxReadRetries})",
                        LogType.Warning);
                    await Task.Delay(attempt * RetryDelayMs, _cancellation.Token);
                }
            }

            if (resultSet == null)
            {
                ctx.WorkerErrors.Add(TaskResult.Retry);
                stopwatch.Stop();
                return (null, null, true, stopwatch.ElapsedMilliseconds);
            }

            byte[]? nextPaging = resultSet.PagingState;

            var rows = new List<object[]>();
            int available = resultSet.GetAvailableWithoutFetching();
            int consumed = 0;
            foreach (var row in resultSet)
            {
                if (consumed >= available) break;
                consumed++;
                var rowValues =
                    new object[ctx.ColumnNames.Count];
                for (int i = 0;
                    i < ctx.ColumnNames.Count; i++)
                    rowValues[i] = row[ctx.ColumnNames[i]];
                rows.Add(rowValues);
            }

            stopwatch.Stop();
            bool isLastPage = rows.Count == 0 || nextPaging == null;
            return (rows, nextPaging, isLastPage, stopwatch.ElapsedMilliseconds);
        }

        internal static string BuildSelectCql(ProcessorContext context, string range) =>
            $"SELECT * FROM \"{context.KeyspaceName}\".\"{context.TableName}\"" +
            $" WHERE COSMOS_CHANGEFEED_FROM_START() = true AND COSMOS_FEEDRANGE() = '{range}'";
    }
}
