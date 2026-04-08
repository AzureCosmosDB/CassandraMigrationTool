using Cassandra;
using CassandraMigrationProcessor.Context;
using CassandraMigrationProcessor.Helpers.Cassandra;
using CassandraMigrationProcessor.Models;
using CassandraMigrationProcessor.Workers;
using System;
using System.Linq;
using System.Threading.Tasks;

namespace CassandraMigrationProcessor.Processors
{
    /// <summary>
    /// Copies rows from source Cassandra (Cosmos DB) to
    /// target Cassandra (OSS) using the DataStax driver.
    /// </summary>
    internal partial class CopyProcessor : MigrationProcessor
    {
        public CopyProcessor(Log log, ISession sourceSession, MigrationSettings config, MigrationJob job,
            MigrationWorker? migrationWorker = null)
            : base(log, sourceSession, config, job, migrationWorker)
        {
            MigrationJobContext.AddVerboseLog("CopyProcessor: Constructor called");
        }

        private Task<TaskResult> CopyProcess_ExceptionHandler(Exception ex, int attemptCount, string processName,
            string keyspace,
            string table,
            int chunkIndex,
            int currentBackoff)
        {
            if (ex is OperationCanceledException) return Task.FromResult(TaskResult.Abort);
            else
            {
                _log.WriteLine($"{processName} attempt {attemptCount} for {keyspace}.{table}[{chunkIndex}] failed. Details:{ex}. Retrying in {currentBackoff}s...",
                    LogType.Error);
                return Task.FromResult(TaskResult.Retry);
            }
        }

        /// <summary>
        /// Processes a single migration chunk: counts source
        /// rows, discovers feed ranges, and copies data to
        /// the target cluster.
        /// </summary>
        private async Task<TaskResult> ProcessChunkAsync(MigrationUnit migrationUnit, int chunkIndex,
            ProcessorContext context,
            double initialPercent,
            double contributionFactor)
        {
            MigrationJobContext.AddVerboseLog($"CopyProcessor.ProcessChunkAsync: {migrationUnit.KeyspaceName}.{migrationUnit.TableName}[{chunkIndex}]");

            _log.WriteLine($"Counting source documents for {context.KeyspaceName}.{context.TableName} (SELECT COUNT(*) with 120s timeout)...");
            long rowCount = await CassandraHelper.GetRowCountAsync(context.SourceSession, context.KeyspaceName,
                context.TableName);
            _log.WriteLine(rowCount >= 0
                    ? $"Source document count: {rowCount:N0} for {context.KeyspaceName}.{context.TableName}"
                    : $"Could not determine document count for {context.KeyspaceName}.{context.TableName} (COUNT timed out)");

            if (rowCount > 0)
            {
                migrationUnit.EstimatedRowCount = rowCount;
                migrationUnit.UpdateParentJob();
            }

            migrationUnit.MigrationChunks[chunkIndex].SourceQueryRowCount = rowCount;
            context.DownloadCount += rowCount;

            _log.WriteLine($"Count for {context.KeyspaceName}.{context.TableName}[{chunkIndex}] is {rowCount}");

            if (_targetSession == null
                && !_job.IsSimulatedRun)
            {
                _targetSession = CassandraClientFactory.CreateTargetSession(_log, _job, string.Empty);
                await CassandraHelper.EnsureKeyspaceExistsAsync(_targetSession, context.TargetKeyspaceName);
            }

            _log.WriteLine($"Discovering feed ranges for {context.KeyspaceName}.{context.TableName}...");
            var feedRanges = await CassandraHelper.GetFeedRangesAsync(_sourceSession!, context.KeyspaceName,
                context.TableName);
            _log.WriteLine($"Found {feedRanges.Count} feed ranges for {context.KeyspaceName}.{context.TableName}");

            if (_job.IsSimulatedRun)
            {
                _log.WriteLine($"Simulated: would copy {rowCount} rows from {context.KeyspaceName}.{context.TableName}");
                return TaskResult.Success;
            }

            TaskResult result;
            _log.WriteLine($"Pipeline copy: {feedRanges.Count} feed range(s) for {context.KeyspaceName}.{context.TableName}");
            result = await CopyWithFeedRangesAsync(migrationUnit, chunkIndex, initialPercent, contributionFactor,
                rowCount, context, feedRanges);

            if (result == TaskResult.Success)
            {
                if (!_cancellation.Token.IsCancellationRequested
                    && !MigrationJobContext.ControlledPauseRequested
                    && migrationUnit.MigrationChunks[chunkIndex].Segments
                        .All(seg => seg.IsProcessed == true))
                {
                    migrationUnit.MigrationChunks[chunkIndex].IsDownloaded = true;
                    migrationUnit.MigrationChunks[chunkIndex].IsUploaded = true;
                }
                MigrationJobContext.SaveMigrationUnit(migrationUnit, false);
                return TaskResult.Success;
            }
            else if (result == TaskResult.Canceled)
            {
                _log.WriteLine($"Copy paused for {context.KeyspaceName}.{context.TableName}[{chunkIndex}].");
                return TaskResult.Canceled;
            }
            else
            {
                _log.WriteLine($"Copy failed for {context.KeyspaceName}.{context.TableName}[{chunkIndex}].",
                    LogType.Error);
                return TaskResult.Retry;
            }
        }
    }
}
