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
        private static string TruncRange(string r) => r.Length > 30 ? r[..15] + "..." : r;
        private static bool IsRetriableWriteError(Exception ex) => Helpers.ExceptionClassifier.IsTransient(ex);
        private static bool IsFatalError(Exception ex) => Helpers.ExceptionClassifier.IsFatal(ex);

        public CopyProcessor(MigrationLog MigrationLog, ISession sourceSession, MigrationSettings config, MigrationJob job,
            MigrationWorker? migrationWorker = null)
            : base(MigrationLog, sourceSession, config, job, migrationWorker)
        {
        }

        private Task<TaskResult> CopyProcess_ExceptionHandler(Exception ex, int attemptCount, string processName,
            string keyspace,
            string table,
            int chunkIndex,
            int currentBackoff)
        {
            if (ex is OperationCanceledException) return Task.FromResult(TaskResult.Abort);

            _log.WriteLine($"{processName} attempt {attemptCount} for {keyspace}.{table}[{chunkIndex}] failed. Details:{ex}. Retrying in {currentBackoff}s...",
                LogType.Warning);
            return Task.FromResult(TaskResult.Retry);
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
            long rowCount = await CassandraHelper.GetRowCountAsync(context.SourceSession, context.KeyspaceName,
                context.TableName);

            if (rowCount > 0)
            {
                migrationUnit.EstimatedRowCount = rowCount;
                migrationUnit.UpdateParentJob();
            }

            migrationUnit.MigrationChunks[chunkIndex].SourceQueryRowCount = rowCount;
            context.DownloadCount += rowCount;

            if (_targetSession == null && !_job.IsSimulatedRun)
            {
                _targetSession = CassandraClientFactory.CreateTargetSession(_log, _job, string.Empty);
                await SchemaManager.EnsureKeyspaceExistsAsync(_targetSession, context.TargetKeyspaceName);
            }

            var feedRanges = await CassandraHelper.GetFeedRangesAsync(_sourceSession!, context.KeyspaceName,
                context.TableName);

            _log.WriteLine($"{context.KeyspaceName}.{context.TableName}: " +
                $"{(rowCount >= 0 ? $"{rowCount:N0} rows" : "count unavailable")}, " +
                $"{feedRanges.Count} feed range(s)", LogType.Info);

            if (_job.IsSimulatedRun)
            {
                _log.WriteLine($"Simulated: {context.KeyspaceName}.{context.TableName}", LogType.Info);
                return TaskResult.Success;
            }

            TaskResult result;
            result = await CopyWithFeedRangesAsync(new PipelineRequest(migrationUnit, chunkIndex, initialPercent, contributionFactor,
                rowCount, context, feedRanges));

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
                _log.WriteLine($"Copy paused for {context.KeyspaceName}.{context.TableName}[{chunkIndex}].", LogType.Info);
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
