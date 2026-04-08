using Cassandra;
using CassandraMigrationProcessor.Context;
using CassandraMigrationProcessor.Helpers.Cassandra;
using CassandraMigrationProcessor.Helpers.JobManagement;
using CassandraMigrationProcessor.Models;
using CassandraMigrationProcessor.Workers;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;

namespace CassandraMigrationProcessor.Processors
{
    /// <summary>
    /// Copies rows from source Cassandra (Cosmos DB) to
    /// target Cassandra (OSS) using the DataStax driver.
    /// </summary>
    internal partial class CopyProcessor : MigrationProcessor
    {
        public CopyProcessor(
            Log log,
            ISession sourceSession,
            MigrationSettings config,
            MigrationWorker? migrationWorker = null)
            : base(log, sourceSession, config, migrationWorker)
        {
            MigrationJobContext.AddVerboseLog(
                "CopyProcessor: Constructor called");
        }

        private Task<TaskResult> CopyProcess_ExceptionHandler(
            Exception ex,
            int attemptCount,
            string processName,
            string keyspace,
            string table,
            int chunkIndex,
            int currentBackoff)
        {
            Console.WriteLine(
                $"  CHUNK ERROR: {keyspace}.{table}[{chunkIndex}] " +
                $"attempt={attemptCount}: {ex.Message}");

            if (ex is OperationCanceledException)
            {
                return Task.FromResult(TaskResult.Abort);
            }
            else
            {
                _log.WriteLine(
                    $"{processName} attempt {attemptCount} for " +
                    $"{keyspace}.{table}[{chunkIndex}] failed. " +
                    $"Details:{ex}. Retrying in {currentBackoff}s...",
                    LogType.Error);
                return Task.FromResult(TaskResult.Retry);
            }
        }

        private async Task<TaskResult> ProcessChunkAsync(
            MigrationUnit mu,
            int chunkIndex,
            ProcessorContext ctx,
            double initialPercent,
            double contributionFactor)
        {
            MigrationJobContext.AddVerboseLog(
                $"CopyProcessor.ProcessChunkAsync: " +
                $"{mu.KeyspaceName}.{mu.TableName}[{chunkIndex}]");

            Console.WriteLine(
                $"  GetRowCount: {ctx.KeyspaceName}.{ctx.TableName}...");
            _log.WriteLine(
                $"Counting source documents for " +
                $"{ctx.KeyspaceName}.{ctx.TableName} " +
                $"(SELECT COUNT(*) with 120s timeout)...");
            long rowCount = await CassandraHelper.GetRowCountAsync(
                ctx.SourceSession,
                ctx.KeyspaceName,
                ctx.TableName)
                .ConfigureAwait(false);
            Console.WriteLine(
                $"  RowCount={rowCount}");
            _log.WriteLine(
                rowCount >= 0
                    ? $"Source document count: {rowCount:N0} " +
                      $"for {ctx.KeyspaceName}.{ctx.TableName}"
                    : $"Could not determine document count " +
                      $"for {ctx.KeyspaceName}.{ctx.TableName} " +
                      $"(COUNT timed out)");

            // Persist row count on migration unit
            if (rowCount > 0)
            {
                mu.EstimatedRowCount = rowCount;
                mu.UpdateParentJob();
            }

            mu.MigrationChunks[chunkIndex].SourceQueryRowCount =
                rowCount;
            ctx.DownloadCount += rowCount;

            _log.WriteLine(
                $"Count for {ctx.KeyspaceName}.{ctx.TableName}" +
                $"[{chunkIndex}] is {rowCount}");

            if (_targetSession == null
                && !MigrationJobContext
                    .CurrentlyActiveJob.IsSimulatedRun)
            {
                var job = MigrationJobContext.CurrentlyActiveJob;
                Console.WriteLine($"CopyProcessor: Creating target session for {ctx.TargetKeyspaceName}");
                _targetSession = CassandraClientFactory
                    .CreateTargetSession(
                        _log, job,
                        string.Empty);
                await CassandraHelper.EnsureKeyspaceExistsAsync(
                    _targetSession,
                    ctx.TargetKeyspaceName)
                    .ConfigureAwait(false);
                Console.WriteLine($"CopyProcessor: Target session ready for {ctx.TargetKeyspaceName}");
            }

            Console.WriteLine(
                $"  Starting CopyRowsAsync: {rowCount} rows...");

            // Discover feed ranges for parallel copy
            _log.WriteLine(
                $"Discovering feed ranges for " +
                $"{ctx.KeyspaceName}.{ctx.TableName}...");
            var feedRanges = await CassandraHelper.GetFeedRangesAsync(
                _sourceSession!,
                ctx.KeyspaceName,
                ctx.TableName)
                .ConfigureAwait(false);
            _log.WriteLine(
                $"Found {feedRanges.Count} feed ranges " +
                $"for {ctx.KeyspaceName}.{ctx.TableName}");

            TaskResult result;
            if (feedRanges.Count > 1)
            {
                _log.WriteLine(
                    $"Parallel copy: {feedRanges.Count} " +
                    $"feed ranges for " +
                    $"{ctx.KeyspaceName}.{ctx.TableName}");
                result = await CopyWithFeedRangesAsync(
                    mu, chunkIndex,
                    initialPercent, contributionFactor,
                    rowCount, ctx, feedRanges);
            }
            else
            {
                var copier = new DocumentCopyWorker();
                copier.Initialize(
                    _log,
                    _sourceSession!,
                    _targetSession!,
                    ctx.KeyspaceName,
                    ctx.TableName,
                    ctx.TargetKeyspaceName,
                    ctx.TargetTableName,
                    _config.CqlCopyPageSize);
                result = await copier.CopyRowsAsync(
                    mu, chunkIndex,
                    initialPercent, contributionFactor,
                    rowCount, _cts.Token,
                    MigrationJobContext
                        .CurrentlyActiveJob.IsSimulatedRun);
            }
            Console.WriteLine(
                $"  CopyRowsAsync result: {result}");

            if (result == TaskResult.Success)
            {
                if (!_cts.Token.IsCancellationRequested
                    && !MigrationJobContext.ControlledPauseRequested
                    && mu.MigrationChunks[chunkIndex].Segments
                        .All(seg => seg.IsProcessed == true))
                {
                    mu.MigrationChunks[chunkIndex].IsDownloaded = true;
                    mu.MigrationChunks[chunkIndex].IsUploaded = true;
                }
                MigrationJobContext.SaveMigrationUnit(mu, false);
                return TaskResult.Success;
            }
            else if (result == TaskResult.Canceled)
            {
                _log.WriteLine(
                    $"Copy paused for {ctx.KeyspaceName}" +
                    $".{ctx.TableName}[{chunkIndex}].");
                return TaskResult.Canceled;
            }
            else
            {
                _log.WriteLine(
                    $"Copy failed for {ctx.KeyspaceName}" +
                    $".{ctx.TableName}[{chunkIndex}].",
                    LogType.Error);
                return TaskResult.Retry;
            }
        }
    }
}