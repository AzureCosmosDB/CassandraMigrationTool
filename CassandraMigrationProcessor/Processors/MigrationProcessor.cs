using Cassandra;
using CassandraMigrationProcessor.Context;
using CassandraMigrationProcessor.Helpers.Cassandra;
using CassandraMigrationProcessor.Helpers.JobManagement;
using CassandraMigrationProcessor.Models;
using CassandraMigrationProcessor.Workers;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace CassandraMigrationProcessor.Processors
{
    /// <summary>
    /// Base class for Cassandra migration processors.
    /// Manages source/target sessions and cancellation.
    /// </summary>
    public abstract class MigrationProcessor : IDisposable
    {
        protected ISession? _sourceSession;
        protected ISession? _targetSession;
        protected MigrationSettings _config;
        protected CancellationTokenSource _cts;
        protected Log _log;
        protected MigrationWorker? _migrationWorker;
        protected ChangeFeedProcessor? _changeFeedProcessor;

        public volatile bool ProcessRunning;
        public volatile bool IsChangeFeedRunning;

        protected MigrationProcessor(
            Log log,
            ISession sourceSession,
            MigrationSettings config,
            MigrationWorker? migrationWorker = null)
        {
            _log = log;
            _sourceSession = sourceSession;
            _targetSession = null;
            _config = config;
            _cts = new CancellationTokenSource();
            _migrationWorker = migrationWorker;
        }

        public virtual void StopProcessing(bool updateStatus = true,
            bool isPause = false)
        {
            MigrationJobContext.AddVerboseLog(
                $"MigrationProcessor.StopProcessing: " +
                $"updateStatus={updateStatus}, isPause={isPause}");

            // Cancel first so workers see the signal
            _cts?.Cancel();

            if (_changeFeedProcessor != null)
                _changeFeedProcessor.ExecutionCancelled = true;

            if (MigrationJobContext.CurrentlyActiveJob != null)
            {
                if (isPause)
                    MigrationJobContext.CurrentlyActiveJob.Status
                        = JobStatus.Paused;
                else if (MigrationJobContext.CurrentlyActiveJob.Status
                         == JobStatus.Running)
                    MigrationJobContext.CurrentlyActiveJob.Status
                        = JobStatus.Pending;
            }

            MigrationJobContext.SaveMigrationJob(
                MigrationJobContext.CurrentlyActiveJob);

            if (updateStatus)
                ProcessRunning = false;
        }

        /// <summary>
        /// Recreate the CTS so the processor can be restarted
        /// after a cancel/pause.
        /// </summary>
        public void ResetCancellationToken()
        {
            _cts?.Dispose();
            _cts = new CancellationTokenSource();
        }

        protected ProcessorContext SetProcessorContext(
            MigrationUnit mu)
        {
            var keyspaceName = mu.KeyspaceName;
            var tableName = mu.TableName;
            var targetKeyspaceName = mu.GetEffectiveTargetKeyspaceName();
            var targetTableName = mu.GetEffectiveTargetTableName();

            var context = new ProcessorContext
            {
                MigrationUnitId = mu.Id,
                JobId = MigrationJobContext.CurrentlyActiveJob?.Id
                    ?? string.Empty,
                KeyspaceName = keyspaceName,
                TableName = tableName,
                TargetKeyspaceName = targetKeyspaceName,
                TargetTableName = targetTableName,
                SourceSession = _sourceSession!,
            };

            return context;
        }

        public bool AddTableToChangeFeedQueue(MigrationUnit mu)
        {
            Console.WriteLine(
                $"AddTableToChangeFeedQueue: mu={mu.Id} " +
                $"table={mu.KeyspaceName}.{mu.TableName}");
            MigrationJobContext.AddVerboseLog(
                $"MigrationProcessor.AddTableToChangeFeedQueue: " +
                $"mu={mu.Id}");

            if (!Helper.IsOnline(MigrationJobContext.CurrentlyActiveJob))
            {
                Console.WriteLine("AddTableToChangeFeedQueue: Not online, skip");
                return false;
            }

            if (_targetSession == null)
            {
                var job = MigrationJobContext.CurrentlyActiveJob;
                _targetSession = CassandraClientFactory
                    .CreateTargetSession(_log, job, string.Empty);
                CassandraHelper.EnsureKeyspaceExists(
                    _targetSession,
                    mu.GetEffectiveTargetKeyspaceName());
            }

            if (_changeFeedProcessor == null
                && _sourceSession != null)
            {
                Console.WriteLine("AddTableToChangeFeedQueue: Creating ChangeFeedProcessor with fresh source session");
                var job = MigrationJobContext.CurrentlyActiveJob;
                var freshSourceSession = CassandraClientFactory
                    .CreateSourceSession(_log, job, mu.KeyspaceName);
                _changeFeedProcessor = new ChangeFeedProcessor(
                    _log, freshSourceSession, _targetSession!,
                    MigrationJobContext.MigrationUnitsCache, _config);
            }

            Console.WriteLine(
                $"AddTableToChangeFeedQueue: cfp={(_changeFeedProcessor != null)} " +
                $"Adding {mu.KeyspaceName}.{mu.TableName}");
            _log.WriteLine(
                $"Adding {mu.KeyspaceName}.{mu.TableName} " +
                $"to change feed queue", LogType.Debug);
            _changeFeedProcessor?.AddTableToProcess(mu.Id, _cts);

            return true;
        }

        public bool RunChangeFeedForAllTables()
        {
            Console.WriteLine(
                "RunChangeFeedForAllTables called");
            MigrationJobContext.AddVerboseLog(
                "MigrationProcessor.RunChangeFeedForAllTables");

            if (IsChangeFeedRunning)
            {
                Console.WriteLine("CF: Already running, skipping");
                return false;
            }
            if (!Helper.IsOnline(MigrationJobContext.CurrentlyActiveJob))
            {
                Console.WriteLine("CF: Not online mode, skipping");
                return false;
            }
            if (!Helper.IsOfflineJobCompleted(
                MigrationJobContext.CurrentlyActiveJob))
            {
                Console.WriteLine("CF: Offline job not completed, skipping");
                return false;
            }
            if (!Helper.AnyValidTable(
                MigrationJobContext.CurrentlyActiveJob))
            {
                Console.WriteLine("CF: No valid tables, skipping");
                return false;
            }

            IsChangeFeedRunning = true;
            Console.WriteLine("CF: Passed all checks, starting...");

            if (_targetSession == null
                && !MigrationJobContext
                    .CurrentlyActiveJob.IsSimulatedRun)
            {
                var job = MigrationJobContext.CurrentlyActiveJob;
                _targetSession = CassandraClientFactory
                    .CreateTargetSession(_log, job, string.Empty);
            }

            if (_changeFeedProcessor == null
                && _sourceSession != null)
            {
                Console.WriteLine("CF: Creating new ChangeFeedProcessor with fresh source session");
                var job = MigrationJobContext.CurrentlyActiveJob;
                var freshSourceSession = CassandraClientFactory
                    .CreateSourceSession(_log, job, string.Empty);
                _changeFeedProcessor = new ChangeFeedProcessor(
                    _log, freshSourceSession, _targetSession!,
                    MigrationJobContext.MigrationUnitsCache,
                    _config, false, _migrationWorker);
            }

            Console.WriteLine(
                $"CF: _changeFeedProcessor={(_changeFeedProcessor != null ? "exists" : "null")}");
            _changeFeedProcessor?
                .RunChangeFeedForAllTables(_cts);

            return true;
        }

        public void StopOfflineOrInvokeChangeFeed()
        {
            if (!Helper.IsOnline(MigrationJobContext.CurrentlyActiveJob)
                && Helper.IsOfflineJobCompleted(
                    MigrationJobContext.CurrentlyActiveJob))
            {
                // Do NOT mark completed if cancelled or paused
                var job = MigrationJobContext.CurrentlyActiveJob;
                if (!MigrationJobContext.ControlledPauseRequested
                    && job?.Status != JobStatus.Cancelled)
                {
                    _log.WriteLine(
                        $"Job {job?.Id} " +
                        $"Completed");
                    job!.Status = JobStatus.Completed;
                    MigrationJobContext.SaveMigrationJob(job);
                }
                StopProcessing();
            }
            else
            {
                if (!MigrationJobContext.ControlledPauseRequested)
                {
                    _log.WriteLine(
                        "Invoke RunChangeFeedForAllTables.",
                        LogType.Debug);
                    RunChangeFeedForAllTables();
                }
            }
        }

        public virtual Task<TaskResult> StartProcessAsync(
            string migrationUnitId)
        {
            return Task.FromResult(TaskResult.Success);
        }

        public void Dispose()
        {
            _cts?.Dispose();
            try { _targetSession?.Dispose(); } catch { }
            // _sourceSession is owned by the caller
        }
    }
}
