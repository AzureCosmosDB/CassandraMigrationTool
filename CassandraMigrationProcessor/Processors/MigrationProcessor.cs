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
        protected MigrationJob _job;
        protected MigrationWorker? _migrationWorker;
        protected ChangeFeedProcessor? _changeFeedProcessor;

        public volatile bool ProcessRunning;
        public volatile bool IsChangeFeedRunning;

        protected MigrationProcessor(
            Log log,
            ISession sourceSession,
            MigrationSettings config,
            MigrationJob job,
            MigrationWorker? migrationWorker = null)
        {
            _log = log;
            _sourceSession = sourceSession;
            _targetSession = null;
            _config = config;
            _job = job;
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

            if (_job != null)
            {
                if (isPause)
                    _job.Status
                        = JobStatus.Paused;
                else if (_job.Status
                         == JobStatus.Running)
                    _job.Status
                        = JobStatus.Pending;
            }

            MigrationJobContext.SaveMigrationJob(
                _job);

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
                JobId = _job?.Id
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
            MigrationJobContext.AddVerboseLog(
                $"MigrationProcessor.AddTableToChangeFeedQueue: " +
                $"mu={mu.Id}");

            if (!Helper.IsOnline(_job))
            {
                return false;
            }

            if (_targetSession == null)
            {
                _targetSession = CassandraClientFactory
                    .CreateTargetSession(_log, _job, string.Empty);
                CassandraHelper.EnsureKeyspaceExists(
                    _targetSession,
                    mu.GetEffectiveTargetKeyspaceName());
            }

            if (_changeFeedProcessor == null
                && _sourceSession != null)
            {
                var freshSourceSession = CassandraClientFactory
                    .CreateSourceSession(_log, _job, mu.KeyspaceName);
                _changeFeedProcessor = new ChangeFeedProcessor(
                    _log, freshSourceSession, _targetSession!,
                    MigrationJobContext.MigrationUnitsCache, _config,
                    _job);
            }

            _log.WriteLine(
                $"Adding {mu.KeyspaceName}.{mu.TableName} " +
                $"to change feed queue", LogType.Debug);
            _changeFeedProcessor?.AddTableToProcess(mu.Id, _cts);

            return true;
        }

        public bool RunChangeFeedForAllTables()
        {
            MigrationJobContext.AddVerboseLog(
                "MigrationProcessor.RunChangeFeedForAllTables");

            if (IsChangeFeedRunning)
            {
                return false;
            }
            if (!Helper.IsOnline(_job))
            {
                return false;
            }
            if (!Helper.IsOfflineJobCompleted(
                _job))
            {
                return false;
            }
            if (!Helper.AnyValidTable(
                _job))
            {
                return false;
            }

            IsChangeFeedRunning = true;

            if (_targetSession == null
                && !_job.IsSimulatedRun)
            {
                _targetSession = CassandraClientFactory
                    .CreateTargetSession(_log, _job, string.Empty);
            }

            if (_changeFeedProcessor == null
                && _sourceSession != null)
            {
                var freshSourceSession = CassandraClientFactory
                    .CreateSourceSession(_log, _job, string.Empty);
                _changeFeedProcessor = new ChangeFeedProcessor(
                    _log, freshSourceSession, _targetSession!,
                    MigrationJobContext.MigrationUnitsCache,
                    _config, _job, false, _migrationWorker);
            }

            _changeFeedProcessor?
                .RunChangeFeedForAllTables(_cts);

            return true;
        }

        public void StopOfflineOrInvokeChangeFeed()
        {
            if (!Helper.IsOnline(_job)
                && Helper.IsOfflineJobCompleted(
                    _job))
            {
                // Do NOT mark completed if cancelled or paused
                if (!MigrationJobContext.ControlledPauseRequested
                    && _job?.Status != JobStatus.Cancelled
                    && _job?.Status != JobStatus.Paused)
                {
                    _log.WriteLine(
                        $"Job {_job?.Id} " +
                        $"Completed");
                    _job!.Status = JobStatus.Completed;
                    MigrationJobContext.SaveMigrationJob(_job);
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
