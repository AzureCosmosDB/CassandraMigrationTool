using Cassandra;
using CassandraMigrationProcessor.Context;
using CassandraMigrationProcessor.Helpers.Cassandra;
using CassandraMigrationProcessor.Helpers.JobManagement;
using CassandraMigrationProcessor.Models;
using CassandraMigrationProcessor.Helpers;
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
        protected readonly ISession _sourceSession;
        protected ISession? _targetSession;
        protected MigrationSettings _config;
        protected CancellationTokenSource _cancellation;
        protected MigrationLog _log;
        protected MigrationJob _job;
        protected MigrationWorker? _worker;
        protected ChangeFeedProcessor? _changeFeedProcessor;
        private readonly object _changeFeedLock = new();

        public volatile bool ProcessRunning;
        public volatile bool IsChangeFeedRunning;

        protected MigrationProcessor(MigrationLog MigrationLog, ISession sourceSession, MigrationSettings config, MigrationJob job,
            MigrationWorker? worker = null)
        {
            _log = MigrationLog;
            _sourceSession = sourceSession;
            _config = config;
            _job = job;
            _cancellation = new CancellationTokenSource();
            _worker = worker;
        }

        /// <summary>
        /// Lazily creates the target session. Returns the
        /// existing one if already created.
        /// </summary>
        protected ISession EnsureTargetSession()
        {
            if (_targetSession == null)
                _targetSession = CassandraClientFactory.CreateTargetSession(_log, _job, string.Empty);
            return _targetSession;
        }

        /// <summary>
        /// Gracefully stop the migration processor. Cancels the
        /// token, updates job status, and optionally marks the
        /// process as no longer running.
        /// </summary>
        public virtual void StopProcessing(bool updateStatus = true, bool isPause = false)
        {
            // Cancel first so workers see the signal
            _cancellation?.Cancel();

            IsChangeFeedRunning = false;

            if (_changeFeedProcessor != null)
                _changeFeedProcessor.ExecutionCancelled = true;

            if (_job != null)
            {
                if (isPause)
                    _job.Status = JobStatus.Paused;
                else if (_job.Status == JobStatus.Running)
                    _job.Status = JobStatus.Pending;
                // Don't downgrade Paused/Completed/Cancelled/Faulted
            }

            MigrationJobContext.SaveMigrationJob(_job);

            if (updateStatus)
                ProcessRunning = false;
        }

        /// <summary>
        /// Recreate the CTS so the processor can be restarted
        /// after a cancel/pause.
        /// </summary>
        public void ResetCancellationToken()
        {
            _cancellation?.Dispose();
            _cancellation = new CancellationTokenSource();
        }

        protected ProcessorContext SetProcessorContext(MigrationUnit mu)
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
                SourceSession = _sourceSession,
            };

            return context;
        }

        /// <summary>
        /// Enqueue a single table for change-feed processing.
        /// Creates the target session and <see cref="ChangeFeedProcessor"/>
        /// on first call. Thread-safe for concurrent calls from
        /// parallel bulk-copy threads.
        /// </summary>
        public bool AddTableToChangeFeedQueue(MigrationUnit mu)
        {
            if (!MigrationHelper.IsOnline(_job)) return false;

            lock (_changeFeedLock)
            {
                if (_targetSession == null)
                {
                    var target = EnsureTargetSession();
                    SchemaManager.EnsureKeyspaceExists(target, mu.GetEffectiveTargetKeyspaceName());
                }

                if (_changeFeedProcessor == null)
                {
                    var freshSourceSession = CassandraClientFactory.CreateSourceSession(_log, _job, mu.KeyspaceName);
                    _changeFeedProcessor = new ChangeFeedProcessor(_log, freshSourceSession, _targetSession!,
                        MigrationJobContext.MigrationUnitsCache, _config,
                        _job);
                }
            }

            _log.WriteLine($"Adding {mu.KeyspaceName}.{mu.TableName} to change feed queue", LogType.Debug);
            _changeFeedProcessor?.AddTableToProcess(mu.Id, _cancellation);

            return true;
        }

        /// <summary>
        /// Start change-feed processing for all completed tables
        /// in the job. Returns <c>false</c> if the feed is already
        /// running or preconditions are not met.
        /// </summary>
        public bool RunChangeFeedForAllTables()
        {
            if (IsChangeFeedRunning) return false;
            if (!MigrationHelper.IsOnline(_job)) return false;
            if (!MigrationHelper.IsOfflineJobCompleted(_job)) return false;
            if (!MigrationHelper.AnyValidTable(_job)) return false;

            IsChangeFeedRunning = true;

            if (_targetSession == null && !_job.IsSimulatedRun)
                EnsureTargetSession();

            if (_changeFeedProcessor == null)
            {
                var freshSourceSession = CassandraClientFactory.CreateSourceSession(_log, _job, string.Empty);
                _changeFeedProcessor = new ChangeFeedProcessor(_log, freshSourceSession, _targetSession!,
                    MigrationJobContext.MigrationUnitsCache,
                    _config, _job, false, _worker);
            }

            _changeFeedProcessor?.RunChangeFeedForAllTables(_cancellation);

            return true;
        }

        /// <summary>
        /// After all tables finish, either mark the job completed
        /// (offline mode) or start the change-feed processors
        /// (online mode).
        /// </summary>
        public void StopOfflineOrInvokeChangeFeed()
        {
            if (!MigrationHelper.IsOnline(_job)
                && MigrationHelper.IsOfflineJobCompleted(_job))
            {
                // Do NOT mark completed if cancelled or paused
                if (!MigrationJobContext.ControlledPauseRequested
                    && _job?.Status != JobStatus.Cancelled
                    && _job?.Status != JobStatus.Paused)
                {
                    _log.WriteLine($"Job {_job?.Id} Completed", LogType.Info);
                    _job!.Status = JobStatus.Completed;
                    MigrationJobContext.SaveMigrationJob(_job);
                }
                StopProcessing();
            }
            else
            {
                if (!MigrationJobContext.ControlledPauseRequested)
                {
                    _log.WriteLine("Invoke RunChangeFeedForAllTables.", LogType.Debug);
                    RunChangeFeedForAllTables();
                }
            }
        }

        public virtual Task<TaskResult> StartProcessAsync(string migrationUnitId)
        {
            return Task.FromResult(TaskResult.Success);
        }

        public void Dispose()
        {
            IsChangeFeedRunning = false;
            _cancellation?.Dispose();
            MigrationHelper.SafeDispose(_targetSession, "MigrationProcessor target session");
            // _sourceSession is owned by the caller
        }
    }
}
