using Cassandra;
using CassandraMigrationProcessor.Context;
using CassandraMigrationProcessor.Infrastructure;
using CassandraMigrationProcessor.CassandraDriver;
using CassandraMigrationProcessor.Models;
using System;
using System.Threading;

namespace CassandraMigrationProcessor.DataTransfer
{
    /// <summary>
    /// Manages change feed (replay) lifecycle: creates the
    /// ReplayProcessor on demand, enqueues tables, and
    /// starts/stops the feed.
    /// </summary>
    public class ChangeFeedManager : IDisposable
    {
        private readonly MigrationLog _log;
        private readonly MigrationJob _job;
        private readonly PipelineConfig _pipelineConfig;
        private readonly ISession _targetSession;
        private readonly TokenRefreshManager? _tokenRefreshManager;
        private ReplayProcessor? _replayProcessor;
        private readonly object _lock = new();

        public volatile bool IsRunning;

        public ChangeFeedManager(MigrationLog log, MigrationJob job, MigrationSettings settings,
            ISession targetSession, TokenRefreshManager? tokenRefreshManager = null)
        {
            _log = log;
            _job = job;
            _pipelineConfig = PipelineConfig.Resolve(job, settings);
            _targetSession = targetSession;
            _tokenRefreshManager = tokenRefreshManager;
        }

        public void Stop()
        {
            IsRunning = false;
            if (_replayProcessor != null)
                _replayProcessor.ExecutionCancelled = true;
        }

        public void Dispose() { Stop(); }

        public bool AddTable(MigrationUnit mu, CancellationToken cancellationToken)
        {
            if (!MigrationUtilities.IsOnline(_job)) return false;

            lock (_lock)
            {
                SchemaManager.EnsureKeyspaceExists(_targetSession, mu.GetEffectiveTargetKeyspaceName());

                if (_replayProcessor == null)
                {
                    var source = CassandraClientFactory.CreateSourceSession(_log, _job, mu.KeyspaceName, _tokenRefreshManager);
                    _replayProcessor = new ReplayProcessor(_log, source, _targetSession,
                        MigrationJobContext.MigrationUnitsCache, _pipelineConfig,
                        _job, true, null, _tokenRefreshManager);
                }
            }

            _log.WriteLine($"Adding {mu.KeyspaceName}.{mu.TableName} to change feed queue", LogType.Debug);
            _replayProcessor?.AddTableToProcess(mu.Id, cancellationToken);

            return true;
        }

        public bool StartAll(CancellationToken cancellationToken, MigrationWorker worker)
        {
            if (IsRunning) return false;
            if (!MigrationUtilities.IsOnline(_job)) return false;
            if (!MigrationUtilities.IsOfflineJobCompleted(_job)) return false;
            if (!MigrationUtilities.AnyValidTable(_job)) return false;

            IsRunning = true;

            if (_replayProcessor == null)
            {
                var source = CassandraClientFactory.CreateSourceSession(_log, _job, string.Empty, _tokenRefreshManager);
                _replayProcessor = new ReplayProcessor(_log, source, _targetSession,
                    MigrationJobContext.MigrationUnitsCache,
                    _pipelineConfig, _job, false, worker, _tokenRefreshManager);
            }

            _replayProcessor?.RunChangeFeedForAllTables(cancellationToken);

            return true;
        }
    }
}
