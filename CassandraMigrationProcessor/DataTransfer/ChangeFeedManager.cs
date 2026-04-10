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
    public class ChangeFeedManager
    {
        private readonly MigrationLog _log;
        private readonly MigrationJob _job;
        private readonly MigrationSettings _config;
        private readonly Func<ISession> _ensureTargetSession;
        private ReplayProcessor? _replayProcessor;
        private readonly object _lock = new();

        public volatile bool IsRunning;

        public ChangeFeedManager(MigrationLog log, MigrationJob job, MigrationSettings config,
            Func<ISession> ensureTargetSession)
        {
            _log = log;
            _job = job;
            _config = config;
            _ensureTargetSession = ensureTargetSession;
        }

        public void Stop()
        {
            IsRunning = false;
            if (_replayProcessor != null)
                _replayProcessor.ExecutionCancelled = true;
        }

        public bool AddTable(MigrationUnit mu, CancellationTokenSource cancellation)
        {
            if (!MigrationUtilities.IsOnline(_job)) return false;

            lock (_lock)
            {
                var target = _ensureTargetSession();
                SchemaManager.EnsureKeyspaceExists(target, mu.GetEffectiveTargetKeyspaceName());

                if (_replayProcessor == null)
                {
                    var source = CassandraClientFactory.CreateSourceSession(_log, _job, mu.KeyspaceName);
                    _replayProcessor = new ReplayProcessor(_log, source, target,
                        MigrationJobContext.MigrationUnitsCache, _config,
                        _job, true, null);
                }
            }

            _log.WriteLine($"Adding {mu.KeyspaceName}.{mu.TableName} to change feed queue", LogType.Debug);
            _replayProcessor?.AddTableToProcess(mu.Id, cancellation);

            return true;
        }

        public bool StartAll(CancellationTokenSource cancellation, MigrationWorker worker)
        {
            if (IsRunning) return false;
            if (!MigrationUtilities.IsOnline(_job)) return false;
            if (!MigrationUtilities.IsOfflineJobCompleted(_job)) return false;
            if (!MigrationUtilities.AnyValidTable(_job)) return false;

            IsRunning = true;

            if (!_job.IsSimulatedRun)
                _ensureTargetSession();

            if (_replayProcessor == null)
            {
                var target = _ensureTargetSession();
                var source = CassandraClientFactory.CreateSourceSession(_log, _job, string.Empty);
                _replayProcessor = new ReplayProcessor(_log, source, target,
                    MigrationJobContext.MigrationUnitsCache,
                    _config, _job, false, worker);
            }

            _replayProcessor?.RunChangeFeedForAllTables(cancellation);

            return true;
        }
    }
}
