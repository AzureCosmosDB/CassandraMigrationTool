using Cassandra;
using CassandraMigrationProcessor.Context;
using CassandraMigrationProcessor.CassandraDriver;
using CassandraMigrationProcessor.Models;
using CassandraMigrationProcessor.Infrastructure;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace CassandraMigrationProcessor.DataTransfer
{
    /// <summary>
    /// Tails the Cosmos DB Cassandra change feed for one or
    /// more tables and replicates changes to the target OSS
    /// Cassandra cluster.
    ///
    /// Uses COSMOS_CHANGEFEED_START_TIME() with SELECT * to
    /// read change feed rows and replay them as inserts on
    /// the target. SetAutoPage(false) is critical to avoid
    /// long-poll hang. PagingState acts as the continuation
    /// token.
    ///
    /// Each table is handled by a dedicated
    /// <see cref="ReplayWorker"/> instance.
    /// </summary>
    public class ReplayProcessor
    {
        private readonly MigrationLog _log;
        private readonly ISession _sourceSession;
        private readonly ISession? _targetSession;
        private readonly MigrationUnitCache _muCache;
        private readonly PipelineConfig _pipelineConfig;
        private readonly MigrationJob _job;
        private readonly bool _singleTable;
        private readonly MigrationWorker? _migrationWorker;

        private readonly ConcurrentQueue<string> _pendingTables = new();
        private readonly ConcurrentDictionary<string, Task>
            _activeTasks = new();

        private volatile bool _executionCancelled;
        public bool ExecutionCancelled
        {
            get => _executionCancelled;
            set => _executionCancelled = value;
        }

        public ReplayProcessor(MigrationLog log, ISession sourceSession, ISession targetSession, MigrationUnitCache muCache,
            PipelineConfig pipelineConfig, MigrationJob job,
            bool singleTable, MigrationWorker? migrationWorker)
        {
            _log = log;
            _sourceSession = sourceSession;
            _targetSession = targetSession;
            _muCache = muCache;
            _pipelineConfig = pipelineConfig;
            _job = job;
            _singleTable = singleTable;
            _migrationWorker = migrationWorker;
        }

        /// <summary>
        /// Enqueue a single table for change-feed processing.
        /// </summary>
        public void AddTableToProcess(string migrationUnitId, CancellationToken ct)
        {
            _pendingTables.Enqueue(migrationUnitId);
            StartPendingTables(ct);
        }

        /// <summary>
        /// Start change-feed polling for all completed tables.
        /// </summary>
        public void RunChangeFeedForAllTables(CancellationToken ct)
        {
            var job = _job;
            if (job?.Tables == null) return;

            foreach (var mub in job.Tables)
            {
                if (!MigrationUtilities.IsMigrationUnitValid(mub)) continue;
                if (!mub.CopyComplete) continue;
                if (!_activeTasks.ContainsKey(mub.Id))
                    _pendingTables.Enqueue(mub.Id);
            }

            StartPendingTables(ct);
        }

        private void StartPendingTables(CancellationToken ct)
        {
            while (_pendingTables.TryDequeue(out var muId))
            {
                if (_activeTasks.ContainsKey(muId)) continue;
                if (ExecutionCancelled) break;

                var task = Task.Run(async () =>
                {
                    try
                    {
                        var mu = _muCache.GetMigrationUnit(muId, _job?.Id);
                        if (mu == null)
                        {
                            _log.WriteLine(
                                $"ChangeFeed: MU {muId} not found", LogType.Error);
                            return;
                        }

                        var worker = new ReplayWorker(
                            _log, _sourceSession, _targetSession,
                            _pipelineConfig, () => ExecutionCancelled);

                        await worker.RunAsync(mu, ct);
                    }
                    catch (Exception ex)
                    {
                        Console.Error.WriteLine(
                            $"[CRITICAL] CF muId={muId}: {ex.GetType().Name}: {ex.Message}");
                    }

                    _activeTasks.TryRemove(muId, out _);
                });

                _activeTasks[muId] = task;
            }
        }
    }
}
