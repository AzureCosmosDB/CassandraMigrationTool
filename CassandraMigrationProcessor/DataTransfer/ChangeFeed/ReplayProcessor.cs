using Cassandra;
using CassandraMigrationProcessor.Context;
using CassandraMigrationProcessor.CassandraDriver;
using CassandraMigrationProcessor.Models;
using CassandraMigrationProcessor.Infrastructure;
using CassandraMigrationProcessor.DataTransfer;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace CassandraMigrationProcessor.DataTransfer.ChangeFeed;
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
public class ReplayProcessor : IDisposable
{
    private readonly MigrationLog _log;
    private readonly ISession _sourceSession;
    private readonly ISession? _targetSession;
    private readonly TableMigrationCache _muCache;
    private readonly PipelineConfig _pipelineConfig;
    private readonly Job _job;
    private readonly TokenRefreshManager? _tokenRefreshManager;

    private readonly ConcurrentQueue<string> _pendingTables = new();
    private readonly ConcurrentDictionary<string, Task>
        _activeTasks = new();

    private volatile bool _executionCancelled;
    public bool ExecutionCancelled
    {
        get => _executionCancelled;
        set => _executionCancelled = value;
    }

    public void Dispose()
    {
        ExecutionCancelled = true;
        MigrationUtilities.SafeDispose(_sourceSession, "ReplayProcessor source session");
    }

    public ReplayProcessor(MigrationLog log, ISession sourceSession, ISession targetSession, TableMigrationCache muCache,
        PipelineConfig pipelineConfig, Job job,
        TokenRefreshManager? tokenRefreshManager = null)
    {
        _log = log;
        _sourceSession = sourceSession;
        _targetSession = targetSession;
        _muCache = muCache;
        _pipelineConfig = pipelineConfig;
        _job = job;
        _tokenRefreshManager = tokenRefreshManager;
    }

    /// <summary>
    /// Enqueue a single table for change-feed processing.
    /// </summary>
    public void QueueTableForReplay(string migrationUnitId, CancellationToken ct)
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
        if (job == null) return;

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
                const int maxTaskRetries = 3;
                for (int attempt = 1; attempt <= maxTaskRetries; attempt++)
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
                            _pipelineConfig, () => ExecutionCancelled,
                            _tokenRefreshManager);

                        await worker.ReplayTableAsync(mu, ct);
                        return; // success
                    }
                    catch (OperationCanceledException)
                    {
                        return; // expected on pause/stop
                    }
                    catch (Exception ex)
                    {
                        _log.WriteLine(
                            $"CF muId={muId} attempt {attempt}/{maxTaskRetries}: {ex.GetType().Name}: {ex.Message}",
                            LogType.Error);

                        if (ExceptionClassifier.IsFatal(ex) || attempt >= maxTaskRetries)
                        {
                            _log.WriteLine(
                                $"CF muId={muId}: GIVING UP after {attempt} attempt(s) — marking table as failed",
                                LogType.Error);
                            var failedMu = _muCache.GetMigrationUnit(muId, _job?.Id);
                            if (failedMu != null)
                            {
                                failedMu.SourceStatus = TableStatus.Failed;
                                MigrationJobContext.Instance.SaveMigrationUnit(failedMu, true);
                            }
                            return;
                        }

                        await Task.Delay(5000 * attempt);
                    }
                }
            });

            _activeTasks[muId] = task;

            // Clean up tracking after task completes (success or failure)
            _ = task.ContinueWith(_ => _activeTasks.TryRemove(muId, out _));
        }
    }
}
