using Cassandra;
using CassandraMigrationProcessor.Context;
using CassandraMigrationProcessor.Infrastructure;
using CassandraMigrationProcessor.CassandraDriver;
using CassandraMigrationProcessor.Models;
using CassandraMigrationProcessor.DataTransfer;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace CassandraMigrationProcessor.DataTransfer.ChangeFeed;
/// <summary>
/// Manages change feed (replay) lifecycle: creates the
/// ReplayProcessor on demand, enqueues tables, and
/// starts/stops the feed.
/// </summary>
public class ChangeFeedManager : IDisposable
{
    private readonly MigrationLog _log;
    private readonly Job _job;
    private readonly PipelineConfig _pipelineConfig;
    private readonly ISession _targetSession;
    private readonly TokenRefreshManager? _tokenRefreshManager;
    private ReplayProcessor? _replayProcessor;
    private readonly SemaphoreSlim _lock = new(1, 1);

    public volatile bool IsRunning;

    public ChangeFeedManager(MigrationLog log, Job job, AppSettings settings,
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

    public void Dispose()
    {
        Stop();
        (_replayProcessor as IDisposable)?.Dispose();
        _lock?.Dispose();
    }

    public async Task<bool> AddTable(TableMigration mu, CancellationToken cancellationToken)
    {
        if (!MigrationUtilities.IsOnline(_job)) return false;

        await _lock.WaitAsync(cancellationToken);
        try
        {
            await SchemaManager.EnsureKeyspaceExistsAsync(_targetSession, mu.GetEffectiveTargetKeyspaceName());
            EnsureReplayProcessor(mu.KeyspaceName);
        }
        finally
        {
            _lock.Release();
        }

        _log.WriteLine($"Adding {mu.KeyspaceName}.{mu.TableName} to change feed queue", LogType.Debug);
        _replayProcessor?.QueueTableForReplay(mu.Id, cancellationToken);

        return true;
    }

    public bool StartAll(CancellationToken cancellationToken)
    {
        if (IsRunning) return false;
        if (!MigrationUtilities.IsOnline(_job)) return false;
        if (!MigrationUtilities.IsOfflineJobCompleted(_job)) return false;
        if (!MigrationUtilities.AnyValidTable(_job)) return false;

        IsRunning = true;

        EnsureReplayProcessor(string.Empty);

        _replayProcessor?.RunChangeFeedForAllTables(cancellationToken);

        return true;
    }

    private void EnsureReplayProcessor(string keyspace)
    {
        if (_replayProcessor == null)
        {
            var source = CassandraClientFactory.CreateSourceSession(_log, _job, keyspace, _tokenRefreshManager);
            _replayProcessor = new ReplayProcessor(_log, source, _targetSession,
                MigrationJobContext.Instance.MigrationUnitsCache, _pipelineConfig,
                _job, _tokenRefreshManager);
        }
    }
}
