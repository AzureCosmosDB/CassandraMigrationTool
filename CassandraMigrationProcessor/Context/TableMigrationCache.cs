using System.Collections.Concurrent;
using CassandraMigrationProcessor.Models;

namespace CassandraMigrationProcessor.Context;

/// <summary>
/// In-memory cache of <see cref="TableMigration"/> instances keyed by
/// (jobId, migrationUnitId), with read-through to <see cref="UnitStore"/>.
/// </summary>
public class TableMigrationCache
{
    private readonly ConcurrentDictionary<string, TableMigration> _migrationUnits = new();

    private static string BuildCacheKey(string migrationUnitId, string jobId) => $"{jobId}::{migrationUnitId}";

    public TableMigration GetMigrationUnit(string migrationUnitId, string jobId = null)
    {
        if (string.IsNullOrEmpty(jobId))
        {
            jobId = MigrationJobContext.Instance.CurrentlyActiveJob?.Id;
            if (string.IsNullOrEmpty(jobId))
                return null;
        }

        var cacheKey = BuildCacheKey(migrationUnitId, jobId);

        if (_migrationUnits.TryGetValue(cacheKey, out TableMigration? cachedMigrationUnit))
            return cachedMigrationUnit;

        var mu = MigrationJobContext.Instance.GetMigrationUnitFromStorage(jobId, migrationUnitId);
        if (mu != null)
            _migrationUnits[cacheKey] = mu;

        return mu;
    }

    public bool UpdateMigrationUnit(TableMigration TableMigration)
    {
        if (TableMigration == null || string.IsNullOrEmpty(TableMigration.Id) || string.IsNullOrEmpty(TableMigration.JobId))
            return false;

        var cacheKey = BuildCacheKey(TableMigration.Id, TableMigration.JobId);
        _migrationUnits[cacheKey] = TableMigration;
        return true;
    }

    public void RemoveMigrationUnit(string migrationUnitId)
    {
        if (string.IsNullOrEmpty(migrationUnitId))
            return;

        foreach (var key in _migrationUnits.Keys)
        {
            if (key.EndsWith($"::{migrationUnitId}", StringComparison.Ordinal))
                _migrationUnits.TryRemove(key, out _);
        }
    }

    /// <summary>
    /// Evicts every cached unit that belongs to <paramref name="jobId"/>.
    /// Called when a job reaches a terminal state so its per-table
    /// runtime state (partition snapshots, counters) does not survive
    /// in the process-wide cache.
    /// </summary>
    public void RemoveAllForJob(string jobId)
    {
        if (string.IsNullOrEmpty(jobId))
            return;

        var prefix = $"{jobId}::";
        foreach (var key in _migrationUnits.Keys)
        {
            if (key.StartsWith(prefix, StringComparison.Ordinal))
                _migrationUnits.TryRemove(key, out _);
        }
    }
}
