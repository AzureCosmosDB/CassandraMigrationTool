using CassandraMigrationProcessor.Context;
using System;
using System.Collections.Concurrent;
using CassandraMigrationProcessor.Models;

namespace CassandraMigrationProcessor.Context;
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
}
