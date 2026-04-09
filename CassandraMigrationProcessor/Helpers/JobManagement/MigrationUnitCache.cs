using CassandraMigrationProcessor.Context;
using System;
using System.Collections.Concurrent;
using CassandraMigrationProcessor.Models;

namespace CassandraMigrationProcessor.Helpers.JobManagement
{
    public class MigrationUnitCache
    {
        private readonly ConcurrentDictionary<string, MigrationUnit> _migrationUnits = new();

        private static string BuildCacheKey(string migrationUnitId, string jobId) => $"{jobId}::{migrationUnitId}";

        public MigrationUnit GetMigrationUnit(string migrationUnitId, string JobId = null)
        {
            if (string.IsNullOrEmpty(JobId))
            {
                JobId = MigrationJobContext.CurrentlyActiveJob?.Id;
                if (string.IsNullOrEmpty(JobId))
                    return null;
            }

            var cacheKey = BuildCacheKey(migrationUnitId, JobId);

            if (_migrationUnits.TryGetValue(cacheKey, out MigrationUnit? cachedMigrationUnit))
                return cachedMigrationUnit;

            var mu = MigrationJobContext.GetMigrationUnitFromStorage(JobId, migrationUnitId);
            if (mu != null)
                _migrationUnits[cacheKey] = mu;

            return mu;
        }

        public bool UpdateMigrationUnit(MigrationUnit migrationUnit)
        {
            if (migrationUnit == null || string.IsNullOrEmpty(migrationUnit.Id) || string.IsNullOrEmpty(migrationUnit.JobId))
                return false;

            var cacheKey = BuildCacheKey(migrationUnit.Id, migrationUnit.JobId);
            _migrationUnits[cacheKey] = migrationUnit;
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
}
