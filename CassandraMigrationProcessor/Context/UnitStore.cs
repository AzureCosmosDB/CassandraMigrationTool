using Newtonsoft.Json;
using System;
using System.IO;

namespace CassandraMigrationProcessor.Context
{
    public static class UnitStore
    {
        private static readonly object _writeMULock = new object();

        public static MigrationUnit GetUnit(
            string key, string jobId = null)
        {
            if (string.IsNullOrEmpty(jobId)
                && MigrationJobContext.CurrentlyActiveJob != null)
            {
                jobId = MigrationJobContext.CurrentlyActiveJob.Id;
            }

            if (MigrationJobContext.MigrationUnitsCache == null)
                return GetFromStorage(jobId, key);
            else
                return MigrationJobContext.MigrationUnitsCache
                    .GetMigrationUnit(key, jobId);
        }

        public static bool SaveUnit(
            MigrationUnit mu, bool updateParent)
        {
            try
            {
                if (mu == null) return false;

                if (MigrationJobContext.CurrentlyActiveJob != null)
                    mu.ParentJob =
                        MigrationJobContext.CurrentlyActiveJob;

                if (mu.ParentJob != null && updateParent)
                    mu.UpdateParentJob();

                lock (_writeMULock)
                {
                    var muFilePath = Path.Combine(
                        JobStore.JobsFolder, mu.JobId,
                        $"{mu.Id}.json");
                    string muJson =
                        JsonConvert.SerializeObject(
                            mu, Formatting.Indented);
                    MigrationJobContext.Store.UpsertDocument(
                        muFilePath, muJson);
                }

                if (MigrationJobContext.CurrentlyActiveJob != null
                    && updateParent)
                {
                    JobStore.PersistActiveJobUnderLock();
                }

                if (MigrationJobContext.MigrationUnitsCache != null)
                    MigrationJobContext.MigrationUnitsCache
                        .UpdateMigrationUnit(mu);

                return true;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[WARN] SaveUnit failed: {ex.Message}");
                return false;
            }
        }

        public static MigrationUnit GetFromStorage(
            string jobId, string unitId)
        {
            MigrationJobContext.AddVerboseLog(
                $"GetMigrationUnit: jobId={jobId}, unitId={unitId}");
            try
            {
                var filePath = Path.Combine(
                    JobStore.JobsFolder, jobId, $"{unitId}.json");
                string json = MigrationJobContext.Store
                    .ReadDocument(filePath);
                return JsonConvert
                    .DeserializeObject<MigrationUnit>(json);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[WARN] GetFromStorage({jobId}, {unitId}) failed: {ex.Message}");
                return null;
            }
        }
    }
}
