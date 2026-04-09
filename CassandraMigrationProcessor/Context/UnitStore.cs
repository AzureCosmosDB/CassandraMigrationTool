using Newtonsoft.Json;
using System;
using System.IO;

namespace CassandraMigrationProcessor.Context
{
    // TODO: Convert to injectable singleton service.
    // Currently static for backward compatibility with
    // the processor library which lacks DI support.
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
                    MigrationJobContext.Store.Write(
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

        public static bool RemoveUnit(MigrationUnitBasic unit)
        {
            if (unit == null || unit.ParentJob == null)
                return false;

            try
            {
                var job = unit.ParentJob;
                var index = job.Tables
                    .FindIndex(mu => mu.Id == unit.Id);
                if (index == -1) return false;

                job.Tables.RemoveAt(index);

                var filePath = Path.Combine(
                    JobStore.JobsFolder, unit.JobId,
                    $"{unit.Id}.json");
                MigrationJobContext.Store.Delete(filePath);

                return MigrationJobContext.SaveMigrationJob(job);
            }
            catch (Exception ex)
            {
                Console.WriteLine(
                    $"[WARN] RemoveUnit failed: {ex.Message}");
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
                    .Read(filePath);
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
