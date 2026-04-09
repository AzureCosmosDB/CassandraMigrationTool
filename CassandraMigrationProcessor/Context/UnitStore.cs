using Newtonsoft.Json;
using System;
using System.IO;
using CassandraMigrationProcessor.Helpers;
using CassandraMigrationProcessor.Models;
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
            return MigrationHelper.SafeExecute(() =>
            {
                if (mu == null) return false;

                if (mu.ParentJob == null && MigrationJobContext.CurrentlyActiveJob != null)
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
            }, false, "SaveUnit");
        }

        public static bool RemoveUnit(MigrationUnitBasic unit)
        {
            if (unit == null || unit.ParentJob == null)
                return false;

            return MigrationHelper.SafeExecute(() =>
            {
                var job = unit.ParentJob;
                var index = job.Tables
                    .FindIndex(mu => mu.Id == unit.Id);
                if (index == -1) return false;

                job.Tables.RemoveAt(index);

                if (!MigrationJobContext.SaveMigrationJob(job))
                    return false;

                var filePath = Path.Combine(
                    JobStore.JobsFolder, unit.JobId,
                    $"{unit.Id}.json");
                MigrationJobContext.Store.Delete(filePath);

                return true;
            }, false, "RemoveUnit");
        }

        public static MigrationUnit GetFromStorage(
            string jobId, string unitId)
        {
            return MigrationHelper.SafeExecute(() =>
            {
                var filePath = Path.Combine(
                    JobStore.JobsFolder, jobId, $"{unitId}.json");
                string json = MigrationJobContext.Store
                    .Read(filePath);
                return JsonConvert
                    .DeserializeObject<MigrationUnit>(json);
            }, (MigrationUnit)null, $"GetFromStorage({jobId}, {unitId})");
        }
    }
}
