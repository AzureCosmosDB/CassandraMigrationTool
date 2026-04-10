using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using CassandraMigrationProcessor.Infrastructure;
using CassandraMigrationProcessor.Models;
namespace CassandraMigrationProcessor.Context
{
    public static class UnitStore
    {
        private static readonly object _writeMULock = new object();

        public static MigrationUnit GetUnit(
            string unitId, string jobId = null)
        {
            if (string.IsNullOrEmpty(jobId)
                && MigrationJobContext.CurrentlyActiveJob != null)
            {
                jobId = MigrationJobContext.CurrentlyActiveJob.Id;
            }

            if (MigrationJobContext.MigrationUnitsCache == null)
                return GetFromStorage(jobId, unitId);
            else
                return MigrationJobContext.MigrationUnitsCache
                    .GetMigrationUnit(unitId, jobId);
        }

        public static bool SaveUnit(
            MigrationUnit mu, bool updateParent)
        {
            return MigrationUtilities.SafeExecute(() =>
            {
                if (mu == null) return false;

                if (mu.ParentJob == null && MigrationJobContext.CurrentlyActiveJob != null)
                    mu.ParentJob =
                        MigrationJobContext.CurrentlyActiveJob;

                if (mu.ParentJob != null && updateParent)
                    MigrationUnitMapper.UpdateParentJob(mu);

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

            return MigrationUtilities.SafeExecute(() =>
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
            return MigrationUtilities.SafeExecute(() =>
            {
                var filePath = Path.Combine(
                    JobStore.JobsFolder, jobId, $"{unitId}.json");
                string json = MigrationJobContext.Store
                    .Read(filePath);
                return JsonConvert
                    .DeserializeObject<MigrationUnit>(json);
            }, (MigrationUnit)null, $"GetFromStorage({jobId}, {unitId})");
        }

        public static List<MigrationUnit> GetMigrationUnitsToMigrate(
            MigrationJob job)
        {
            List<MigrationUnit> units = new();
            if (job == null) return units;

            foreach (var summary in job.Tables)
            {
                if (!MigrationUtilities.IsMigrationUnitValid(summary)) continue;
                if (summary.CopyComplete) continue;
                if (summary.SkippedDueToMaxRetries) continue;

                var mu = MigrationJobContext.GetMigrationUnit(summary.Id);
                if (mu != null)
                {
                    mu.ParentJob = job;
                    units.Add(mu);
                }
            }
            return units;
        }

        public static bool AddMigrationUnits(
            List<MigrationUnit> unitsToAdd,
            MigrationJob job,
            MigrationLog log = null)
        {
            var newUnits = unitsToAdd
                .Where(mu => !job.Tables
                    .Any(summary => summary.Id == MigrationUtilities.GenerateMigrationUnitId(
                        mu.KeyspaceName, mu.TableName)))
                .ToList();

            if (newUnits.Count > 0)
            {
                log?.WriteLine(
                    $"Adding {newUnits.Count} migration units",
                    LogType.Debug);

                foreach (var mu in newUnits)
                {
                    if (!MigrationJobContext.SaveMigrationUnit(mu, false))
                    {
                        log?.WriteLine(
                            $"Warning: failed to save migration unit {mu.KeyspaceName}.{mu.TableName}",
                            LogType.Warning);
                    }
                    AddMigrationUnit(mu, job);
                }
                MigrationJobContext.SaveMigrationJob(job);
            }
            return true;
        }

        private static void AddMigrationUnit(
            MigrationUnit mu, MigrationJob job)
        {
            if (job == null) return;
            job.Tables ??= new List<MigrationUnitBasic>();

            if (job.Tables.Find(m => m.Id == mu.Id) != null)
                return;

            mu.ParentJob = job;
            job.Tables.Add(MigrationUnitMapper.ToSummary(mu));
        }
    }
}
