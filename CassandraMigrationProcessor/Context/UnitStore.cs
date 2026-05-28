using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using CassandraMigrationProcessor.Infrastructure;
using CassandraMigrationProcessor.Models;
namespace CassandraMigrationProcessor.Context;

/// <summary>
/// Persistence gateway for <see cref="TableMigration"/> documents (one JSON
/// file per unit under the job folder). Handles add / save / remove and
/// keeps the parent <see cref="Job"/>'s table summaries in sync.
/// </summary>
public static class UnitStore
{
    private static readonly object _writeMULock = new object();

    /// <summary>Retrieves a migration unit by ID, using the in-memory cache when available.</summary>
    public static TableMigration GetUnit(
        string unitId, string jobId = null)
    {
        if (string.IsNullOrEmpty(jobId)
            && MigrationJobContext.Instance.CurrentlyActiveJob != null)
        {
            jobId = MigrationJobContext.Instance.CurrentlyActiveJob.Id;
        }

        if (MigrationJobContext.Instance.MigrationUnitsCache == null)
            return GetFromStorage(jobId, unitId);
        else
            return MigrationJobContext.Instance.MigrationUnitsCache
                .GetMigrationUnit(unitId, jobId);
    }

    /// <summary>Persists a migration unit to disk and optionally updates its parent job.</summary>
    public static bool SaveUnit(
        TableMigration mu, bool updateParent)
    {
        return MigrationUtilities.SafeExecute(() =>
        {
            if (mu == null) return false;

            if (mu.ParentJob == null && MigrationJobContext.Instance.CurrentlyActiveJob != null)
                mu.ParentJob =
                    MigrationJobContext.Instance.CurrentlyActiveJob;

            if (mu.ParentJob != null && updateParent)
                TableMigrationMapper.UpdateParentJob(mu);

            lock (_writeMULock)
            {
                var muFilePath = Path.Combine(
                    JobStore.JobsFolder, mu.JobId,
                    $"{mu.Id}.json");
                string muJson =
                    JsonConvert.SerializeObject(
                        mu, Formatting.Indented);
                MigrationJobContext.Instance.Store.Write(
                    muFilePath, muJson);
            }

            if (MigrationJobContext.Instance.CurrentlyActiveJob != null
                && updateParent)
            {
                JobStore.PersistActiveJobUnderLock();
            }

            if (MigrationJobContext.Instance.MigrationUnitsCache != null)
                MigrationJobContext.Instance.MigrationUnitsCache
                    .UpdateMigrationUnit(mu);

            return true;
        }, false, "SaveUnit");
    }

    /// <summary>Removes a migration unit from its parent job and deletes it from storage.</summary>
    public static bool RemoveUnit(TableMigrationSummary unit)
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

            if (!MigrationJobContext.Instance.SaveMigrationJob(job))
                return false;

            var filePath = Path.Combine(
                JobStore.JobsFolder, unit.JobId,
                $"{unit.Id}.json");
            MigrationJobContext.Instance.Store.Delete(filePath);

            return true;
        }, false, "RemoveUnit");
    }

    public static TableMigration GetFromStorage(
        string jobId, string unitId)
    {
        return MigrationUtilities.SafeExecute(() =>
        {
            var filePath = Path.Combine(
                JobStore.JobsFolder, jobId, $"{unitId}.json");
            string json = MigrationJobContext.Instance.Store
                .Read(filePath);
            return JsonConvert
                .DeserializeObject<TableMigration>(json);
        }, (TableMigration)null, $"GetFromStorage({jobId}, {unitId})");
    }

    public static List<TableMigration> GetMigrationUnitsToMigrate(
        Job job)
    {
        List<TableMigration> units = new();
        if (job == null) return units;

        foreach (var summary in job.Tables)
        {
            if (!MigrationUtilities.IsMigrationUnitValid(summary)) continue;
            // For online jobs, include CopyComplete tables too — the
            // merged DataCopyWorker re-seeds their feed ranges in
            // Replay phase to keep tailing the change feed.
            if (summary.CopyComplete && !MigrationUtilities.IsOnline(job)) continue;
            if (summary.SkippedDueToMaxRetries) continue;

            var mu = MigrationJobContext.Instance.GetMigrationUnit(summary.Id);
            if (mu != null)
            {
                mu.ParentJob = job;
                units.Add(mu);
            }
        }
        return units;
    }

    public static bool AddMigrationUnits(
        List<TableMigration> unitsToAdd,
        Job job,
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
                if (!MigrationJobContext.Instance.SaveMigrationUnit(mu, false))
                {
                    log?.WriteLine(
                        $"Warning: failed to save migration unit {mu.KeyspaceName}.{mu.TableName}",
                        LogType.Warning);
                }
                AddMigrationUnit(mu, job);
            }
            MigrationJobContext.Instance.SaveMigrationJob(job);
        }
        return true;
    }

    private static void AddMigrationUnit(
        TableMigration mu, Job job)
    {
        if (job == null) return;
        job.Tables ??= new List<TableMigrationSummary>();

        if (job.Tables.Find(m => m.Id == mu.Id) != null)
            return;

        mu.ParentJob = job;
        job.Tables.Add(TableMigrationMapper.ToSummary(mu));
    }
}
