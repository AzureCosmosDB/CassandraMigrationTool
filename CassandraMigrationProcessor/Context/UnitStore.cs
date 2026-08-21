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

        return MigrationJobContext.Instance.MigrationUnitsCache == null
            ? GetFromStorage(jobId, unitId)
            : MigrationJobContext.Instance.MigrationUnitsCache.GetMigrationUnit(unitId, jobId);
    }

    /// <summary>Persists a migration unit to disk and optionally updates its parent job.</summary>
    public static bool SaveUnit(
        TableMigration mu, bool updateParent)
    {
        ArgumentNullException.ThrowIfNull(mu);

        if (mu.ParentJob == null && MigrationJobContext.Instance.CurrentlyActiveJob != null)
            mu.ParentJob =
                MigrationJobContext.Instance.CurrentlyActiveJob;

        if (mu.ParentJob != null && updateParent)
            TableMigrationMapper.UpdateParentJob(mu);

        lock (_writeMULock)
        {
            JsonStore.Write(
                JobStore.GetUnitDocumentPath(mu.JobId, mu.Id), mu);
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
    }

    /// <summary>Removes a migration unit from its parent job and deletes it from storage.</summary>
    public static bool RemoveUnit(TableMigrationSummary unit)
    {
        if (unit == null || unit.ParentJob == null)
            return false;

        var job = unit.ParentJob;
        var index = job.Tables
            .FindIndex(mu => mu.Id == unit.Id);
        if (index == -1) return false;

        job.Tables.RemoveAt(index);

        MigrationJobContext.Instance.SaveMigrationJob(job);

        var filePath = JobStore.GetUnitDocumentPath(unit.JobId, unit.Id);
        if (!MigrationJobContext.Instance.Store.Delete(filePath))
            throw new IOException(
                $"Failed to delete migration unit '{filePath}'.");

        return true;
    }

    public static TableMigration GetFromStorage(
        string jobId, string unitId)
    {
        return JsonStore.Read<TableMigration>(
            JobStore.GetUnitDocumentPath(jobId, unitId));
    }

    public static List<TableMigration> GetMigrationUnitsToMigrate(
        Job job)
    {
        List<TableMigration> units = new();
        if (job == null) return units;

        foreach (var summary in job.Tables.Where(summary => summary.IsValid))
        {
            // For online jobs, include CopyComplete tables too — the
            // merged DataCopyWorker re-seeds their feed ranges in
            // Replay phase to keep tailing the change feed.
            if (summary.CopyComplete && !job.IsOnline) continue;
            if (summary.SkippedDueToMaxRetries) continue;

            var mu = GetUnit(summary.Id);
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
        bool allSaved = true;

        var newUnits = unitsToAdd
            .Where(mu => !job.Tables
                .Any(summary => summary.Id == TableMigration.GenerateId(
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
                    allSaved = false;
                    log?.WriteLine(
                        $"Warning: failed to save migration unit {mu.KeyspaceName}.{mu.TableName}",
                        LogType.Warning);
                }
                AddMigrationUnit(mu, job);
            }
            MigrationJobContext.Instance.SaveMigrationJob(job);
        }
        return allSaved;
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
