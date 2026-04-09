using CassandraMigrationProcessor.Context;
using CassandraMigrationProcessor.Models;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Security.Cryptography;
using System.Text;

#pragma warning disable CS8600
#pragma warning disable CS8602
#pragma warning disable CS8604

namespace CassandraMigrationProcessor.Helpers
{
    public static class MigrationHelper
    {
        public static bool IsOnline(MigrationJob job)
        {
            if (job == null) return false;
            return job.CDCMode != CDCMode.Offline;
        }

        public static bool IsMigrationUnitValid(MigrationUnitBasic mu)
        {
            // Allow both OK and Failed status — Failed tables
            // are retried on resume (e.g. after token expiry).
            // Only NotFound tables are truly invalid.
            return mu.SourceStatus == TableStatus.OK
                || mu.SourceStatus == TableStatus.Failed;
        }

        #region Logging

        public static void LogToFile(
            string message,
            string fileName = "AutoStartLog.txt")
        {
            try
            {
                string path = Path.Combine(
                    WorkingFolderResolver.GetWorkingFolder(), fileName);
                string timestamp =
                    DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.fff");
                string logEntry =
                    $"[{timestamp} UTC] {message}{Environment.NewLine}";

                System.IO.File.AppendAllText(path, logEntry);
            }
            catch (Exception ex)
            {
                Console.Error.WriteLine($"[WARN] LogToFile failed: {ex.Message}");
            }
        }

        /// <summary>
        /// Disposes an object, swallowing and logging any exception.
        /// Use instead of try { obj?.Dispose(); } catch { ... } blocks.
        /// </summary>
        public static void SafeDispose(IDisposable? obj, string name)
        {
            try { obj?.Dispose(); }
            catch (Exception ex)
            {
                Console.Error.WriteLine($"[WARN] {name} dispose failed: {ex.Message}");
            }
        }

        /// <summary>
        /// Executes an action, returning a fallback on failure.
        /// Shared helper for the repeated try/catch-warn-return pattern.
        /// </summary>
        public static T SafeExecute<T>(Func<T> action, T fallback, string operation)
        {
            try { return action(); }
            catch (Exception ex)
            {
                Console.WriteLine($"[WARN] {operation}: {ex.Message}");
                return fallback;
            }
        }

        #endregion

        public static string GenerateMigrationUnitId(
            string keyspaceName, string tableName)
        {
            using (var sha = SHA256.Create())
            {
                byte[] hashBytes = sha.ComputeHash(
                    Encoding.UTF8.GetBytes(
                        $"{keyspaceName}.{tableName}"));
                return BitConverter.ToString(hashBytes)
                    .Replace("-", "").Substring(0, 16).ToLower();
            }
        }

        public static long GetMigrationUnitRowCount(MigrationUnit mu)
        {
            return Math.Max(mu.ActualRowCount, mu.EstimatedRowCount);
        }

        public static (long Total, long Inserted, long Skipped, long Failed)
            GetProcessedTotals(MigrationUnit mu)
        {
            long skipped = mu.MigrationChunks?
                .Sum(c => c.SkippedAsDuplicateCount) ?? 0;
            long inserted = (mu.MigrationChunks?
                .Sum(c => c.TargetInsertedRowCount) ?? 0) - skipped;
            long failed = mu.MigrationChunks?
                .Sum(c => c.TargetFailedRowCount) ?? 0;
            long total = inserted + skipped + failed;
            return (total, inserted, skipped, failed);
        }

        public static string GetTimestampDiff(DateTime timestamp)
        {
            var lag = DateTime.UtcNow - timestamp;
            if (lag.TotalSeconds < 0) return "Invalid";
            if (lag.TotalSeconds < 60)
                return $"{(int)lag.TotalSeconds} sec";
            else if (lag.TotalMinutes < 60)
                return $"{(int)lag.TotalMinutes} min {(int)lag.Seconds} sec";
            else
                return $"{(int)lag.TotalHours}h {(int)lag.Minutes}m";
        }

        public static bool IsOfflineJobCompleted(MigrationJob job)
        {
            if (job?.Tables == null
                || job.Tables.Count == 0)
                return false;

            return job.Tables
                .Where(mu => IsMigrationUnitValid(mu))
                .All(mu => mu.CopyComplete);
        }

        public static bool AnyValidTable(MigrationJob job)
        {
            if (job?.Tables == null)
                return false;
            return job.Tables
                .Any(mu => IsMigrationUnitValid(mu));
        }

        public static List<MigrationUnit> GetMigrationUnitsToMigrate(
            MigrationJob job)
        {
            List<MigrationUnit> units = new();
            if (job?.Tables == null) return units;

            foreach (var summary in job.Tables)
            {
                if (!IsMigrationUnitValid(summary)) continue;
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
            MigrationLog MigrationLog = null)
        {
            var newUnits = unitsToAdd
                .Where(mu => !job.Tables
                    .Any(summary => summary.Id == GenerateMigrationUnitId(
                        mu.KeyspaceName, mu.TableName)))
                .ToList();

            if (newUnits.Count > 0)
            {
                MigrationLog?.WriteLine(
                    $"Adding {newUnits.Count} migration units",
                    LogType.Debug);

                foreach (var mu in newUnits)
                {
                    if (!MigrationJobContext.SaveMigrationUnit(mu, false))
                    {
                        MigrationLog?.WriteLine(
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
            if (job.Tables == null)
                job.Tables = new List<MigrationUnitBasic>();

            if (job.Tables.Find(m => m.Id == mu.Id) != null)
                return;

            mu.ParentJob = job;
            job.Tables.Add(mu.ToSummary());
        }
    }
}
