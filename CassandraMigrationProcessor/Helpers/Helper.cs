using Newtonsoft.Json;
using CassandraMigrationProcessor.Context;
using CassandraMigrationProcessor.Models;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using System.Threading.Tasks;

#pragma warning disable CS8600
#pragma warning disable CS8602
#pragma warning disable CS8604

namespace CassandraMigrationProcessor
{
    public static class Helper
    {
        static string _workingFolder = string.Empty;

        /// <summary>
        /// Extract the host from a contact point string.
        /// For Cassandra, the contact point is already the host.
        /// </summary>
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
            return mu.SourceStatus == CollectionStatus.OK
                || mu.SourceStatus == CollectionStatus.Failed;
        }

        #region Logging

        public static void LogToFile(
            string message,
            string fileName = "AutoStartLog.txt")
        {
            try
            {
                string path = Path.Combine(
                    Helper.GetWorkingFolder(), fileName);
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

        #endregion

        public static string GetWorkingFolder()
        {
            if (!string.IsNullOrEmpty(_workingFolder))
                return _workingFolder;

            if (!IsWindows())
            {
                _workingFolder =
                    $"{Environment.GetEnvironmentVariable("ResourceDrive")}/" +
                    $"{MigrationJobContext.AppId}/";
                if (!Directory.Exists(_workingFolder))
                    Directory.CreateDirectory(_workingFolder);
                Console.WriteLine($"WorkingFolder (Linux): {_workingFolder}");
                return _workingFolder;
            }

            if (Directory.Exists(
                $"{Path.GetTempPath()}migrationjobs"))
            {
                _workingFolder = Path.GetTempPath();
                Console.WriteLine($"WorkingFolder (Temp): {_workingFolder}");
                return _workingFolder;
            }

            string homePath =
                Environment.GetEnvironmentVariable("ResourceDrive");

            if (string.IsNullOrEmpty(homePath))
                _workingFolder = Path.GetTempPath();

            if (!string.IsNullOrEmpty(homePath)
                && Directory.Exists(
                    Path.Combine(homePath, "home\\")))
            {
                _workingFolder = Path.Combine(homePath, "home\\");
            }

            Console.WriteLine($"WorkingFolder (Win): {_workingFolder} (ResourceDrive={homePath})");
            return _workingFolder;
        }

        public static bool IsWindows()
        {
            return Environment.OSVersion.Platform
                == PlatformID.Win32NT;
        }

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
            if (job?.MigrationUnitBasics == null
                || job.MigrationUnitBasics.Count == 0)
                return false;

            return job.MigrationUnitBasics
                .Where(mu => IsMigrationUnitValid(mu))
                .All(mu => mu.CopyComplete);
        }

        public static bool AnyValidTable(MigrationJob job)
        {
            if (job?.MigrationUnitBasics == null)
                return false;
            return job.MigrationUnitBasics
                .Any(mu => IsMigrationUnitValid(mu));
        }

        public static List<MigrationUnit> GetMigrationUnitsToMigrate(
            MigrationJob job)
        {
            List<MigrationUnit> units = new();
            if (job?.MigrationUnitBasics == null) return units;

            foreach (var mub in job.MigrationUnitBasics)
            {
                if (!IsMigrationUnitValid(mub)) continue;
                if (mub.CopyComplete) continue;
                if (mub.SkippedDueToMaxRetries) continue;

                var mu = MigrationJobContext.GetMigrationUnit(mub.Id);
                if (mu != null)
                {
                    mu.ParentJob = job;
                    units.Add(mu);
                }
            }
            return units;
        }

        /// <summary>
        /// Parse a namespace string (JSON or CSV format) into CollectionInfo entries.
        /// Returns null if the input is invalid.
        /// </summary>
        private static List<CollectionInfo>? ParseNamespaceEntries(string input)
        {
            if (string.IsNullOrWhiteSpace(input))
                return null;

            // Try JSON format first
            try
            {
                var parsed = JsonConvert
                    .DeserializeObject<List<CollectionInfo>>(input);
                if (parsed != null)
                    return parsed;
            }
            catch { }

            // CSV format: keyspace.table, keyspace.table
            var entries = input.Split(new[] { ',', '\n', '\r', ';' })
                .Select(s => s.Trim())
                .Where(s => !string.IsNullOrEmpty(s))
                .ToList();

            var result = new List<CollectionInfo>();
            foreach (var fullName in entries)
            {
                int dotIdx = fullName.IndexOf('.');
                if (dotIdx <= 0 || dotIdx == fullName.Length - 1)
                    return null; // invalid entry

                result.Add(new CollectionInfo
                {
                    KeyspaceName = fullName.Substring(0, dotIdx).Trim(),
                    TableName = fullName.Substring(dotIdx + 1).Trim()
                });
            }
            return result;
        }

        public static async Task<List<MigrationUnit>>
            PopulateJobTablesAsync(
                MigrationJob job,
                string namespacesToMigrate)
        {
            List<MigrationUnit> unitsToAdd = new();
            if (string.IsNullOrWhiteSpace(namespacesToMigrate))
                return unitsToAdd;

            // Try JSON format first
            List<CollectionInfo>? loadedObject = null;
            try
            {
                loadedObject = JsonConvert
                    .DeserializeObject<List<CollectionInfo>>(
                        namespacesToMigrate);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[WARN] PopulateJobTablesAsync JSON parse failed, trying CSV: {ex.Message}");
            }

            if (loadedObject != null)
            {
                foreach (var item in loadedObject)
                {
                    var srcKs = item.KeyspaceName.Trim();
                    var srcTbl = item.TableName.Trim();
                    var tgtKs =
                        string.IsNullOrWhiteSpace(item.TargetKeyspaceName)
                        ? srcKs : item.TargetKeyspaceName.Trim();
                    var tgtTbl =
                        string.IsNullOrWhiteSpace(item.TargetTableName)
                        ? srcTbl : item.TargetTableName.Trim();

                    if (!unitsToAdd.Any(x =>
                        x.KeyspaceName == srcKs
                        && x.TableName == srcTbl))
                    {
                        var mu = new MigrationUnit(
                            job, srcKs, srcTbl,
                            new List<MigrationChunk>());
                        mu.TargetKeyspaceName = tgtKs;
                        mu.TargetTableName = tgtTbl;
                        mu.SourceStatus = CollectionStatus.OK;
                        unitsToAdd.Add(mu);
                    }
                }
            }
            else
            {
                // CSV format: keyspace.table, keyspace.table
                var entries = namespacesToMigrate
                    .Split(',')
                    .Select(s => s.Trim())
                    .Where(s => !string.IsNullOrEmpty(s));

                foreach (var fullName in entries)
                {
                    int dotIdx = fullName.IndexOf('.');
                    if (dotIdx <= 0
                        || dotIdx == fullName.Length - 1) continue;

                    string ks = fullName.Substring(0, dotIdx).Trim();
                    string tbl = fullName.Substring(dotIdx + 1).Trim();

                    if (!unitsToAdd.Any(x =>
                        x.KeyspaceName == ks && x.TableName == tbl))
                    {
                        var mu = new MigrationUnit(
                            job, ks, tbl,
                            new List<MigrationChunk>());
                        mu.SourceStatus = CollectionStatus.OK;
                        unitsToAdd.Add(mu);
                    }
                }
            }

            return unitsToAdd;
        }

        public static bool AddMigrationUnits(
            List<MigrationUnit> unitsToAdd,
            MigrationJob job,
            Log log = null)
        {
            var newUnits = unitsToAdd
                .Where(mu => !job.MigrationUnitBasics
                    .Any(mub => mub.Id == GenerateMigrationUnitId(
                        mu.KeyspaceName, mu.TableName)))
                .ToList();

            if (newUnits.Count > 0)
            {
                log?.WriteLine(
                    $"Adding {newUnits.Count} migration units",
                    LogType.Debug);

                foreach (var mu in newUnits)
                {
                    MigrationJobContext.SaveMigrationUnit(mu, false);
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
            if (job.MigrationUnitBasics == null)
                job.MigrationUnitBasics = new List<MigrationUnitBasic>();

            if (job.MigrationUnitBasics.Find(m => m.Id == mu.Id) != null)
                return;

            mu.ParentJob = job;
            job.MigrationUnitBasics.Add(mu.GetBasic());
        }

        public static Tuple<bool, string, string> ValidateNamespaceFormat(
            string input, JobType jobType)
        {
            if (string.IsNullOrWhiteSpace(input))
                return Tuple.Create(false, string.Empty,
                    "Namespaces cannot be null or empty.");

            var parsed = ParseNamespaceEntries(input);
            if (parsed == null)
                return Tuple.Create(false, string.Empty,
                    "Invalid format. Expected JSON array or CSV of keyspace.table entries.");

            foreach (var item in parsed)
            {
                if (string.IsNullOrWhiteSpace(item.KeyspaceName)
                    || string.IsNullOrWhiteSpace(item.TableName))
                {
                    return Tuple.Create(false, string.Empty,
                        "Each entry must have KeyspaceName and TableName.");
                }
            }

            // Re-serialize to normalized JSON if the original was JSON
            List<CollectionInfo>? jsonCheck = null;
            try { jsonCheck = JsonConvert.DeserializeObject<List<CollectionInfo>>(input); }
            catch { }

            string normalizedOutput = jsonCheck != null
                ? JsonConvert.SerializeObject(parsed)
                : input;

            return Tuple.Create(true, normalizedOutput, string.Empty);
        }
    }
}
