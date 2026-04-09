using Newtonsoft.Json;
using CassandraMigrationProcessor.Models;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

#pragma warning disable CS8600
#pragma warning disable CS8602
#pragma warning disable CS8604

namespace CassandraMigrationProcessor.Helpers
{
    public static class NamespaceParser
    {
        /// <summary>
        /// Attempts JSON deserialization, returning null on failure.
        /// Consolidates the repeated try-parse-catch pattern.
        /// </summary>
        private static List<TableMapping>? TryDeserializeJson(string input, string context)
        {
            try { return JsonConvert.DeserializeObject<List<TableMapping>>(input); }
            catch (Exception ex)
            {
                Console.WriteLine($"[WARN] {context}: {ex.Message}");
                return null;
            }
        }
        /// <summary>
        /// Parse a namespace string (JSON or CSV format) into TableMapping entries.
        /// Returns null if the input is invalid.
        /// </summary>
        private static List<TableMapping>? ParseNamespaceEntries(string input)
        {
            if (string.IsNullOrWhiteSpace(input))
                return null;

            // Try JSON format first
            var parsed = TryDeserializeJson(input, "JSON parse failed during namespace parsing");
            if (parsed != null)
                return parsed;

            var entries = input.Split(new[] { ',', '\n', '\r', ';' })
                .Select(s => s.Trim())
                .Where(s => !string.IsNullOrEmpty(s))
                .ToList();

            var result = new List<TableMapping>();
            foreach (var fullName in entries)
            {
                int dotIdx = fullName.IndexOf('.');
                if (dotIdx <= 0 || dotIdx == fullName.Length - 1)
                    return null; // invalid entry

                result.Add(new TableMapping
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
            List<TableMapping>? loadedObject = TryDeserializeJson(
                namespacesToMigrate, "PopulateJobTablesAsync JSON parse failed, trying CSV");

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
                        mu.SourceStatus = TableStatus.OK;
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

                    string keyspace = fullName.Substring(0, dotIdx).Trim();
                    string table = fullName.Substring(dotIdx + 1).Trim();

                    if (!unitsToAdd.Any(x =>
                        x.KeyspaceName == keyspace && x.TableName == table))
                    {
                        var mu = new MigrationUnit(
                            job, keyspace, table,
                            new List<MigrationChunk>());
                        mu.SourceStatus = TableStatus.OK;
                        unitsToAdd.Add(mu);
                    }
                }
            }

            return unitsToAdd;
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
            var jsonCheck = TryDeserializeJson(input, "JSON re-parse failed during validation");

            string normalizedOutput = jsonCheck != null
                ? JsonConvert.SerializeObject(parsed)
                : input;

            return Tuple.Create(true, normalizedOutput, string.Empty);
        }
    }
}
