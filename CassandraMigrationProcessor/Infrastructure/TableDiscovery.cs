using Newtonsoft.Json;
using CassandraMigrationProcessor.CassandraDriver;
using CassandraMigrationProcessor.Models;

#pragma warning disable CS8600
#pragma warning disable CS8602
#pragma warning disable CS8604

namespace CassandraMigrationProcessor.Infrastructure;

/// <summary>
/// Parses the user-supplied namespace specification (JSON or CSV) and
/// expands it into the concrete list of <see cref="TableMapping"/> entries
/// to migrate, querying source keyspaces/tables as needed.
/// </summary>
public static class TableDiscovery
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
            string keyspace;
            string table;
            try
            {
                // CQL-aware split: handles both bare 'foo.bar' and
                // quoted forms like 'foo."MixedCase_Table-1"' or
                // '"My-KS"."Some.Table"' — the surrounding "..." is
                // stripped and ""-escapes are resolved.
                (keyspace, table) = CqlIdentifier.SplitQualifiedName(fullName);
            }
            catch (ArgumentException)
            {
                return null; // invalid entry
            }

            result.Add(new TableMapping
            {
                KeyspaceName = keyspace,
                TableName = table
            });
        }
        return result;
    }

    public static async Task<List<TableMigration>>
        PopulateJobTablesAsync(
            Job job,
            string namespacesToMigrate)
    {
        List<TableMigration> unitsToAdd = new();
        if (string.IsNullOrWhiteSpace(namespacesToMigrate))
            return unitsToAdd;

        // Try JSON format first
        List<TableMapping>? loadedObject = TryDeserializeJson(
                namespacesToMigrate, "PopulateJobTablesAsync JSON parse failed, trying CSV");

        if (loadedObject != null)
        {
            foreach (var item in loadedObject)
            {
                // CqlIdentifier.Unquote strips surrounding "..." and
                // resolves "" escapes; bare names pass through unchanged.
                var srcKs = CqlIdentifier.Unquote(item.KeyspaceName);
                var srcTbl = CqlIdentifier.Unquote(item.TableName);
                var tgtKs =
                    string.IsNullOrWhiteSpace(item.TargetKeyspaceName)
                    ? srcKs : CqlIdentifier.Unquote(item.TargetKeyspaceName);
                var tgtTbl =
                    string.IsNullOrWhiteSpace(item.TargetTableName)
                    ? srcTbl : CqlIdentifier.Unquote(item.TargetTableName);

                if (!unitsToAdd.Any(x =>
                    x.KeyspaceName == srcKs
                    && x.TableName == srcTbl))
                {
                    var mu = new TableMigration(
                        job, srcKs, srcTbl);
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
                string keyspace;
                string table;
                try
                {
                    // CQL-aware split: tolerates 'foo."MixedCase-1"' etc.
                    (keyspace, table) = CqlIdentifier.SplitQualifiedName(fullName);
                }
                catch (ArgumentException)
                {
                    continue; // skip malformed entries
                }

                if (!unitsToAdd.Any(x =>
                    x.KeyspaceName == keyspace && x.TableName == table))
                {
                    var mu = new TableMigration(
                        job, keyspace, table);
                    mu.SourceStatus = TableStatus.OK;
                    unitsToAdd.Add(mu);
                }
            }
        }

        return unitsToAdd;
    }

    public static Tuple<bool, string, string> ValidateNamespaceFormat(
        string input)
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

