using Cassandra;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Numerics;
using System.Text.Json;

namespace CassandraMigrationProcessor.Processors
{
    public partial class ChangeFeedProcessor
    {
        /// <summary>
        /// Process a single FFCF row (shared by single and
        /// parallel paths). Handles insert/replace/delete.
        /// </summary>
        private void ProcessFfcfRow(
            Row row,
            MigrationUnit mu,
            PreparedStatement ps,
            List<string> colNames,
            PreparedStatement? deletePs,
            List<string>? deletePkNames,
            List<(string Name, string Type, string Kind, string ClusteringOrder, int Position)> userColumns,
            List<(string Name, string Type, string Kind, string ClusteringOrder, int Position)> pkColumns,
            ref int insertCount,
            ref int updateCount,
            ref int deleteCount)
        {
            var json = row.GetValue<string>("[json]");
            var doc = JsonDocument.Parse(json);
            var root = doc.RootElement;

            string opType;
            if (root.TryGetProperty("__sys_metadata", out var sysMeta)
                && sysMeta.ValueKind != JsonValueKind.Null)
            {
                opType = sysMeta
                    .GetProperty("operationType")
                    .GetString() ?? "create";
            }
            else
            {
                var snippet = json.Length > 200
                    ? json.Substring(0, 200) : json;
                _log.WriteLine(
                    "ChangeFeed: __sys_metadata missing. JSON: "
                    + snippet, LogType.Error);
                throw new InvalidOperationException(
                    "FFCF document missing __sys_metadata.");
            }

            bool isRowDelete = false;
            if (root.TryGetProperty(
                "__sys_rw_tmbstn", out var rwTombstone))
            {
                isRowDelete = rwTombstone.ValueKind
                    != JsonValueKind.Null;
            }
            else
            {
                var snippet = json.Length > 200
                    ? json.Substring(0, 200) : json;
                _log.WriteLine(
                    "ChangeFeed: __sys_rw_tmbstn missing. JSON: "
                    + snippet, LogType.Error);
                throw new InvalidOperationException(
                    "FFCF document missing __sys_rw_tmbstn.");
            }

            if (isRowDelete
                && deletePs != null
                && deletePkNames != null)
            {
                var pkValues =
                    new object[deletePkNames.Count];
                for (int i = 0; i < deletePkNames.Count; i++)
                {
                    pkValues[i] = ExtractJsonValue(
                        root, deletePkNames[i],
                        pkColumns.First(c =>
                            c.Name == deletePkNames[i]).Type);
                }
                _targetSession!.ExecuteAsync(
                    deletePs.Bind(pkValues))
                    .GetAwaiter().GetResult();
                deleteCount++;
            }
            else
            {
                var values = new object[colNames.Count];
                for (int i = 0; i < colNames.Count; i++)
                {
                    values[i] = ExtractJsonValue(
                        root, colNames[i],
                        userColumns.First(c =>
                            c.Name == colNames[i]).Type);
                }
                _targetSession!.ExecuteAsync(ps.Bind(values))
                    .GetAwaiter().GetResult();
                if (opType == "replace")
                    updateCount++;
                else
                    insertCount++;
            }
        }

        /// <summary>
        /// Extract a typed value from a JSON element based on
        /// the Cassandra column type.
        /// </summary>
        private static object ExtractJsonValue(
            JsonElement root,
            string columnName,
            string cassandraType)
        {
            if (!root.TryGetProperty(columnName, out var el)
                || el.ValueKind == JsonValueKind.Null)
                return null!;

            var lowerType = cassandraType.ToLowerInvariant();

            // Scalar types
            if (lowerType == "uuid" || lowerType == "timeuuid")
                return Guid.Parse(el.GetString()!);
            if (lowerType == "int")
                return el.GetInt32();
            if (lowerType == "bigint" || lowerType == "counter")
                return el.GetInt64();
            if (lowerType == "smallint")
                return (short)el.GetInt32();
            if (lowerType == "tinyint")
                return (sbyte)el.GetInt32();
            if (lowerType == "float")
                return el.GetSingle();
            if (lowerType == "double")
                return el.GetDouble();
            if (lowerType == "decimal")
                return el.GetDecimal();
            if (lowerType == "boolean")
                return el.GetBoolean();
            if (lowerType == "timestamp")
                return DateTimeOffset.Parse(el.GetString()!);
            if (lowerType == "date")
                return Cassandra.LocalDate.Parse(
                    el.GetString()!);
            if (lowerType == "time")
                return Cassandra.LocalTime.Parse(
                    el.GetString()!);
            if (lowerType == "text" || lowerType == "varchar"
                || lowerType == "ascii")
                return el.GetString()!;
            if (lowerType == "blob")
            {
                var blobStr = el.GetString() ?? "";
                if (blobStr.StartsWith("0x",
                    StringComparison.OrdinalIgnoreCase))
                {
                    // FFCF returns blob as hex "0x01ab..."
                    var hex = blobStr.Substring(2);
                    var bytes = new byte[hex.Length / 2];
                    for (int i = 0; i < bytes.Length; i++)
                        bytes[i] = Convert.ToByte(
                            hex.Substring(i * 2, 2), 16);
                    return bytes;
                }
                return Convert.FromBase64String(blobStr);
            }
            if (lowerType == "inet")
                return IPAddress.Parse(
                    el.GetString()!);
            if (lowerType == "varint")
            {
                if (el.TryGetInt64(out var v))
                    return new BigInteger(v);
                return BigInteger.Parse(
                    el.GetRawText());
            }

            // Collection types: set<T>, list<T>, map<K,V>,
            // and frozen<> wrappers.
            if (lowerType.StartsWith("set<")
                || lowerType.StartsWith("frozen<set<"))
            {
                var innerType = ExtractInnerType(lowerType);
                return ParseJsonArray(el, innerType)
                    .ToHashSet();
            }
            if (lowerType.StartsWith("list<")
                || lowerType.StartsWith("frozen<list<"))
            {
                var innerType = ExtractInnerType(lowerType);
                return ParseJsonArray(el, innerType);
            }
            if (lowerType.StartsWith("map<")
                || lowerType.StartsWith("frozen<map<"))
            {
                return ParseJsonMap(el, lowerType);
            }

            // Fallback for unknown/UDT types
            return el.GetRawText();
        }

        /// <summary>
        /// Extract the element type from a collection type
        /// string like "set&lt;text&gt;" or
        /// "frozen&lt;set&lt;int&gt;&gt;".
        /// </summary>
        private static string ExtractInnerType(string cqlType)
        {
            // Strip frozen<...> wrapper if present
            var t = cqlType;
            if (t.StartsWith("frozen<"))
                t = t.Substring(7, t.Length - 8); // remove frozen< and >

            // Now t is e.g. "set<text>" or "list<int>"
            var open = t.IndexOf('<');
            var close = t.LastIndexOf('>');
            if (open >= 0 && close > open)
                return t.Substring(open + 1, close - open - 1)
                    .Trim();
            return "text";
        }

        /// <summary>
        /// Parse a JSON array into a List of typed objects.
        /// </summary>
        private static List<object> ParseJsonArray(
            JsonElement el, string innerType)
        {
            var list = new List<object>();
            if (el.ValueKind != JsonValueKind.Array)
                return list;
            foreach (var item in el.EnumerateArray())
            {
                list.Add(ConvertScalar(item, innerType));
            }
            return list;
        }

        /// <summary>
        /// Parse a JSON object into a Dictionary for map
        /// types. Keys are always strings in JSON; values
        /// are converted based on the map's value type.
        /// </summary>
        private static Dictionary<string, object>
            ParseJsonMap(JsonElement el, string cqlType)
        {
            var dict = new Dictionary<string, object>();
            if (el.ValueKind != JsonValueKind.Object)
                return dict;

            // Extract value type from "map<text, int>"
            var inner = ExtractInnerType(cqlType);
            // inner is "text, int" — split on comma
            var parts = inner.Split(',');
            var valType = parts.Length > 1
                ? parts[1].Trim() : "text";

            foreach (var prop in el.EnumerateObject())
            {
                dict[prop.Name] =
                    ConvertScalar(prop.Value, valType);
            }
            return dict;
        }

        /// <summary>
        /// Convert a single JSON element to a .NET scalar
        /// matching the CQL type.
        /// </summary>
        private static object ConvertScalar(
            JsonElement el, string cqlType)
        {
            if (el.ValueKind == JsonValueKind.Null)
                return null!;
            var t = cqlType.Trim().ToLowerInvariant();
            if (t == "int") return el.GetInt32();
            if (t == "bigint") return el.GetInt64();
            if (t == "smallint") return (short)el.GetInt32();
            if (t == "tinyint") return (sbyte)el.GetInt32();
            if (t == "float") return el.GetSingle();
            if (t == "double") return el.GetDouble();
            if (t == "decimal") return el.GetDecimal();
            if (t == "boolean") return el.GetBoolean();
            if (t == "uuid" || t == "timeuuid")
                return Guid.Parse(el.GetString()!);
            if (t == "timestamp")
                return DateTimeOffset.Parse(el.GetString()!);
            if (t == "blob")
                return Convert.FromBase64String(
                    el.GetString() ?? "");
            if (t == "inet")
                return IPAddress.Parse(
                    el.GetString()!);
            // Default: text/varchar/ascii
            return el.GetString() ?? el.GetRawText();
        }
    }
}
