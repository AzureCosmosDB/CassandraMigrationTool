using Cassandra;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using CassandraMigrationProcessor.Helpers;

namespace CassandraMigrationProcessor.Persistence
{
    /// <summary>
    /// Cassandra-backed implementation of PersistenceStorage.
    /// Uses a keyspace with two tables: data_documents and
    /// log_entries, keyed by a normalized string ID.
    /// </summary>
    public class DocumentDBPersistence : PersistenceStorage
    {
        private static ISession? _session;
        private static bool _isInitialized = false;
        private static readonly object _initLock = new object();
        private static string _appId = string.Empty;

        private const string KEYSPACE = "migration_state";
        private const string DATA_TABLE = "data_documents";
        private const string LOG_TABLE = "log_entries";

        public override bool IsInitialized => _isInitialized;

        /// <summary>
        /// Initialize with a Cassandra contact point or
        /// connection string. Creates keyspace and tables
        /// if they don't exist.
        /// </summary>
        public override void Initialize(
            string connectionStringOrPath, string appId)
        {
            if (_isInitialized) return;
            lock (_initLock)
            {
                if (_isInitialized) return;
                _appId = appId ?? string.Empty;

                try
                {
                    string contactPoint =
                        ResolveConnectionString(
                            connectionStringOrPath);

                    var cluster = Cluster.Builder()
                        .AddContactPoint(contactPoint)
                        .Build();

                    _session = cluster.Connect();

                    // Create keyspace
                    _session.ExecuteAsync(
                        new SimpleStatement(
                            $"CREATE KEYSPACE IF NOT EXISTS " +
                            $"\"{KEYSPACE}\" WITH replication = " +
                            $"{{'class':'SimpleStrategy'," +
                            $"'replication_factor':1}}"))
                        .GetAwaiter().GetResult();

                    _session.ChangeKeyspace(KEYSPACE);

                    // Data table
                    _session.ExecuteAsync(
                        new SimpleStatement(
                            $"CREATE TABLE IF NOT EXISTS " +
                            $"\"{DATA_TABLE}\" (" +
                            $"id text PRIMARY KEY, " +
                            $"job_id text, " +
                            $"content text, " +
                            $"updated_at timestamp)"))
                        .GetAwaiter().GetResult();

                    // Log table - partitioned by job_id,
                    // clustered by timestamp
                    _session.ExecuteAsync(
                        new SimpleStatement(
                            $"CREATE TABLE IF NOT EXISTS " +
                            $"\"{LOG_TABLE}\" (" +
                            $"job_id text, " +
                            $"ts timestamp, " +
                            $"content text, " +
                            $"PRIMARY KEY (job_id, ts)) " +
                            $"WITH CLUSTERING ORDER BY " +
                            $"(ts DESC)"))
                        .GetAwaiter().GetResult();

                    _isInitialized = true;
                }
                catch (Exception ex)
                {
                    Helper.LogToFile(
                        $"[DocumentDBPersistence] Init error: " +
                        $"{ex}", "DocumentDBPersistence.txt");
                    throw;
                }
            }
        }

        private static string ResolveConnectionString(
            string input)
        {
            bool isFilePath =
                input.StartsWith("/") ||
                input.StartsWith("\\") ||
                (input.Length > 1 && input[1] == ':');

            if (isFilePath)
            {
                try
                {
                    var cs = File.ReadAllText(input).Trim();
                    if (string.IsNullOrEmpty(cs))
                        throw new InvalidOperationException(
                            $"File {input} is empty.");
                    return cs;
                }
                catch (IOException ex)
                {
                    throw new InvalidOperationException(
                        $"Cannot read {input}: {ex.Message}",
                        ex);
                }
            }
            return input;
        }

        private void EnsureInitialized()
        {
            if (!_isInitialized || _session == null)
                throw new InvalidOperationException(
                    "DocumentDBPersistence not initialized.");
        }

        private static string NormalizeId(string id)
        {
            return $"{_appId}.{id.Replace('\\', '_').Replace('/', '_')}";
        }

        private static string ExtractJobId(string normalizedId)
        {
            var match = System.Text.RegularExpressions.Regex
                .Match(normalizedId,
                    @"migrationjobs_([0-9a-fA-F\-]+)");
            return match.Success
                ? match.Groups[1].Value : string.Empty;
        }

        public override bool UpsertDocument(
            string id, string jsonContent)
        {
            EnsureInitialized();

            if (string.IsNullOrWhiteSpace(id))
                throw new ArgumentException(
                    "ID cannot be null or empty", nameof(id));
            if (string.IsNullOrWhiteSpace(jsonContent))
                throw new ArgumentException(
                    "Content cannot be null or empty",
                    nameof(jsonContent));

            try
            {
                var nid = NormalizeId(id);
                var jobId = ExtractJobId(nid);

                _session!.ExecuteAsync(new SimpleStatement(
                    $"INSERT INTO \"{DATA_TABLE}\" " +
                    $"(id, job_id, content, updated_at) " +
                    $"VALUES (?, ?, ?, ?)",
                    nid, jobId, jsonContent,
                    DateTimeOffset.UtcNow))
                    .GetAwaiter().GetResult();

                return true;
            }
            catch (Exception ex)
            {
                Helper.LogToFile(
                    $"[DocumentDBPersistence] Upsert error " +
                    $"{id}: {ex}",
                    "DocumentDBPersistence.txt");
                return false;
            }
        }

        public override string? ReadDocument(string id)
        {
            EnsureInitialized();

            if (string.IsNullOrWhiteSpace(id))
                throw new ArgumentException(
                    "ID cannot be null or empty", nameof(id));

            try
            {
                var nid = NormalizeId(id);
                var rs = _session!.ExecuteAsync(
                    new SimpleStatement(
                        $"SELECT content FROM " +
                        $"\"{DATA_TABLE}\" WHERE id = ?",
                        nid))
                    .GetAwaiter().GetResult();

                var row = rs.FirstOrDefault();
                return row?.GetValue<string>("content");
            }
            catch (Exception ex)
            {
                Helper.LogToFile(
                    $"[DocumentDBPersistence] Read error " +
                    $"{id}: {ex}",
                    "DocumentDBPersistence.txt");
                return null;
            }
        }

        public override bool DocumentExists(string id)
        {
            EnsureInitialized();
            if (string.IsNullOrWhiteSpace(id)) return false;

            try
            {
                var nid = NormalizeId(id);
                var rs = _session!.ExecuteAsync(
                    new SimpleStatement(
                        $"SELECT id FROM \"{DATA_TABLE}\" " +
                        $"WHERE id = ?", nid))
                    .GetAwaiter().GetResult();
                return rs.FirstOrDefault() != null;
            }
            catch
            {
                return false;
            }
        }

        public override bool DeleteDocument(string id)
        {
            EnsureInitialized();
            if (string.IsNullOrWhiteSpace(id))
                throw new ArgumentException(
                    "ID cannot be null or empty", nameof(id));

            try
            {
                var nid = NormalizeId(id);
                _session!.ExecuteAsync(new SimpleStatement(
                    $"DELETE FROM \"{DATA_TABLE}\" " +
                    $"WHERE id = ?", nid))
                    .GetAwaiter().GetResult();

                // Also delete children (prefix match via
                // ALLOW FILTERING is slow - delete by prefix)
                var prefix = nid + "_";
                var all = ListDocumentIdsRaw();
                foreach (var docId in all)
                {
                    if (docId.StartsWith(prefix))
                    {
                        _session.ExecuteAsync(new SimpleStatement(
                            $"DELETE FROM \"{DATA_TABLE}\" " +
                            $"WHERE id = ?", docId))
                            .GetAwaiter().GetResult();
                    }
                }
                return true;
            }
            catch (Exception ex)
            {
                Helper.LogToFile(
                    $"[DocumentDBPersistence] Delete error " +
                    $"{id}: {ex}",
                    "DocumentDBPersistence.txt");
                return false;
            }
        }

        private List<string> ListDocumentIdsRaw()
        {
            var result = new List<string>();
            try
            {
                var rs = _session!.ExecuteAsync(
                    new SimpleStatement(
                        $"SELECT id FROM \"{DATA_TABLE}\""))
                    .GetAwaiter().GetResult();
                foreach (var row in rs)
                    result.Add(row.GetValue<string>("id"));
            }
            catch { }
            return result;
        }

        public override List<string> ListDocumentIds()
        {
            EnsureInitialized();

            var result = new List<string>();
            try
            {
                var prefix = _appId + ".";
                var all = ListDocumentIdsRaw();
                foreach (var docId in all)
                {
                    if (docId.StartsWith(prefix))
                    {
                        result.Add(
                            docId.Substring(prefix.Length)
                                .Replace('_', '\\'));
                    }
                }
            }
            catch (Exception ex)
            {
                Helper.LogToFile(
                    $"[DocumentDBPersistence] ListIds error: " +
                    $"{ex}", "DocumentDBPersistence.txt");
            }
            return result;
        }

        public override bool TestConnection()
        {
            try
            {
                if (_session == null) return false;
                _session.ExecuteAsync(
                    new SimpleStatement(
                        "SELECT now() FROM system.local"))
                    .GetAwaiter().GetResult();
                return true;
            }
            catch
            {
                return false;
            }
        }

        public override void PushLogEntry(
            string jobId, LogObject logObj)
        {
            EnsureInitialized();
            try
            {
                var nJobId = NormalizeId(
                    $"migrationjobs\\{jobId}");
                string json = JsonConvert.SerializeObject(
                    logObj);

                _session!.ExecuteAsync(new SimpleStatement(
                    $"INSERT INTO \"{LOG_TABLE}\" " +
                    $"(job_id, ts, content) VALUES (?, ?, ?)",
                    nJobId,
                    DateTimeOffset.UtcNow,
                    json))
                    .GetAwaiter().GetResult();
            }
            catch (Exception ex)
            {
                Helper.LogToFile(
                    $"[DocumentDBPersistence] Log push error: " +
                    $"{ex}", "DocumentDBPersistence.txt");
            }
        }

        public override LogBucket ReadLogs(
            string id, out string fileName)
        {
            EnsureInitialized();
            fileName = $"migration_log_{id}.json";

            var bucket = new LogBucket
            {
                Logs = new List<LogObject>()
            };

            try
            {
                var nJobId = NormalizeId(
                    $"migrationjobs\\{id}");
                var rs = _session!.ExecuteAsync(
                    new SimpleStatement(
                        $"SELECT content FROM \"{LOG_TABLE}\" " +
                        $"WHERE job_id = ? LIMIT 500",
                        nJobId))
                    .GetAwaiter().GetResult();

                foreach (var row in rs)
                {
                    var json = row.GetValue<string>("content");
                    if (!string.IsNullOrEmpty(json))
                    {
                        var obj = JsonConvert
                            .DeserializeObject<LogObject>(json);
                        if (obj != null)
                            bucket.Logs.Add(obj);
                    }
                }
            }
            catch (Exception ex)
            {
                Helper.LogToFile(
                    $"[DocumentDBPersistence] ReadLogs error: " +
                    $"{ex}", "DocumentDBPersistence.txt");
            }
            return bucket;
        }

        public override byte[] DownloadLogsAsJsonBytes(
            string id,
            int topEntries = 20,
            int bottomEntries = 230)
        {
            EnsureInitialized();
            try
            {
                var nJobId = NormalizeId(
                    $"migrationjobs\\{id}");
                var all = new List<LogObject>();

                var rs = _session!.ExecuteAsync(
                    new SimpleStatement(
                        $"SELECT content FROM \"{LOG_TABLE}\" " +
                        $"WHERE job_id = ?",
                        nJobId))
                    .GetAwaiter().GetResult();

                foreach (var row in rs)
                {
                    var json = row.GetValue<string>("content");
                    if (!string.IsNullOrEmpty(json))
                    {
                        var obj = JsonConvert
                            .DeserializeObject<LogObject>(json);
                        if (obj != null) all.Add(obj);
                    }
                }

                var selected = new List<LogObject>();
                if (all.Count <= topEntries + bottomEntries)
                {
                    selected = all;
                }
                else
                {
                    selected.AddRange(all.Take(topEntries));
                    selected.AddRange(
                        all.Skip(all.Count - bottomEntries));
                }

                string jsonResult = JsonConvert.SerializeObject(
                    selected, Formatting.Indented);
                return Encoding.UTF8.GetBytes(jsonResult);
            }
            catch (Exception ex)
            {
                Helper.LogToFile(
                    $"[DocumentDBPersistence] DownloadLogs " +
                    $"error: {ex}",
                    "DocumentDBPersistence.txt");
                return Array.Empty<byte>();
            }
        }

        public override int GetLogCount(string id)
        {
            EnsureInitialized();
            try
            {
                var nJobId = NormalizeId(
                    $"migrationjobs\\{id}");
                var rs = _session!.ExecuteAsync(
                    new SimpleStatement(
                        $"SELECT COUNT(*) FROM " +
                        $"\"{LOG_TABLE}\" WHERE job_id = ?",
                        nJobId))
                    .GetAwaiter().GetResult();
                var row = rs.FirstOrDefault();
                return row != null
                    ? (int)row.GetValue<long>(0) : 0;
            }
            catch
            {
                return 0;
            }
        }

        public override byte[] DownloadLogsPaginated(
            string id, int skip, int take)
        {
            EnsureInitialized();
            try
            {
                var nJobId = NormalizeId(
                    $"migrationjobs\\{id}");
                var all = new List<LogObject>();

                var rs = _session!.ExecuteAsync(
                    new SimpleStatement(
                        $"SELECT content FROM \"{LOG_TABLE}\" " +
                        $"WHERE job_id = ?",
                        nJobId))
                    .GetAwaiter().GetResult();

                foreach (var row in rs)
                {
                    var json = row.GetValue<string>("content");
                    if (!string.IsNullOrEmpty(json))
                    {
                        var obj = JsonConvert
                            .DeserializeObject<LogObject>(json);
                        if (obj != null) all.Add(obj);
                    }
                }

                var page = all.Skip(skip).Take(take).ToList();
                string jsonResult = JsonConvert.SerializeObject(
                    page, Formatting.Indented);
                return Encoding.UTF8.GetBytes(jsonResult);
            }
            catch (Exception ex)
            {
                Helper.LogToFile(
                    $"[DocumentDBPersistence] Paginated error: " +
                    $"{ex}", "DocumentDBPersistence.txt");
                return Array.Empty<byte>();
            }
        }

        public override long DeleteLogs(string jobId)
        {
            EnsureInitialized();
            try
            {
                var nJobId = NormalizeId(
                    $"migrationjobs\\{jobId}");

                // Count first, then delete by partition
                int count = GetLogCount(jobId);

                // Cassandra doesn't support DELETE without
                // full primary key easily for range deletes.
                // Read timestamps then delete each.
                var rs = _session!.ExecuteAsync(
                    new SimpleStatement(
                        $"SELECT ts FROM \"{LOG_TABLE}\" " +
                        $"WHERE job_id = ?", nJobId))
                    .GetAwaiter().GetResult();

                foreach (var row in rs)
                {
                    var ts = row.GetValue<DateTimeOffset>("ts");
                    _session.ExecuteAsync(new SimpleStatement(
                        $"DELETE FROM \"{LOG_TABLE}\" " +
                        $"WHERE job_id = ? AND ts = ?",
                        nJobId, ts))
                        .GetAwaiter().GetResult();
                }

                return count;
            }
            catch (Exception ex)
            {
                Helper.LogToFile(
                    $"[DocumentDBPersistence] DeleteLogs " +
                    $"error: {ex}",
                    "DocumentDBPersistence.txt");
                return -1;
            }
        }
    }
}
