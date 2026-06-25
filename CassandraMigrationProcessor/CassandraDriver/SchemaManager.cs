using Cassandra;
using CassandraMigrationProcessor.Infrastructure;
using CassandraMigrationProcessor.Models;

namespace CassandraMigrationProcessor.CassandraDriver;

/// <summary>
/// Source-keyspace replication metadata used when mirroring CREATE
/// KEYSPACE onto the target. <c>Cql</c> is the rendered replication
/// map literal (alphabetically ordered for deterministic DDL).
/// <c>DurableWrites</c> is the value read from
/// <c>system_schema.keyspaces</c>. <c>Map</c> is the raw key/value
/// dictionary so the caller can inspect strategy class / per-DC
/// entries without re-parsing the rendered literal. All three fields
/// are <c>null</c> when the source read fails or the keyspace is
/// missing, signalling the caller to fall through to the safe
/// SimpleStrategy default.
/// </summary>
internal sealed record KeyspaceReplicationInfo(
    string? Cql,
    bool? DurableWrites,
    IDictionary<string, string>? Map);

/// <summary>
/// CQL <c>WITH</c> clause plus the names of any options that were
/// intentionally skipped because target distributions commonly reject
/// them. Returned by
/// <see cref="SchemaManager.BuildForwardableTableOptionsAsync"/>.
/// </summary>
internal sealed record ForwardableTableOptions(
    string WithOptionsClause,
    IReadOnlyList<string> NotForwarded);

/// <summary>
/// Handles schema discovery, creation, and synchronization
/// between source and target Cassandra clusters.
/// </summary>
public static class SchemaManager
{
    private const int ProbeTimeoutMs = 15_000;
    private const int ThrottleMaxRetries = 10;

    /// <summary>
    /// Synchronises the target schema with the source:
    /// ensure keyspace → check table exists → create or
    /// alter → return source column list.
    /// </summary>
    public static async Task
        SyncSchemaAsync(ISession sourceSession, ISession targetSession,
            string sourceKeyspace, string sourceTable,
            string targetKeyspace, string targetTable,
            MigrationLog? log = null)
    {
        await EnsureKeyspaceExistsAsync(
            targetSession, targetKeyspace,
            sourceSession: sourceSession,
            sourceKeyspace: sourceKeyspace,
            log: log);

        // Discover source columns first so UDT replication is scoped to
        // the UDTs actually referenced by this table.
        var sourceColumns = await GetTableColumnsAsync(sourceSession, sourceKeyspace, sourceTable);

        var allUdts = await GetUserDefinedTypesAsync(sourceSession, sourceKeyspace);
        var requiredUdts = FilterUdtsReferencedByTable(allUdts, sourceColumns.Select(c => c.Type));
        if (requiredUdts.Count > 0)
        {
            await ReplicateUserDefinedTypesAsync(sourceSession, targetSession,
                sourceKeyspace, targetKeyspace, requiredUdts, log);
        }

        // CreateTableFromSourceAsync forwards columns, PRIMARY KEY,
        // CLUSTERING ORDER and the safe subset of table options.
        // Options commonly rejected by target distributions are skipped
        // and surfaced via a WARN log.
        await CreateTableFromSourceAsync(sourceSession, targetSession,
            sourceKeyspace, sourceTable, targetKeyspace, targetTable, log);
    }

    /// <summary>
    /// Replicate the supplied User-Defined Types (or every UDT in the
    /// source keyspace if <paramref name="udtsToReplicate"/> is null)
    /// from source to target. UDTs are created in dependency order
    /// using <c>CREATE TYPE IF NOT EXISTS</c>.
    /// </summary>
    public static async Task ReplicateUserDefinedTypesAsync(ISession sourceSession, ISession targetSession,
        string sourceKeyspace, string targetKeyspace,
        IReadOnlyList<UserDefinedTypeDef>? udtsToReplicate = null,
        MigrationLog? log = null)
    {
        CqlIdentifier.Validate(sourceKeyspace);
        CqlIdentifier.Validate(targetKeyspace);

        udtsToReplicate ??= await GetUserDefinedTypesAsync(sourceSession, sourceKeyspace);
        if (udtsToReplicate.Count == 0) return;

        var ordered = TopologicallySortUdts(udtsToReplicate.ToList());

        foreach (var udt in ordered)
        {
            CqlIdentifier.Validate(udt.TypeName);

            var fieldDefs = new List<string>(udt.FieldNames.Count);
            for (int i = 0; i < udt.FieldNames.Count; i++)
            {
                CqlIdentifier.Validate(udt.FieldNames[i]);
                fieldDefs.Add($"\"{udt.FieldNames[i]}\" {udt.FieldTypes[i]}");
            }

            string cql =
                $"CREATE TYPE IF NOT EXISTS \"{targetKeyspace}\".\"{udt.TypeName}\" (" +
                string.Join(", ", fieldDefs) + ")";

            log?.WriteLine($"DDL on target: {cql}", LogType.Info);
            await RetryExecutor.ExecuteAsync(() =>
                targetSession.ExecuteAsync(new SimpleStatement(cql)));
        }
    }

    /// <summary>
    /// Execute a schema-metadata query against <c>system_schema.*</c>
    /// (or any other system table that returns small, deterministic
    /// rowsets) with the standard schema read-timeout and the shared
    /// <see cref="RetryExecutor"/> policy applied. Centralises the
    /// timeout + retry contract so that adding a new schema query
    /// cannot silently inherit the driver-default timeout or skip the
    /// retry envelope.
    /// </summary>
    private static Task<RowSet> ExecuteSchemaQueryAsync(
        ISession session, string cql, params object[] args)
    {
        var statement = new SimpleStatement(cql, args);
        statement.SetReadTimeoutMillis(MigrationDefaults.SchemaQueryTimeoutMs);
        return RetryExecutor.ExecuteAsync(() => session.ExecuteAsync(statement));
    }

    /// <summary>
    /// Read every User-Defined Type defined in the given keyspace
    /// from <c>system_schema.types</c>. Returns type name plus the
    /// field name/type pairs in declaration order.
    /// </summary>
    public static async Task<List<UserDefinedTypeDef>> GetUserDefinedTypesAsync(ISession session, string keyspace)
    {
        var resultSet = await ExecuteSchemaQueryAsync(
            session,
            "SELECT type_name, field_names, field_types " +
            "FROM system_schema.types WHERE keyspace_name = ?",
            keyspace);

        var udts = new List<UserDefinedTypeDef>();
        foreach (var row in resultSet)
        {
            var typeName = row.GetValue<string>("type_name");
            var fieldNames = row.GetValue<IEnumerable<string>>("field_names")?.ToList()
                             ?? new List<string>();
            var fieldTypes = row.GetValue<IEnumerable<string>>("field_types")?.ToList()
                             ?? new List<string>();
            udts.Add(new UserDefinedTypeDef(typeName, fieldNames, fieldTypes));
        }
        return udts;
    }

    /// <summary>
    /// Reduce <paramref name="allUdts"/> to the subset transitively
    /// referenced by <paramref name="tableColumnTypes"/>.
    /// <para>
    /// A UDT is included if its name appears (with CQL identifier
    /// boundaries) in any column type, or in the field types of another
    /// UDT that is itself included. The matcher correctly handles nested
    /// forms such as <c>frozen&lt;list&lt;frozen&lt;my_udt&gt;&gt;&gt;</c>,
    /// <c>map&lt;text, frozen&lt;my_udt&gt;&gt;</c> and
    /// <c>tuple&lt;…, my_udt, …&gt;</c>.
    /// </para>
    /// </summary>
    public static List<UserDefinedTypeDef> FilterUdtsReferencedByTable(
        IReadOnlyList<UserDefinedTypeDef> allUdts,
        IEnumerable<string> tableColumnTypes)
    {
        if (allUdts == null || allUdts.Count == 0) return new List<UserDefinedTypeDef>();

        var byName = new Dictionary<string, UserDefinedTypeDef>(StringComparer.OrdinalIgnoreCase);
        foreach (var u in allUdts) byName[u.TypeName] = u;

        var reached = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        var queue = new Queue<string>();

        foreach (var columnType in tableColumnTypes ?? Enumerable.Empty<string>())
        {
            if (string.IsNullOrEmpty(columnType)) continue;
            foreach (var u in allUdts)
            {
                if (ContainsTypeName(columnType, u.TypeName) && reached.Add(u.TypeName))
                    queue.Enqueue(u.TypeName);
            }
        }

        while (queue.Count > 0)
        {
            var name = queue.Dequeue();
            if (!byName.TryGetValue(name, out var udt)) continue;
            foreach (var ft in udt.FieldTypes)
            {
                foreach (var other in allUdts)
                {
                    if (string.Equals(other.TypeName, name, StringComparison.OrdinalIgnoreCase)) continue;
                    if (ContainsTypeName(ft, other.TypeName) && reached.Add(other.TypeName))
                        queue.Enqueue(other.TypeName);
                }
            }
        }

        return allUdts.Where(u => reached.Contains(u.TypeName)).ToList();
    }

    /// <summary>
    /// Topologically sort UDTs so that any UDT referenced by another
    /// UDT in the same keyspace appears earlier in the returned list.
    /// References are detected by case-insensitive word-boundary
    /// matching against the field-type text. If a dependency cycle
    /// is detected the remaining UDTs are appended in their original
    /// order (the create attempt will surface the cycle as a Cassandra
    /// error rather than silently dropping any UDT).
    /// </summary>
    internal static List<UserDefinedTypeDef> TopologicallySortUdts(List<UserDefinedTypeDef> udts)
    {
        var byName = new Dictionary<string, UserDefinedTypeDef>(StringComparer.OrdinalIgnoreCase);
        foreach (var u in udts) byName[u.TypeName] = u;

        var dependsOn = new Dictionary<string, HashSet<string>>(StringComparer.OrdinalIgnoreCase);
        foreach (var u in udts)
        {
            var deps = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            foreach (var ft in u.FieldTypes)
            {
                foreach (var other in udts)
                {
                    if (string.Equals(other.TypeName, u.TypeName, StringComparison.OrdinalIgnoreCase))
                        continue;
                    if (ContainsTypeName(ft, other.TypeName))
                        deps.Add(other.TypeName);
                }
            }
            dependsOn[u.TypeName] = deps;
        }

        var resolved = new List<UserDefinedTypeDef>();
        var resolvedNames = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        bool progress = true;
        while (progress && resolved.Count < udts.Count)
        {
            progress = false;
            foreach (var u in udts.Where(u => !resolvedNames.Contains(u.TypeName)))
            {
                if (dependsOn[u.TypeName].All(d => resolvedNames.Contains(d)))
                {
                    resolved.Add(u);
                    resolvedNames.Add(u.TypeName);
                    progress = true;
                }
            }
        }
        if (resolved.Count < udts.Count)
        {
            foreach (var u in udts)
                if (!resolvedNames.Contains(u.TypeName))
                    resolved.Add(u);
        }
        return resolved;
    }

    private static bool ContainsTypeName(string fieldType, string typeName)
    {
        if (string.IsNullOrEmpty(fieldType) || string.IsNullOrEmpty(typeName)) return false;
        int idx = 0;
        while (idx <= fieldType.Length - typeName.Length)
        {
            int found = fieldType.IndexOf(typeName, idx, StringComparison.OrdinalIgnoreCase);
            if (found < 0) return false;
            bool leftOk = found == 0 || !IsIdentifierChar(fieldType[found - 1]);
            int after = found + typeName.Length;
            bool rightOk = after >= fieldType.Length || !IsIdentifierChar(fieldType[after]);
            if (leftOk && rightOk) return true;
            idx = found + 1;
        }
        return false;
    }

    private static bool IsIdentifierChar(char c) => char.IsLetterOrDigit(c) || c == '_';

    /// <summary>Source-side definition of a User-Defined Type.</summary>
    public sealed record UserDefinedTypeDef(string TypeName, List<string> FieldNames, List<string> FieldTypes);

    /// <summary>
    /// One-shot discovery scan that warns about source-side schema
    /// objects this tool does NOT migrate (secondary indexes,
    /// materialized views, UDFs, UDAs, triggers). Operators
    /// queries returned empty; now they see a single Warn line per
    /// category per keyspace at job start so they can recreate them
    /// manually before switchover.
    /// </summary>
    public static async Task WarnAboutUnreplicatedSchemaAsync(
        ISession sourceSession, IEnumerable<string> inScopeKeyspaces,
        MigrationLog? log)
    {
        if (log == null) return;

        async Task<(int count, bool failed)> CountAsync(string ks, string table, string col)
        {
            try
            {
                var stmt = new SimpleStatement(
                    $"SELECT {col} FROM system_schema.{table} " +
                    $"WHERE keyspace_name = ?", ks);
                stmt.SetReadTimeoutMillis(ProbeTimeoutMs);
                var rs = await sourceSession.ExecuteAsync(stmt);
                int count = 0;
                foreach (var _ in rs) count++;
                return (count, false);
            }
            catch (InvalidQueryException)
            {
                // system_schema.<table> isn't present on this source
                // distribution (older OSS Cassandra, Cosmos DB
                // Cassandra API, etc.). Nothing actionable; treat as
                // "0 unreplicated objects of this category".
                return (0, false);
            }
            catch (Exception ex)
            {
                // Real failure (timeout, auth, network blip). Do
                // NOT report 0 — that's exactly the silent drop the
                // warning exists to prevent. Surface as a Warning so
                // the operator knows to re-run / investigate before
                // cutover.
                log.WriteLine(
                    $"[Schema] Could not enumerate {table} in " +
                    $"\"{ks}\" ({ex.GetType().Name}: {ex.Message}). " +
                    $"This category was NOT scanned — re-run after " +
                    $"resolving the failure if your source has any.",
                    LogType.Warning);
                return (0, true);
            }
        }

        var categories = new (string Table, string KeyCol, string Label)[]
        {
            ("indexes",    "index_name",    "secondary indexes"),
            ("views",      "view_name",     "materialized views"),
            ("functions",  "function_name", "user-defined functions"),
            ("aggregates", "aggregate_name", "user-defined aggregates"),
            ("triggers",   "trigger_name",  "triggers"),
        };

        foreach (var ks in inScopeKeyspaces.Distinct(StringComparer.Ordinal))
        {
            foreach (var (table, keyCol, label) in categories)
            {
                var (count, _) = await CountAsync(ks, table, keyCol);
                if (count > 0)
                {
                    log.WriteLine(
                        $"[Schema] Source keyspace \"{ks}\" has " +
                        $"{count} {label}. This tool does not " +
                        $"migrate them — recreate manually on the " +
                        $"target before cutover.",
                        LogType.Warning);
                }
            }
        }
    }

    /// <summary>
    /// Probes the target table for the presence of any row. Used as
    /// a safety gate before destructive schema operations: if the
    /// target holds operator data we refuse to drop it. Probes at
    /// <c>LocalQuorum</c> so a row on an unconsulted replica cannot
    /// fool a single-replica ONE/LOCAL_ONE read into reporting
    /// "empty". On any failure (including consistency-unavailable)
    /// returns <c>true</c> — fail closed so we never silently DROP.
    /// </summary>
    private static async Task<bool> TargetTableHasDataAsync(
        ISession session, string keyspace, string table)
    {
        try
        {
            var stmt = new SimpleStatement(
                $"SELECT * FROM \"{keyspace}\".\"{table}\" LIMIT 1");
            stmt.SetReadTimeoutMillis(ProbeTimeoutMs);
            // Explicit LocalQuorum — a single-replica ONE read could
            // miss a write that lives on a replica not consulted by
            // this probe, returning "empty" and unblocking a DROP.
            // If the cluster cannot satisfy LocalQuorum we fail
            // closed (catch below) instead of degrading.
            stmt.SetConsistencyLevel(ConsistencyLevel.LocalQuorum);
            var rs = await session.ExecuteAsync(stmt);
            return rs.GetEnumerator().MoveNext();
        }
        catch (Exception)
        {
            // If we cannot decide, fail closed — treat as if data
            // could be present. The operator can re-run after fixing
            // permissions / connectivity / consistency.
            return true;
        }
    }

    /// <summary>
    /// Check if a keyspace exists.
    /// </summary>
    public static async Task<bool> KeyspaceExistsAsync(ISession session, string keyspace)
    {
        var keyspaces = await CassandraQueries.ListKeyspacesAsync(session);
        return keyspaces.Contains(keyspace, StringComparer.OrdinalIgnoreCase);
    }

    private static string FormatSortedDcList(IEnumerable<string> dcs) =>
        string.Join(", ", dcs.OrderBy(d => d, StringComparer.Ordinal));

    /// <summary>
    /// Ensure target keyspace exists. When <paramref name="sourceSession"/>
    /// and <paramref name="sourceKeyspace"/> are supplied, mirrors the
    /// source keyspace's replication strategy and durable_writes; otherwise
    /// falls back to SimpleStrategy with <paramref name="replicationFactor"/>.
    /// Refuses to auto-create with SimpleStrategy on multi-DC target
    /// clusters: SimpleStrategy is unsafe across DCs and an
    /// operator-chosen NetworkTopologyStrategy is required.
    /// </summary>
    public static async Task EnsureKeyspaceExistsAsync(ISession session, string keyspace,
        int replicationFactor = 1, MigrationLog? log = null,
        ISession? sourceSession = null, string? sourceKeyspace = null)
    {
        CqlIdentifier.Validate(keyspace);
        if (await KeyspaceExistsAsync(session, keyspace))
        {
            log?.WriteLine($"Target keyspace \"{keyspace}\" already exists; skipping schema mirror for keyspace.",
                LogType.Debug);
            return;
        }

        // Try to mirror the source keyspace's replication strategy +
        // durable_writes. The previous behaviour silently downgraded
        // a NetworkTopologyStrategy keyspace to SimpleStrategy(RF=1),
        // which on multi-DC migrations produced an undetected
        // topology downgrade.
        var replication = sourceSession != null && !string.IsNullOrEmpty(sourceKeyspace)
            ? await TryReadSourceKeyspaceMetadataAsync(sourceSession, sourceKeyspace!, log)
            : new KeyspaceReplicationInfo(null, null, null);

        if (replication.Cql == null)
        {
            replication = replication with
            {
                Cql = $"{{'class': 'SimpleStrategy', 'replication_factor': {replicationFactor}}}",
                Map = new Dictionary<string, string>(StringComparer.Ordinal)
                {
                    ["class"] = "SimpleStrategy",
                    ["replication_factor"] = replicationFactor.ToString(),
                },
            };
        }

        // Multi-DC + SimpleStrategy safety check applies regardless
        // of where the replication map originated (mirrored from
        // source OR generated as the fallback). Without this guard,
        // a source keyspace that itself uses SimpleStrategy would be
        // faithfully mirrored onto a multi-DC target — exactly the
        // unsafe scenario the original check existed to prevent.
        if (IsSimpleStrategy(replication.Map))
        {
            var dataCenters = await GetTargetDataCentersAsync(session);
            if (dataCenters.Count > 1)
            {
                string dcList = FormatSortedDcList(dataCenters);
                string msg =
                    $"Refusing to create keyspace \"{keyspace}\" with SimpleStrategy: " +
                    $"target cluster has multiple datacenters ({dcList}). " +
                    "SimpleStrategy is unsafe on multi-DC clusters (writes may not " +
                    "reach all DCs and reads may miss replicas). " +
                    "Pre-create the keyspace with an explicit NetworkTopologyStrategy " +
                    "and per-DC replication factor, e.g.: CREATE KEYSPACE \"" + keyspace +
                    "\" WITH replication = {'class': 'NetworkTopologyStrategy', " +
                    "'<dc-name>': <rf>, ...};";
                log?.WriteLine(msg, LogType.Error);
                throw new InvalidOperationException(msg);
            }
        }

        // NTS keyspaces specify DC names verbatim. If the mirrored
        // map references DCs that don't exist on the target, the
        // CREATE KEYSPACE silently succeeds but the keyspace ends up
        // with zero replicas on the actual target DC — writes then
        // succeed without ever being persisted. Refuse to proceed on
        // empty intersection; warn loudly on partial.
        if (IsNetworkTopologyStrategy(replication.Map))
        {
            var sourceDcs = replication.Map!
                .Where(kv => !string.Equals(kv.Key, "class", StringComparison.OrdinalIgnoreCase))
                .Select(kv => kv.Key)
                .ToHashSet(StringComparer.OrdinalIgnoreCase);

            var targetDcs = await GetTargetDataCentersAsync(session);
            if (targetDcs.Count > 0 && sourceDcs.Count > 0)
            {
                var intersection = sourceDcs
                    .Where(d => targetDcs.Contains(d, StringComparer.OrdinalIgnoreCase))
                    .ToList();

                string srcList = FormatSortedDcList(sourceDcs);
                string tgtList = FormatSortedDcList(targetDcs);

                if (intersection.Count == 0)
                {
                    string msg =
                        $"Refusing to create keyspace \"{keyspace}\": source replication " +
                        $"map references DCs [{srcList}] but target cluster reports DCs " +
                        $"[{tgtList}] — no overlap. Cassandra would accept the CREATE " +
                        $"KEYSPACE but the keyspace would end up with zero replicas on the " +
                        $"target DC, so writes would silently succeed without being persisted. " +
                        $"Pre-create the keyspace with a replication map that names a target DC.";
                    log?.WriteLine(msg, LogType.Error);
                    throw new InvalidOperationException(msg);
                }

                var missing = sourceDcs
                    .Where(d => !targetDcs.Contains(d, StringComparer.OrdinalIgnoreCase))
                    .ToList();
                if (missing.Count > 0)
                {
                    log?.WriteLine(
                        $"[Schema] Replication map for \"{keyspace}\" references " +
                        $"source DCs [{string.Join(", ", missing)}] not present on target " +
                        $"[{tgtList}]. Those replication factors will be ignored by the " +
                        $"target cluster — data will only be replicated to overlapping " +
                        $"DCs [{string.Join(", ", intersection)}].",
                        LogType.Warning);
                }
            }
        }

        if (replication.DurableWrites is false)
        {
            // Mirrored faithfully, but during the migration window
            // (dual-write or CDC tail readers) durable_writes=false
            // on target means a target-side power loss silently
            // drops in-flight writes. Surface it.
            log?.WriteLine(
                $"[Schema] Mirrored durable_writes=false from source keyspace " +
                $"\"{sourceKeyspace}\" to target \"{keyspace}\" — target writes " +
                $"will bypass the commitlog and may be lost on crash. Consider " +
                $"setting durable_writes=true on the target for the migration window.",
                LogType.Warning);
        }

        string durableClause = replication.DurableWrites is false
            ? " AND durable_writes = false"
            : string.Empty;
        string cql =
            $"CREATE KEYSPACE IF NOT EXISTS \"{keyspace}\" " +
            $"WITH replication = {replication.Cql}{durableClause}";
        log?.WriteLine($"DDL on target: {cql}", LogType.Info);
        await session.ExecuteAsync(new SimpleStatement(cql));
    }

    /// <summary>
    /// Returns the replication strategy class name from a
    /// <c>system_schema.keyspaces.replication</c> map, or <c>null</c>
    /// when the map is null/empty or has no <c>class</c> entry.
    /// Lookup is case-insensitive so a driver that returns
    /// <c>"Class"</c> (or any other casing) still resolves.
    /// </summary>
    private static string? GetReplicationClass(IDictionary<string, string>? replication)
    {
        if (replication == null) return null;
        var cls = replication.FirstOrDefault(kv =>
            string.Equals(kv.Key, "class", StringComparison.OrdinalIgnoreCase)).Value;
        return string.IsNullOrEmpty(cls) ? null : cls;
    }

    private static bool IsSimpleStrategy(IDictionary<string, string>? replication) =>
        GetReplicationClass(replication)
            ?.EndsWith("SimpleStrategy", StringComparison.OrdinalIgnoreCase) == true;

    private static bool IsNetworkTopologyStrategy(IDictionary<string, string>? replication) =>
        GetReplicationClass(replication)
            ?.EndsWith("NetworkTopologyStrategy", StringComparison.OrdinalIgnoreCase) == true;

    /// <summary>
    /// Normalises a CQL type string for equality comparison: strips
    /// whitespace and lowercases. Keeps qualifying punctuation
    /// (<c>&lt;</c>, <c>&gt;</c>, <c>,</c>) so
    /// <c>frozen&lt;list&lt;text&gt;&gt;</c> stays distinct from
    /// <c>frozen&lt;set&lt;text&gt;&gt;</c> while ignoring incidental
    /// whitespace and casing differences from the driver.
    /// </summary>
    private static string NormalizeCqlType(string? type)
    {
        if (string.IsNullOrEmpty(type)) return string.Empty;
        var sb = new System.Text.StringBuilder(type.Length);
        foreach (var ch in type)
        {
            if (!char.IsWhiteSpace(ch)) sb.Append(char.ToLowerInvariant(ch));
        }
        return sb.ToString();
    }

    /// <summary>
    /// Reads <c>replication</c> + <c>durable_writes</c> from the source
    /// keyspace's <c>system_schema.keyspaces</c> row and renders the
    /// replication map as a CQL literal. Returns a
    /// <see cref="KeyspaceReplicationInfo"/> with all-<c>null</c>
    /// fields when the read fails so the caller falls through to the
    /// safe SimpleStrategy default.
    /// </summary>
    private static async Task<KeyspaceReplicationInfo>
        TryReadSourceKeyspaceMetadataAsync(
            ISession sourceSession, string sourceKeyspace,
            MigrationLog? log)
    {
        try
        {
            var stmt = new SimpleStatement(
                "SELECT replication, durable_writes FROM " +
                "system_schema.keyspaces WHERE keyspace_name = ?",
                sourceKeyspace);
            stmt.SetReadTimeoutMillis(ProbeTimeoutMs);
            var rs = await sourceSession.ExecuteAsync(stmt);
            var row = rs.FirstOrDefault();
            if (row == null) return new KeyspaceReplicationInfo(null, null, null);

            var replication = row.GetValue<IDictionary<string, string>>("replication");
            var durableWrites = row.GetValue<bool>("durable_writes");
            if (replication == null || replication.Count == 0)
                return new KeyspaceReplicationInfo(null, durableWrites, null);

            // Render the map as a CQL literal, alphabetically by key
            // so the DDL is deterministic across runs.
            var entries = replication
                .OrderBy(kv => kv.Key, StringComparer.Ordinal)
                .Select(kv => $"'{kv.Key}': '{kv.Value}'");
            return new KeyspaceReplicationInfo(
                $"{{{string.Join(", ", entries)}}}", durableWrites, replication);
        }
        catch (Exception ex)
        {
            log?.WriteLine(
                $"Could not read replication for source keyspace " +
                $"\"{sourceKeyspace}\" ({ex.GetType().Name}: {ex.Message}); " +
                $"falling back to SimpleStrategy default.",
                LogType.Warning);
            return new KeyspaceReplicationInfo(null, null, null);
        }
    }

    /// <summary>
    /// Discover distinct datacenter names from the target cluster
    /// (system.local + system.peers). Returns an empty set if the
    /// query is not supported by the target (e.g. Cosmos DB Cassandra
    /// API), in which case the caller falls through to single-DC
    /// behaviour.
    /// </summary>
    private static async Task<HashSet<string>> GetTargetDataCentersAsync(ISession session)
    {
        var dcs = new HashSet<string>(StringComparer.Ordinal);
        try
        {
            var local = await session.ExecuteAsync(new SimpleStatement(
                "SELECT data_center FROM system.local"));
            foreach (var row in local)
            {
                var dc = row.GetValue<string?>("data_center");
                if (!string.IsNullOrWhiteSpace(dc)) dcs.Add(dc);
            }

            var peers = await session.ExecuteAsync(new SimpleStatement(
                "SELECT data_center FROM system.peers"));
            foreach (var row in peers)
            {
                var dc = row.GetValue<string?>("data_center");
                if (!string.IsNullOrWhiteSpace(dc)) dcs.Add(dc);
            }
        }
        catch
        {
            // Targets that do not expose system.local/system.peers
            // (or reject the query) fall through to single-DC
            // behaviour; the caller will still emit SimpleStrategy.
            return new HashSet<string>(StringComparer.Ordinal);
        }
        return dcs;
    }

    /// <summary>
    /// Check if a table exists and is accessible.
    /// Probes actual data (not just metadata) because
    /// Cosmos DB can return metadata for ghost tables
    /// that 404 on data reads.
    /// </summary>
    public static async Task<bool> TableExistsAsync(ISession session, string keyspace, string table)
    {
        CqlIdentifier.Validate(keyspace);
        CqlIdentifier.Validate(table);
        var tables = await CassandraQueries.ListTablesAsync(session, keyspace);
        if (!tables.Contains(table, StringComparer.OrdinalIgnoreCase))
            return false;

        for (int attempt = 1; attempt <= ThrottleMaxRetries; attempt++)
        {
            try
            {
                var probe = new SimpleStatement(
                    $"SELECT * FROM \"{keyspace}\".\"{table}\" LIMIT 1");
                probe.SetPageSize(1);
                probe.SetAutoPage(false);
                probe.SetReadTimeoutMillis(ProbeTimeoutMs);
                await session.ExecuteAsync(probe);
                return true;
            }
            catch (Exception ex)
            {
                if (ExceptionClassifier.IsThrottle(ex) && attempt < ThrottleMaxRetries)
                {
                    int delaySec = Math.Min(attempt * 3, 30);
                    await Task.Delay(delaySec * 1000);
                    continue;
                }

                if (ExceptionClassifier.IsNotFound(ex))
                    return false;

                throw;
            }
        }
        return false;
    }

    /// <summary>
    /// Get column metadata for a table.
    /// Returns list of (columnName, cqlType, kind,
    /// clusteringOrder, position).
    /// kind = "partition_key", "clustering",
    ///        "regular", "static"
    /// clusteringOrder = "asc", "desc", or "none"
    /// position = ordinal within key group
    /// </summary>
    public static async Task<List<CassandraColumn>>
        GetTableColumnsAsync(ISession session, string keyspace, string table)
    {
        var resultSet = await ExecuteSchemaQueryAsync(
            session,
            "SELECT column_name, type, kind, clustering_order, position " +
            "FROM system_schema.columns WHERE keyspace_name = ? AND table_name = ?",
            keyspace, table);

        return resultSet.Select(r => new CassandraColumn(
            Name: r.GetValue<string>("column_name"),
            Type: r.GetValue<string>("type"),
            Kind: r.GetValue<string>("kind"),
            ClusteringOrder: r.GetValue<string>("clustering_order") ?? "none",
            Position: r.GetValue<int>("position")
        )).ToList();
    }

    /// <summary>
    /// Create a table on the target using the source schema.
    /// </summary>
    public static async Task CreateTableFromSourceAsync(ISession sourceSession, ISession targetSession,
        string sourceKeyspace,
        string sourceTable,
        string targetKeyspace,
        string targetTable,
        MigrationLog? log = null)
    {
        CqlIdentifier.Validate(sourceKeyspace);
        CqlIdentifier.Validate(sourceTable);
        CqlIdentifier.Validate(targetKeyspace);
        CqlIdentifier.Validate(targetTable);
        var columns = await GetTableColumnsAsync(sourceSession, sourceKeyspace, sourceTable);
        if (columns.Count == 0)
            throw new InvalidOperationException($"Source table {sourceKeyspace}.{sourceTable} has no columns or does not exist.");

        if (await TableExistsAsync(targetSession, targetKeyspace, targetTable))
        {
            var targetCols = await GetTableColumnsAsync(targetSession, targetKeyspace, targetTable);
            var srcPartition = ColumnsOfKind(columns, "partition_key");
            var tgtPartition = ColumnsOfKind(targetCols, "partition_key");
            var srcClustering = ColumnsOfKind(columns, "clustering");
            var tgtClustering = ColumnsOfKind(targetCols, "clustering");

            var mismatchReasons = new List<string>();
            if (srcPartition.Count != tgtPartition.Count
                || !srcPartition.Select(c => c.Name)
                    .SequenceEqual(tgtPartition.Select(c => c.Name),
                        StringComparer.OrdinalIgnoreCase))
            {
                mismatchReasons.Add(
                    $"partition key differs " +
                    $"(source [{string.Join(",", srcPartition.Select(c => c.Name))}] " +
                    $"vs target [{string.Join(",", tgtPartition.Select(c => c.Name))}])");
            }
            else
            {
                // Same count + same names + same order — now compare
                // declared CQL types per position. Differing types
                // (e.g. source `id text` vs target `id bigint`, or
                // frozen<udt_v1> vs frozen<udt_v2>) silently passed
                // the original name-only check, skipped recreation,
                // and fell through to AlterTableAddMissingColumnsAsync
                // (which cannot fix PK types). The migration then
                // exploded with per-row coercion errors at INSERT
                // time instead of failing fast at schema sync.
                for (int i = 0; i < srcPartition.Count; i++)
                {
                    if (!NormalizeCqlType(srcPartition[i].Type)
                            .Equals(NormalizeCqlType(tgtPartition[i].Type),
                                StringComparison.OrdinalIgnoreCase))
                    {
                        mismatchReasons.Add(
                            $"partition key type at position {i} differs " +
                            $"for column \"{srcPartition[i].Name}\" " +
                            $"(source {srcPartition[i].Type} " +
                            $"vs target {tgtPartition[i].Type})");
                        break;
                    }
                }
            }
            if (srcClustering.Count != tgtClustering.Count)
                mismatchReasons.Add(
                    $"clustering column count differs " +
                    $"(source {srcClustering.Count} vs target {tgtClustering.Count})");
            else
            {
                for (int i = 0; i < srcClustering.Count; i++)
                {
                    if (!srcClustering[i].Name.Equals(tgtClustering[i].Name,
                            StringComparison.OrdinalIgnoreCase))
                    {
                        mismatchReasons.Add(
                            $"clustering column at position {i} differs " +
                            $"(source \"{srcClustering[i].Name}\" " +
                            $"vs target \"{tgtClustering[i].Name}\")");
                        break;
                    }
                    if (!NormalizeCqlType(srcClustering[i].Type)
                            .Equals(NormalizeCqlType(tgtClustering[i].Type),
                                StringComparison.OrdinalIgnoreCase))
                    {
                        mismatchReasons.Add(
                            $"clustering column type at position {i} differs " +
                            $"for column \"{srcClustering[i].Name}\" " +
                            $"(source {srcClustering[i].Type} " +
                            $"vs target {tgtClustering[i].Type})");
                        break;
                    }
                    if (!srcClustering[i].ClusteringOrder.Equals(tgtClustering[i].ClusteringOrder,
                            StringComparison.OrdinalIgnoreCase))
                    {
                        mismatchReasons.Add(
                            $"clustering order at position {i} differs " +
                            $"(source {srcClustering[i].ClusteringOrder} " +
                            $"vs target {tgtClustering[i].ClusteringOrder})");
                        break;
                    }
                }
            }

            bool clusteringMismatch = mismatchReasons.Count > 0;

            if (clusteringMismatch)
            {
                // Refuse to silently drop a target table that holds
                // operator data. The previous behaviour (unconditional
                // DROP on any clustering-shape divergence) was the
                // single most destructive default in the schema-
                // management layer — it discarded rows from partial
                // migrations, manual seed data, and concurrent jobs
                // with no prompt and only an Info-level log line.
                bool hasData = await TargetTableHasDataAsync(
                    targetSession, targetKeyspace, targetTable);
                string reasonList = string.Join("; ", mismatchReasons);
                if (hasData)
                {
                    string msg =
                        $"Refusing to recreate target table " +
                        $"\"{targetKeyspace}\".\"{targetTable}\": " +
                        $"schema diverges from source ({reasonList}) " +
                        $"AND the target contains data. Repair the " +
                        $"target schema manually (e.g. truncate or " +
                        $"drop after backup), or drop the target row " +
                        $"set if loss is acceptable, then re-run.";
                    log?.WriteLine(msg, LogType.Error);
                    throw new InvalidOperationException(msg);
                }

                log?.WriteLine(
                    $"Schema mismatch detected for " +
                    $"\"{targetKeyspace}\".\"{targetTable}\" " +
                    $"({reasonList}); target is empty so recreating.",
                    LogType.Warning);
                string dropCql = $"DROP TABLE \"{targetKeyspace}\".\"{targetTable}\"";
                log?.WriteLine($"DDL on target: {dropCql}", LogType.Info);
                await targetSession.ExecuteAsync(new SimpleStatement(dropCql));
            }
            else
            {
                await AlterTableAddMissingColumnsAsync(targetSession, targetKeyspace, targetTable, columns,
                    targetCols, log);
                return;
            }
        }

        var partitionKeys = ColumnsOfKind(columns, "partition_key")
            .Select(c => $"\"{c.Name}\"")
            .ToList();

        var clusteringKeys = ColumnsOfKind(columns, "clustering")
            .Select(c => $"\"{c.Name}\"")
            .ToList();

        var colDefs = columns
            .Select(c => c.Kind == "static"
                ? $"  \"{c.Name}\" {c.Type} static"
                : $"  \"{c.Name}\" {c.Type}")
            .ToList();

        string pkClause = clusteringKeys.Count > 0
            ? $"({string.Join(", ", partitionKeys)}), {string.Join(", ", clusteringKeys)}"
            : string.Join(", ", partitionKeys);

        string clusteringOrder = BuildClusteringOrderClause(columns);
        var tableOptions = await BuildForwardableTableOptionsAsync(
            sourceSession, sourceKeyspace, sourceTable, log);

        string withClause = MergeWithClauses(clusteringOrder, tableOptions.WithOptionsClause);

        string cql =
            $"CREATE TABLE IF NOT EXISTS \"{targetKeyspace}\".\"{targetTable}\" (\n" +
            $"{string.Join(",\n", colDefs)},\n  PRIMARY KEY ({pkClause})\n)" +
            withClause;

        log?.WriteLine($"DDL on target: {cql}", LogType.Info);
        await targetSession.ExecuteAsync(new SimpleStatement(cql));

        if (tableOptions.NotForwarded.Count > 0 && log != null)
        {
            log.WriteLine(
                $"[Schema] {targetKeyspace}.{targetTable}: source has non-default table option(s) the target distribution typically rejects; not forwarded: " +
                string.Join(", ", tableOptions.NotForwarded) +
                ". Re-apply manually on the target post-migration if the target accepts them.",
                LogType.Warning);
        }
    }

    /// <summary>
    /// Compare source and target columns. For any
    /// regular/static column in source that is missing
    /// from target, execute ALTER TABLE … ADD.
    /// Primary key columns cannot be added after creation.
    /// </summary>
    public static async Task AlterTableAddMissingColumnsAsync(ISession targetSession, string targetKeyspace,
        string targetTable,
        List<CassandraColumn> sourceColumns,
        List<CassandraColumn> targetColumns,
        MigrationLog? log = null)
    {
        CqlIdentifier.Validate(targetKeyspace);
        CqlIdentifier.Validate(targetTable);
        var targetColNames = new HashSet<string>(targetColumns.Select(c => c.Name),
            StringComparer.OrdinalIgnoreCase);

        var missingCols = sourceColumns
            .Where(c => (c.Kind == "regular" || c.Kind == "static") && !targetColNames.Contains(c.Name))
            .ToList();

        if (missingCols.Count == 0) return;

        var failedCols = new List<(string Name, Exception Ex)>();

        foreach (var col in missingCols)
        {
            string staticClause = col.Kind == "static" ? " static" : "";
            string alterCql =
                $"ALTER TABLE \"{targetKeyspace}\".\"{targetTable}\" " +
                $"ADD \"{col.Name}\" {col.Type}{staticClause}";
            log?.WriteLine($"DDL on target: {alterCql}", LogType.Info);
            try
            {
                await targetSession.ExecuteAsync(new SimpleStatement(alterCql));
            }
            catch (Exception ex)
            {
                log?.WriteLine($"ALTER TABLE failed for column \"{col.Name}\": {ex.GetType().Name}: {ex.Message}",
                    LogType.Error);
                failedCols.Add((col.Name, ex));
            }
        }

        if (failedCols.Count > 0)
        {
            var names = string.Join(", ", failedCols.Select(f => f.Name));
            throw new InvalidOperationException($"ALTER TABLE failed for {failedCols.Count} column(s): {names}. Target schema may be incomplete.");
        }
    }

    /// <summary>
    /// Return the columns whose <see cref="CassandraColumn.Kind"/>
    /// equals <paramref name="kind"/>, ordered by declared
    /// position. Used to surface "partition_key" or "clustering"
    /// groups in key order for schema comparison and DDL build-up.
    /// </summary>
    private static List<CassandraColumn> ColumnsOfKind(
        IEnumerable<CassandraColumn> columns, string kind)
        => columns.Where(c => c.Kind == kind).OrderBy(c => c.Position).ToList();

    /// <summary>
    /// Build a WITH CLUSTERING ORDER BY clause from
    /// column metadata. Returns empty string if no
    /// clustering columns or all are default (ASC).
    /// </summary>
    private static string BuildClusteringOrderClause(List<CassandraColumn> columns)
    {
        var clusteringCols = ColumnsOfKind(columns, "clustering");

        if (clusteringCols.Count == 0)
            return string.Empty;

        bool hasNonDefault = clusteringCols
            .Any(c => c.ClusteringOrder.Equals("desc", StringComparison.OrdinalIgnoreCase));
        if (!hasNonDefault)
            return string.Empty;

        var orderParts = clusteringCols
            .Select(c => $"\"{c.Name}\" {c.ClusteringOrder.ToUpperInvariant()}")
            .ToList();

        return $" WITH CLUSTERING ORDER BY ({string.Join(", ", orderParts)})";
    }

    /// <summary>
    /// Cassandra default ``gc_grace_seconds`` is 10 days; tables created
    /// without an explicit ``WITH gc_grace_seconds = N`` inherit this.
    /// Used by <see cref="WarnIfSourceHasNonDefaultTableOptionsAsync"/>
    /// to decide whether a source table's GC window is worth flagging
    /// to the operator.
    /// </summary>
    private const int CassandraDefaultGcGraceSeconds = 864_000;

    /// <summary>
    /// Inspect ``system_schema.tables`` for the source table and return
    /// a <see cref="ForwardableTableOptions"/>: the subset of table
    /// options that are safe to forward directly into the target's
    /// ``CREATE TABLE ... WITH ...`` clause, and a list of options
    /// intentionally skipped because target distributions (notably
    /// Cosmos DB for Apache Cassandra) commonly reject them.
    /// Best-effort: any failure to read the schema row returns an
    /// empty clause and an empty skip list so schema sync is never
    /// blocked by a fidelity helper.
    /// </summary>
    private static async Task<ForwardableTableOptions>
        BuildForwardableTableOptionsAsync(
            ISession sourceSession, string keyspace, string table,
            MigrationLog? log = null)
    {
        try
        {
            // Pull the well-known columns. Anything unrecognised by the
            // source driver (e.g. Cosmos returning a thin schema) throws
            // and lands in the catch below.
            var rs = await ExecuteSchemaQueryAsync(
                sourceSession,
                "SELECT default_time_to_live, gc_grace_seconds, compaction, " +
                "compression, caching, comment, cdc, bloom_filter_fp_chance, " +
                "speculative_retry, memtable_flush_period_in_ms " +
                "FROM system_schema.tables WHERE keyspace_name = ? AND table_name = ?",
                keyspace, table);
            var row = rs.FirstOrDefault();
            if (row == null) return new ForwardableTableOptions(string.Empty, Array.Empty<string>());

            // Scalar options that the upstream Cassandra grammar accepts
            // verbatim and target distributions generally tolerate. We
            // only emit each one when it diverges from the documented
            // server default — sending the default through is harmless
            // but it clutters the DDL log.
            var forwarded = new List<string>();

            int ttl = TryGet(row, "default_time_to_live", 0);
            if (ttl > 0) forwarded.Add($"default_time_to_live = {ttl}");

            int gc = TryGet(row, "gc_grace_seconds", 0);
            if (gc != CassandraDefaultGcGraceSeconds)
                forwarded.Add($"gc_grace_seconds = {gc}");

            int flushMs = TryGet(row, "memtable_flush_period_in_ms", 0);
            if (flushMs > 0)
                forwarded.Add($"memtable_flush_period_in_ms = {flushMs}");

            double fpc = TryGet(row, "bloom_filter_fp_chance", 0d);
            if (fpc > 0 && Math.Abs(fpc - 0.01) > 0.0001 && Math.Abs(fpc - 0.1) > 0.0001)
                forwarded.Add(
                    $"bloom_filter_fp_chance = {fpc.ToString("R", System.Globalization.CultureInfo.InvariantCulture)}");

            string speculative = TryGet(row, "speculative_retry", string.Empty);
            if (!string.IsNullOrEmpty(speculative)
                && !speculative.Equals("99PERCENTILE", StringComparison.OrdinalIgnoreCase)
                && !speculative.Equals("99p", StringComparison.OrdinalIgnoreCase))
            {
                forwarded.Add($"speculative_retry = '{EscapeCqlString(speculative)}'");
            }

            string comment = TryGet(row, "comment", string.Empty);
            if (!string.IsNullOrEmpty(comment))
                forwarded.Add($"comment = '{EscapeCqlString(comment)}'");

            // Options skipped on purpose: cdc semantics are target-specific
            // (Cosmos uses CDC internally for replay; OSS Cassandra needs
            // log-capture infra) and the three map-typed options (compaction,
            // compression, caching) require the full {'class': '...', ...}
            // map literal which Cosmos commonly rejects outright. Surface
            // these so the operator can re-apply them post-migration if the
            // target distribution accepts them.
            var dropped = new List<string>();

            bool cdc = TryGet(row, "cdc", false);
            if (cdc) dropped.Add("cdc=true");

            if (RowHasNonEmptyMap(row, "compaction")) dropped.Add("compaction=<custom>");
            if (RowHasNonEmptyMap(row, "compression")) dropped.Add("compression=<custom>");
            if (RowHasNonEmptyMap(row, "caching")) dropped.Add("caching=<custom>");

            string clause = forwarded.Count == 0
                ? string.Empty
                : " WITH " + string.Join(" AND ", forwarded);

            return new ForwardableTableOptions(clause, dropped);
        }
        catch (Exception ex)
        {
            log?.WriteLine(
                $"[Schema] {keyspace}.{table}: failed to read source table options ({ex.GetType().Name}: {ex.Message}); " +
                $"target table will use distribution defaults for TTL / gc_grace / compaction / compression / caching.",
                LogType.Warning);
            return new ForwardableTableOptions(string.Empty, Array.Empty<string>());
        }
    }

    /// <summary>
    /// Merge an optional ``WITH CLUSTERING ORDER BY (...)`` clause with
    /// an optional ``WITH option = value AND ...`` clause into a single
    /// ``WITH ... AND ...`` chain. Each input is either empty or starts
    /// with a single leading space + ``WITH ``. Returns empty when both
    /// inputs are empty.
    /// </summary>
    private static string MergeWithClauses(string clusteringOrder, string optionsClause)
    {
        bool hasClustering = !string.IsNullOrEmpty(clusteringOrder);
        bool hasOptions = !string.IsNullOrEmpty(optionsClause);

        if (!hasClustering && !hasOptions) return string.Empty;
        if (hasClustering && !hasOptions) return clusteringOrder;
        if (!hasClustering && hasOptions) return optionsClause;

        const string withPrefix = " WITH ";
        return clusteringOrder + " AND " + optionsClause.Substring(withPrefix.Length);
    }

    private static string EscapeCqlString(string s) => s.Replace("'", "''");

    private static T TryGet<T>(Row row, string column, T fallback)
    {
        try
        {
            var v = row.GetValue<T>(column);
            return v is null ? fallback : v;
        }
        catch { return fallback; }
    }

    private static bool RowHasNonEmptyMap(Row row, string column)
    {
        try
        {
            var map = row.GetValue<IDictionary<string, string>>(column);
            return map != null && map.Count > 0;
        }
        catch { return false; }
    }
}
