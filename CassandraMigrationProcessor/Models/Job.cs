using Newtonsoft.Json;
using System.Collections.Generic;

namespace CassandraMigrationProcessor.Models;
public class Job
{
    // ── Identity ──

    public string Id { get; set; } = string.Empty;
    public string? Name { get; set; }

    // ── Source Connection ──

    public string? SourceContactPoint { get; set; }
    public int SourcePort { get; set; } = 10350;
    public string? SourceUsername { get; set; }
    /// <summary>
    /// Never persisted to disk. On resume the token is
    /// fetched fresh via Azure.Identity.
    /// </summary>
    [JsonIgnore]
    public string? SourcePassword { get; set; }
    public bool SourceUseAad { get; set; }

    // ── Target Connection ──

    public string? TargetContactPoint { get; set; }
    public int TargetPort { get; set; } = 9042;
    public string? TargetUsername { get; set; }
    /// <summary>
    /// Never persisted to disk. On resume the password is
    /// re-supplied by the user or fetched from app config.
    /// </summary>
    [JsonIgnore]
    public string? TargetPassword { get; set; }

    // ── Pipeline Config ──

    /// <summary>
    /// Number of parallel threads for row copy operations.
    /// </summary>
    public int ParallelThreads { get; set; } = 5;

    /// <summary>
    /// Max concurrent feed-range workers per table.
    /// 0 = auto (CPU cores × 15 / parallel tables).
    /// </summary>
    public int MaxFeedRangeParallelism { get; set; } = 0;

    /// <summary>
    /// Max Cassandra driver connections per host.
    /// 0 = default (1 per worker session).
    /// </summary>
    public int MaxConnectionsPerHost { get; set; } = 0;

    /// <summary>
    /// Rows per page when reading from source.
    /// 0 = default (500). Larger pages reduce round-trips
    /// but consume more RU per request.
    /// </summary>
    public int PageSize { get; set; } = 0;

    // ── Job Settings ──

    public CDCMode CDCMode { get; set; } = CDCMode.Offline;

    public JobType JobType{ get; set; } = JobType.CqlCopy;

    public bool IsSimulatedRun { get; set; }
    public bool AppendMode { get; set; }

    /// <summary>
    /// When true, drop target tables before starting the
    /// job so they are recreated fresh from source schema.
    /// Default is false.
    /// </summary>
    public bool DropTargetTableBeforeStart { get; set; }

    /// <summary>
    /// When true, the migration tool does NOT create or modify schema
    /// (keyspaces, tables, or User-Defined Types) on the target. An
    /// identical schema is expected to have been provisioned on the
    /// target before the job starts. Use this when target schema
    /// management is owned by another process or when it must be
    /// customised (e.g. different replication settings, table options,
    /// or a subset of UDTs) and the tool's automatic replication is
    /// not appropriate. Default is false (the tool replicates schema
    /// automatically — see <see cref="CassandraDriver.SchemaManager.SyncSchemaAsync"/>).
    /// </summary>
    public bool SkipSchemaSync { get; set; }

    /// <summary>
    /// Minimum log level. Default is Info.
    /// </summary>
    public LogType LogLevel { get; set; } = LogType.Info;

    // ── Runtime State ──

    /// <summary>
    /// Single source of truth for job lifecycle state.
    /// </summary>
    public JobStatus Status { get; set; } = JobStatus.Pending;

    public DateTime? StartedOn { get; set; }

    [JsonProperty("MigrationUnitBasics")]
    public List<TableMigrationSummary> Tables { get; set; } = new();

    [JsonIgnore]
    public bool AutoRefreshEnabled { get; set; } = true;

    public string? Namespaces { get; set; }

    // ── Computed ──

    [JsonIgnore]
    public ConnectionOptions SourceConnection => new(
        SourceContactPoint ?? "", SourcePort,
        SourceUsername, SourcePassword, true);

    [JsonIgnore]
    public ConnectionOptions TargetConnection => new(
        TargetContactPoint ?? "", TargetPort,
        TargetUsername, TargetPassword, true,
        MaxConnectionsPerHost);
}
