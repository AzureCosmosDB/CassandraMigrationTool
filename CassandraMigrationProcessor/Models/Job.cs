using Newtonsoft.Json;

namespace CassandraMigrationProcessor.Models;

/// <summary>
/// Persistent root document for a migration run: source/target connection
/// info, pipeline tuning knobs, lifecycle <see cref="JobStatus"/>, and the
/// list of <see cref="TableMigrationSummary"/> children it owns.
/// </summary>
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
    /// Size of the shared worker pool that does row-level copy work
    /// across all tables. 0 = auto from <see cref="Environment.ProcessorCount"/>.
    /// </summary>
    public int WorkerCount { get; set; } = 0;

    /// <summary>
    /// Max Cassandra driver connections per host. Back-compat fallback
    /// for source and target when their specific overrides are 0.
    /// 0 here means: use the driver default.
    /// </summary>
    public int MaxConnectionsPerHost { get; set; } = 0;

    /// <summary>
    /// Source (reader) driver connections per host. 0 falls back to
    /// <see cref="MaxConnectionsPerHost"/> then to the driver default.
    /// </summary>
    public int SourceMaxConnectionsPerHost { get; set; } = 0;

    /// <summary>
    /// Target (writer) driver connections per host. Same fallback
    /// chain as <see cref="SourceMaxConnectionsPerHost"/>.
    /// </summary>
    public int TargetMaxConnectionsPerHost { get; set; } = 0;

    /// <summary>
    /// Rows per page when reading from source. 0 = default (500).
    /// </summary>
    public int PageSize { get; set; } = 0;

    /// <summary>
    /// Max retries for a transient source-side page read failure.
    /// 0 = default (3).
    /// </summary>
    public int MaxReadRetries { get; set; } = 0;

    /// <summary>
    /// Max retries for a transient target-side per-row write failure.
    /// 0 = default (5).
    /// </summary>
    public int MaxWriteRetries { get; set; } = 0;

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
    /// When true, the tool does NOT create or modify schema on the
    /// target — an identical schema must be provisioned beforehand.
    /// Default is false.
    /// </summary>
    public bool SkipSchemaSync { get; set; }

    /// <summary>
    /// When true, the tool does NOT run a per-table <c>SELECT COUNT(*)</c>
    /// during partitioning to learn the source row count. Progress
    /// display shows "?%" for those tables. Useful when COUNT(*) is
    /// expensive or disabled on the source. Default is false.
    /// </summary>
    public bool SkipSourceRowCount { get; set; }

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

    /// <summary>
    /// UTC timestamp of the moment <see cref="Status"/> transitioned
    /// to a terminal state (Completed, Faulted, or Cancelled).
    /// Written by MigrationJobRunner.
    /// </summary>
    public DateTime? EndedOn { get; set; }

    [JsonProperty("MigrationUnitBasics")]
    public List<TableMigrationSummary> Tables { get; set; } = new();

    [JsonIgnore]
    public bool AutoRefreshEnabled { get; set; } = true;

    public string? Namespaces { get; set; }

    /// <summary>
    /// True when this job runs change-data-capture replay alongside bulk
    /// copy. False for pure offline jobs which finish once bulk copy
    /// completes.
    /// </summary>
    [JsonIgnore]
    public bool IsOnline => CDCMode != CDCMode.Offline;

    /// <summary>
    /// True iff every valid table in this offline job has finished bulk
    /// copy. Always false for online jobs (they don't have a single
    /// "done" moment — they tail change feeds forever).
    /// </summary>
    [JsonIgnore]
    public bool IsOfflineCompleted
    {
        get
        {
            if (Tables.Count == 0) return false;
            foreach (var t in Tables)
            {
                if (!t.IsValid) continue;
                if (!t.CopyComplete) return false;
            }
            return true;
        }
    }
}
