using Newtonsoft.Json;
using CassandraMigrationProcessor.Infrastructure;
using System;
using System.Collections.Generic;

namespace CassandraMigrationProcessor.Models
{
    public class MigrationJob
    {
        public string Id { get; set; } = string.Empty;
        public string? Name { get; set; }

        // Source: Cosmos DB Cassandra API
        public string? SourceContactPoint { get; set; }
        public int SourcePort { get; set; } = MigrationDefaults.CosmosDbCassandraPort;
        public string? SourceUsername { get; set; }
        /// <summary>
        /// Never persisted to disk. On resume the token is
        /// fetched fresh via Azure.Identity.
        /// </summary>
        [JsonIgnore]
        public string? SourcePassword { get; set; }
        public bool SourceUseAad { get; set; }

        // Target: OSS Cassandra
        public string? TargetContactPoint { get; set; }
        public int TargetPort { get; set; } = MigrationDefaults.DefaultCassandraPort;
        public string? TargetUsername { get; set; }
        /// <summary>
        /// Never persisted to disk. On resume the password is
        /// re-supplied by the user or fetched from app config.
        /// </summary>
        [JsonIgnore]
        public string? TargetPassword { get; set; }

        [JsonProperty("NameSpaces")]
        public string? Namespaces { get; set; }

        public DateTime? StartedOn { get; set; }

        /// <summary>
        /// Single source of truth for job lifecycle state.
        /// </summary>
        public JobStatus Status { get; set; } = JobStatus.Pending;

        // ── Backward-compat boolean properties ──────────
        // Deserialized from old job files to migrate Status.
        // On new saves, Status is the source of truth.
        // Priority order for migration: Cancelled > Faulted >
        // Completed > Paused > Running > Pending.

        [JsonProperty(DefaultValueHandling = DefaultValueHandling.Ignore)]
        public bool IsCompleted
        {
            get => Status == JobStatus.Completed;
            set { if (value) Status = JobStatus.Completed; }
        }
        [JsonProperty(DefaultValueHandling = DefaultValueHandling.Ignore)]
        public bool IsCancelled
        {
            get => Status == JobStatus.Cancelled;
            set { if (value) Status = JobStatus.Cancelled; }
        }
        [JsonProperty(DefaultValueHandling = DefaultValueHandling.Ignore)]
        public bool IsFaulted
        {
            get => Status == JobStatus.Faulted;
            set { if (value) Status = JobStatus.Faulted; }
        }
        [JsonProperty(DefaultValueHandling = DefaultValueHandling.Ignore)]
        public bool IsPaused
        {
            get => Status == JobStatus.Paused;
            set { if (value) Status = JobStatus.Paused; }
        }
        [JsonProperty(DefaultValueHandling = DefaultValueHandling.Ignore)]
        public bool IsStarted
        {
            get => Status == JobStatus.Running;
            set
            {
                if (value) Status = JobStatus.Running;
                else if (Status == JobStatus.Running)
                    Status = JobStatus.Pending;
            }
        }

        public JobType JobType { get; set; } = JobType.CqlCopy;

        public CDCMode CDCMode { get; set; } = CDCMode.Offline;

        public bool IsSimulatedRun { get; set; }
        public bool AppendMode { get; set; }

        /// <summary>
        /// When true, drop target tables before starting the
        /// job so they are recreated fresh from source schema.
        /// Default is false.
        /// </summary>
        public bool DropTargetTableBeforeStart { get; set; }

        /// <summary>
        /// Minimum log level. Default is Info.
        /// </summary>
        public LogType LogLevel { get; set; } = LogType.Info;

        [JsonIgnore]
        public bool AutoRefreshEnabled { get; set; } = true;

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

        // Change feed state (Cosmos DB Cassandra change feed)
        public string? ChangeFeedContinuationToken { get; set; }
        public DateTime? ChangeFeedStartedOn { get; set; }

        [JsonProperty("MigrationUnitBasics")]
        public List<MigrationUnitBasic>? Tables { get; set; }

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
}
