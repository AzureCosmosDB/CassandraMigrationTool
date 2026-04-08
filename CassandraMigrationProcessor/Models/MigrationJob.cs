using Newtonsoft.Json;
using CassandraMigrationProcessor.Models;
using System;
using System.Collections.Generic;
using CassandraMigrationProcessor.Context;

#pragma warning disable CS8618

namespace CassandraMigrationProcessor
{
    /// <summary>
    /// CDC mode for Cassandra migration (change feed).
    /// </summary>
    public enum CDCMode
    {
        Offline,
        Online
    }

    public enum JobStatus
    {
        Pending,
        Running,
        Paused,
        Completed,
        Cancelled,
        Faulted
    }

    public class MigrationJob
    {
        public string Id { get; set; }
        public string? Name { get; set; }

        // Source: Cosmos DB Cassandra API
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

        // Target: OSS Cassandra
        public string? TargetContactPoint { get; set; }
        public int TargetPort { get; set; } = 9042;
        public string? TargetUsername { get; set; }
        /// <summary>
        /// Never persisted to disk. On resume the password is
        /// re-supplied by the user or fetched from app config.
        /// </summary>
        [JsonIgnore]
        public string? TargetPassword { get; set; }

        public string? NameSpaces { get; set; }

        public DateTime? StartedOn { get; set; }

        /// <summary>
        /// Single source of truth for job lifecycle state.
        /// Replaces the individual boolean flags.
        /// </summary>
        public JobStatus Status { get; set; } = JobStatus.Pending;

        // ── Backward-compat boolean properties ──────────
        // Kept for JSON deserialization of old job files and
        // to minimize code churn. Setters update Status.

        [JsonIgnore]
        public bool IsCompleted
        {
            get => Status == JobStatus.Completed;
            set { if (value) Status = JobStatus.Completed; }
        }
        [JsonIgnore]
        public bool IsCancelled
        {
            get => Status == JobStatus.Cancelled;
            set { if (value) Status = JobStatus.Cancelled; }
        }
        [JsonIgnore]
        public bool IsFaulted
        {
            get => Status == JobStatus.Faulted;
            set { if (value) Status = JobStatus.Faulted; }
        }
        [JsonIgnore]
        public bool IsPaused
        {
            get => Status == JobStatus.Paused;
            set { if (value) Status = JobStatus.Paused; }
        }
        [JsonIgnore]
        public bool IsStarted
        {
            get => Status == JobStatus.Running;
            set { if (value) Status = JobStatus.Running;
                  else if (Status == JobStatus.Running)
                      Status = JobStatus.Pending; }
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
        /// Higher = more throughput but more RU consumption.
        /// </summary>
        public int MaxFeedRangeParallelism { get; set; } = 16;

        /// <summary>
        /// Max Cassandra driver connections per host.
        /// 0 = default (8 local, 4 remote).
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

        public List<MigrationUnitBasic>? MigrationUnitBasics { get; set; }

        public bool Persist()
        {
            MigrationJobContext.AddVerboseLog(
                $"MigrationJob.Persist: jobId={this.Id}, " +
                $"jobName={this.Name}");

            var filePath =
                $"migrationjobs\\{this.Id}\\jobdefinition.json";

            string json = JsonConvert.SerializeObject(
                this, Formatting.Indented);

            return MigrationJobContext.Store
                .UpsertDocument(filePath, json);
        }
    }
}
