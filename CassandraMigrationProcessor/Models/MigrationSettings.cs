using Newtonsoft.Json;
using CassandraMigrationProcessor.Context;
using System;

namespace CassandraMigrationProcessor
{
    public class MigrationSettings : ICloneable
    {
        // Default values
        private const int DefaultCqlCopyPageSize = 500;
        private const int DefaultChangeFeedMaxRows = 10000;
        private const int DefaultChangeFeedBatchDuration = 120;
        private const int DefaultChangeFeedBatchDurationMin = 30;
        private const int DefaultChangeFeedMaxTables = 5;
        private const int DefaultChangeFeedPollIntervalMs = 5000;
        private const int DefaultLogPageSize = 5000;

        // Clamping bounds
        private const int MaxChangeFeedMaxRows = 10000;
        private const int MinChangeFeedBatchDuration = 20;
        private const int MinLogPageSize = 1000;
        private const int MaxLogPageSize = 100000;

        public int LogPageSize { get; set; }
        public int CqlCopyPageSize { get; set; }
        public int ChangeFeedMaxRowsInBatch { get; set; }
        public int ChangeFeedBatchDuration { get; set; }
        public int ChangeFeedBatchDurationMin { get; set; }
        public int ChangeFeedMaxTablesInBatch { get; set; }
        public int ChangeFeedPollIntervalMs { get; set; }
        public bool ChangeFeedFullFidelity { get; set; }
        public int MaxFeedRangeParallelism { get; set; }

        private string _filePath = string.Empty;

        public MigrationSettings()
        {
            _filePath = $"{JobStore.JobsFolder}\\config.json";
        }

        public object Clone()
        {
            var json = JsonConvert.SerializeObject(this);
            return JsonConvert.DeserializeObject<MigrationSettings>(json)
                ?? new MigrationSettings();
        }

        private static int DefaultOrValue(int loaded, int defaultVal)
            => loaded == 0 ? defaultVal : loaded;

        private static int DefaultParallelism()
            => Math.Max(4, Environment.ProcessorCount * 2);

        private void ApplyDefaults()
        {
            CqlCopyPageSize = DefaultCqlCopyPageSize;
            ChangeFeedMaxRowsInBatch = DefaultChangeFeedMaxRows;
            ChangeFeedBatchDuration = DefaultChangeFeedBatchDuration;
            ChangeFeedBatchDurationMin = DefaultChangeFeedBatchDurationMin;
            ChangeFeedMaxTablesInBatch = DefaultChangeFeedMaxTables;
            ChangeFeedPollIntervalMs = DefaultChangeFeedPollIntervalMs;
            ChangeFeedFullFidelity = false;
            MaxFeedRangeParallelism = DefaultParallelism();
            LogPageSize = DefaultLogPageSize;
        }

        private void ClampValues()
        {
            if (ChangeFeedMaxRowsInBatch > MaxChangeFeedMaxRows)
                ChangeFeedMaxRowsInBatch = MaxChangeFeedMaxRows;
            if (ChangeFeedBatchDuration < MinChangeFeedBatchDuration)
                ChangeFeedBatchDuration = DefaultChangeFeedBatchDuration;
            if (LogPageSize < MinLogPageSize)
                LogPageSize = MinLogPageSize;
            if (LogPageSize > MaxLogPageSize)
                LogPageSize = MaxLogPageSize;
        }

        public void Load()
        {
            if (MigrationJobContext.Store.DocumentExists(_filePath))
            {
                string json = MigrationJobContext.Store.ReadDocument(_filePath);
                var loaded =
                    JsonConvert.DeserializeObject<MigrationSettings>(json);
                if (loaded != null)
                {
                    CqlCopyPageSize = DefaultOrValue(loaded.CqlCopyPageSize, DefaultCqlCopyPageSize);
                    ChangeFeedMaxRowsInBatch = DefaultOrValue(loaded.ChangeFeedMaxRowsInBatch, DefaultChangeFeedMaxRows);
                    ChangeFeedBatchDuration = DefaultOrValue(loaded.ChangeFeedBatchDuration, DefaultChangeFeedBatchDuration);
                    ChangeFeedBatchDurationMin = DefaultOrValue(loaded.ChangeFeedBatchDurationMin, DefaultChangeFeedBatchDurationMin);
                    ChangeFeedMaxTablesInBatch = DefaultOrValue(loaded.ChangeFeedMaxTablesInBatch, DefaultChangeFeedMaxTables);
                    LogPageSize = DefaultOrValue(loaded.LogPageSize, DefaultLogPageSize);
                    ChangeFeedPollIntervalMs = DefaultOrValue(loaded.ChangeFeedPollIntervalMs, DefaultChangeFeedPollIntervalMs);
                    ChangeFeedFullFidelity = loaded.ChangeFeedFullFidelity;
                    MaxFeedRangeParallelism = DefaultOrValue(loaded.MaxFeedRangeParallelism, DefaultParallelism());

                    ClampValues();
                    return;
                }
            }

            ApplyDefaults();
        }

        public bool Save(out string errorMessage)
        {
            try
            {
                string json = JsonConvert.SerializeObject(this);
                MigrationJobContext.Store.UpsertDocument(_filePath, json);
                errorMessage = string.Empty;
                return true;
            }
            catch (Exception ex)
            {
                errorMessage = $"Error saving data: {ex}";
                return false;
            }
        }
    }
}