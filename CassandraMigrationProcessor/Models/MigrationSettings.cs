using Newtonsoft.Json;
using System;

namespace CassandraMigrationProcessor.Models
{
    public class MigrationSettings : ICloneable
    {
        // Default values
        internal const int DefaultCqlCopyPageSize = 500;
        internal const int DefaultChangeFeedMaxRows = 10000;
        internal const int DefaultChangeFeedBatchDuration = 120;
        internal const int DefaultChangeFeedBatchDurationMin = 30;
        internal const int DefaultChangeFeedMaxTables = 5;
        internal const int DefaultChangeFeedPollIntervalMs = 5000;
        internal const int DefaultLogPageSize = 5000;

        // Clamping bounds
        internal const int MaxChangeFeedMaxRows = 10000;
        internal const int MinChangeFeedBatchDuration = 20;
        internal const int MinLogPageSize = 1000;
        internal const int MaxLogPageSize = 100000;

        public int LogPageSize { get; set; }
        public int CqlCopyPageSize { get; set; }
        public int ChangeFeedMaxRowsInBatch { get; set; }
        public int ChangeFeedBatchDuration { get; set; }
        public int ChangeFeedBatchDurationMin { get; set; }
        public int ChangeFeedMaxTablesInBatch { get; set; }
        public int ChangeFeedPollIntervalMs { get; set; }
        public int MaxFeedRangeParallelism { get; set; }

        public MigrationSettings()
        {
        }

        public object Clone()
        {
            var json = JsonConvert.SerializeObject(this);
            return JsonConvert.DeserializeObject<MigrationSettings>(json)
                ?? new MigrationSettings();
        }

        private static int DefaultOrValue(int loaded, int defaultVal)
            => loaded == 0 ? defaultVal : loaded;

        internal static int DefaultParallelism()
            => Math.Max(4, Environment.ProcessorCount * 2);

        internal void ApplyDefaults()
        {
            CqlCopyPageSize = DefaultCqlCopyPageSize;
            ChangeFeedMaxRowsInBatch = DefaultChangeFeedMaxRows;
            ChangeFeedBatchDuration = DefaultChangeFeedBatchDuration;
            ChangeFeedBatchDurationMin = DefaultChangeFeedBatchDurationMin;
            ChangeFeedMaxTablesInBatch = DefaultChangeFeedMaxTables;
            ChangeFeedPollIntervalMs = DefaultChangeFeedPollIntervalMs;
            MaxFeedRangeParallelism = DefaultParallelism();
            LogPageSize = DefaultLogPageSize;
        }

        internal void ClampValues()
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

        internal void ApplyLoaded(MigrationSettings loaded)
        {
            CqlCopyPageSize = DefaultOrValue(loaded.CqlCopyPageSize, DefaultCqlCopyPageSize);
            ChangeFeedMaxRowsInBatch = DefaultOrValue(loaded.ChangeFeedMaxRowsInBatch, DefaultChangeFeedMaxRows);
            ChangeFeedBatchDuration = DefaultOrValue(loaded.ChangeFeedBatchDuration, DefaultChangeFeedBatchDuration);
            ChangeFeedBatchDurationMin = DefaultOrValue(loaded.ChangeFeedBatchDurationMin, DefaultChangeFeedBatchDurationMin);
            ChangeFeedMaxTablesInBatch = DefaultOrValue(loaded.ChangeFeedMaxTablesInBatch, DefaultChangeFeedMaxTables);
            LogPageSize = DefaultOrValue(loaded.LogPageSize, DefaultLogPageSize);
            ChangeFeedPollIntervalMs = DefaultOrValue(loaded.ChangeFeedPollIntervalMs, DefaultChangeFeedPollIntervalMs);
            MaxFeedRangeParallelism = DefaultOrValue(loaded.MaxFeedRangeParallelism, DefaultParallelism());
            ClampValues();
        }
    }
}