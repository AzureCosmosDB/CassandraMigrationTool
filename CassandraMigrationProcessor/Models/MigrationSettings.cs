using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using CassandraMigrationProcessor.Context;
using System;
using System.IO;

namespace CassandraMigrationProcessor
{
    public class MigrationSettings : ICloneable
    {
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
            _filePath = $"migrationjobs\\config.json";
        }

        public object Clone()
        {
            var json = JsonConvert.SerializeObject(this);
            return JsonConvert.DeserializeObject<MigrationSettings>(json)
                ?? new MigrationSettings();
        }

        public void Load()
        {
            bool initialized = false;
            if (MigrationJobContext.Store.DocumentExists(_filePath))
            {
                string json = MigrationJobContext.Store.ReadDocument(_filePath);
                var loadedObject =
                    JsonConvert.DeserializeObject<MigrationSettings>(json);
                if (loadedObject != null)
                {
                    CqlCopyPageSize = loadedObject.CqlCopyPageSize == 0
                        ? 500 : loadedObject.CqlCopyPageSize;
                    ChangeFeedMaxRowsInBatch =
                        loadedObject.ChangeFeedMaxRowsInBatch == 0
                        ? 10000
                        : loadedObject.ChangeFeedMaxRowsInBatch;
                    ChangeFeedBatchDuration =
                        loadedObject.ChangeFeedBatchDuration == 0
                        ? 120
                        : loadedObject.ChangeFeedBatchDuration;
                    ChangeFeedBatchDurationMin =
                        loadedObject.ChangeFeedBatchDurationMin == 0
                        ? 30
                        : loadedObject.ChangeFeedBatchDurationMin;
                    ChangeFeedMaxTablesInBatch =
                        loadedObject.ChangeFeedMaxTablesInBatch == 0
                        ? 5
                        : loadedObject.ChangeFeedMaxTablesInBatch;
                    LogPageSize = loadedObject.LogPageSize == 0
                        ? 5000 : loadedObject.LogPageSize;

                    ChangeFeedPollIntervalMs =
                        loadedObject.ChangeFeedPollIntervalMs == 0
                        ? 5000
                        : loadedObject.ChangeFeedPollIntervalMs;
                    ChangeFeedFullFidelity =
                        loadedObject.ChangeFeedFullFidelity;
                    MaxFeedRangeParallelism =
                        loadedObject.MaxFeedRangeParallelism == 0
                        ? Math.Max(4, Environment.ProcessorCount * 2)
                        : loadedObject.MaxFeedRangeParallelism;

                    initialized = true;

                    if (ChangeFeedMaxRowsInBatch > 10000)
                        ChangeFeedMaxRowsInBatch = 10000;
                    if (ChangeFeedBatchDuration < 20)
                        ChangeFeedBatchDuration = 120;
                    if (LogPageSize < 1000)
                        LogPageSize = 1000;
                    if (LogPageSize > 100000)
                        LogPageSize = 100000;
                }
            }
            if (!initialized)
            {
                CqlCopyPageSize = 500;
                ChangeFeedMaxRowsInBatch = 10000;
                ChangeFeedBatchDuration = 120;
                ChangeFeedBatchDurationMin = 30;
                ChangeFeedMaxTablesInBatch = 5;
                ChangeFeedPollIntervalMs = 5000;
                ChangeFeedFullFidelity = false;
                MaxFeedRangeParallelism = Math.Max(4,
                    Environment.ProcessorCount * 2);
                LogPageSize = 5000;
            }
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