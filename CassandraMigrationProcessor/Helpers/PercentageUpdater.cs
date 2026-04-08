using CassandraMigrationProcessor.Context;
using System;
using System.Collections.Generic;
using System.Linq;

namespace CassandraMigrationProcessor.Helpers
{
    /// <summary>
    /// Periodically recalculates CopyPercent for active
    /// migration units based on chunk-level progress.
    /// </summary>
    public static class PercentageUpdater
    {
        private const int PERCENTAGE_UPDATE_INTERVAL_MS = 5000;

        private static SafeDictionary<string, bool> _activeTrackers =
            new SafeDictionary<string, bool>();
        private static List<string> _trackersToRemove = new List<string>();
        private static System.Timers.Timer _timer =
            new System.Timers.Timer(PERCENTAGE_UPDATE_INTERVAL_MS);
        private static Log _log;

        public static void Initialize()
        {
            MigrationJobContext.AddVerboseLog(
                "PercentageUpdater Initialize Invoked");
            try
            {
                _activeTrackers = new SafeDictionary<string, bool>();
                _trackersToRemove = new List<string>();
                _timer.Stop();
            }
            finally { }
        }

        public static void AddToPercentageTracker(
            string id, bool unused, Log log)
        {
            MigrationJobContext.AddVerboseLog(
                $"PercentageUpdater.AddToPercentageTracker: " +
                $"id={id}");
            _log = log;
            var key = $"{id}_copy";

            if (!_activeTrackers.ContainsKey(key))
                _activeTrackers.AddOrUpdate(key, false);

            if (!_timer.Enabled)
            {
                _timer.Elapsed += (sender, e) => TimerTick();
                _timer.Start();
            }
        }

        public static void RemovePercentageTracker(
            string id, bool unused, Log log)
        {
            MigrationJobContext.AddVerboseLog(
                $"PercentageUpdater.RemovePercentageTracker: " +
                $"id={id}");
            _log = log;
            var key = $"{id}_copy";
            _trackersToRemove.Add(key);
        }

        private static void TimerTick()
        {
            foreach (var kvp in _activeTrackers.GetAll())
            {
                string id = kvp.Key.Split("_")[0];

                if (_trackersToRemove.Contains(kvp.Key))
                {
                    MigrationJobContext.AddVerboseLog(
                        $"PercentageUpdater remove({kvp.Key}) " +
                        $"count={_activeTrackers.Count}");
                    _activeTrackers.Remove(kvp.Key);

                    if (_activeTrackers.Count == 0)
                    {
                        _timer.Stop();
                        return;
                    }
                    _trackersToRemove.Remove(kvp.Key);
                }

                ProcessMigrationUnitProgress(id);
            }
        }

        private static bool ProcessMigrationUnitProgress(
            string id)
        {
            MigrationJobContext.AddVerboseLog(
                $"ProcessMigrationUnitProgress mu={id}");

            var mu = MigrationJobContext.GetMigrationUnit(id);
            if (mu == null)
            {
                MigrationJobContext.AddVerboseLog(
                    "ProcessMigrationUnitProgress: MU not found");
                return false;
            }

            if (mu.CopyComplete || mu.CopyPercent >= 100)
                return true;

            bool hasActiveChunks = false;
            foreach (var chunk in mu.MigrationChunks)
            {
                if (chunk.IsDownloaded != true
                    && chunk.SourceQueryRowCount > 0)
                {
                    hasActiveChunks = true;
                    break;
                }
            }

            if (hasActiveChunks)
            {
                mu.CopyPercent =
                    CalculateOverallPercentFromAllChunks(
                        mu, _log);
                mu.UpdateParentJob();

                if (mu.CopyPercent >= 99.99)
                {
                    mu.CopyComplete = true;
                    RemovePercentageTracker(id, false, _log);
                    MigrationJobContext.SaveMigrationUnit(
                        mu, true);
                }
            }
            return true;
        }

        /// <summary>
        /// Calculates overall copy progress from all chunks
        /// using SourceQueryRowCount and TargetInsertedRowCount.
        /// </summary>
        public static double CalculateOverallPercentFromAllChunks(
            MigrationUnit mu, Log log)
        {
            MigrationJobContext.AddVerboseLog(
                $"PercentageUpdater.CalcPercent: " +
                $"{mu.KeyspaceName}.{mu.TableName} " +
                $"copyComplete={mu.CopyComplete} " +
                $"copyPercent={mu.CopyPercent}");

            double totalPercent = 0;
            long totalRows = Helper.GetMigrationUnitRowCount(mu);
            if (totalRows == 0) return 0;

            for (int i = 0; i < mu.MigrationChunks.Count; i++)
            {
                var c = mu.MigrationChunks[i];
                if (c.SourceQueryRowCount == 0) continue;

                double chunkContrib =
                    (double)c.SourceQueryRowCount / totalRows;
                double chunkPercent = 0;

                if (c.IsDownloaded == true
                    && c.IsUploaded == true)
                {
                    chunkPercent = 100;
                }
                else if (c.TargetInsertedRowCount > 0)
                {
                    chunkPercent = Math.Min(100,
                        (double)c.TargetInsertedRowCount
                        / c.SourceQueryRowCount * 100);
                }

                totalPercent += chunkPercent * chunkContrib;
            }

            return Math.Min(100, totalPercent);
        }

        public static void StopPercentageTimer()
        {
            if (_timer != null && _timer.Enabled)
                _timer.Stop();
            _activeTrackers.Clear();
        }
    }
}
