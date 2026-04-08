namespace CassandraMigrationProcessor.Helpers.JobManagement
{
    /// <summary>
    /// Helper to adjust worker pool sizes for copy operations.
    /// In the Cassandra migration, dump and restore are unified
    /// as copy workers, but the pool management remains.
    /// </summary>
    internal static class WorkerCountHelper
    {
        public static int AdjustDumpWorkers(
            int newCount,
            int currentActive,
            WorkerPoolManager pool,
            Log log)
        {
            return AdjustWorkers(
                "Copy-Read", newCount, currentActive, pool, log);
        }

        public static int AdjustRestoreWorkers(
            int newCount,
            int currentActive,
            WorkerPoolManager pool,
            Log log)
        {
            return AdjustWorkers(
                "Copy-Write", newCount, currentActive, pool, log);
        }

        private static int AdjustWorkers(
            string poolName,
            int newCount,
            int currentActive,
            WorkerPoolManager pool,
            Log log)
        {
            if (newCount < 1) newCount = 1;
            int currentMax = pool.MaxWorkers;
            if (newCount == currentMax) return newCount;

            log.WriteLine(
                $"{poolName} pool target: " +
                $"{currentMax} -> {newCount} " +
                $"(active={currentActive})",
                LogType.Debug);

            // Pool doesn't support runtime resize;
            // return the requested count for callers to track.
            return newCount;
        }
    }
}
