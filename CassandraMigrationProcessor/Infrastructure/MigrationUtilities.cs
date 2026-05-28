#pragma warning disable CS8600
#pragma warning disable CS8602
#pragma warning disable CS8604

namespace CassandraMigrationProcessor.Infrastructure;

/// <summary>
/// Cross-cutting helpers for process-wide concerns: file logging,
/// safe-dispose/execute wrappers, and the UI-side timestamp formatter.
/// </summary>
public static class MigrationUtilities
{
    #region Logging

    /// <summary>
    /// Out-of-band file trace used only where the structured
    /// <see cref="MigrationLog"/> isn't available yet or has itself
    /// failed: working-folder discovery in
    /// <see cref="DataDirectoryResolver"/> (runs before any log exists)
    /// and the persistence-layer crash fallback in
    /// <see cref="Persistence.LogPersistence"/>. Do not use this for
    /// regular lifecycle tracing — route those through the per-job
    /// <see cref="MigrationLog"/> instead.
    /// </summary>
    public static void LogToFile(
        string message,
        string fileName = "AutoStartLog.txt")
    {
        try
        {
            string path = Path.Combine(
                DataDirectoryResolver.GetWorkingFolder(), fileName);
            string timestamp =
                DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.fff");
            string logEntry =
                $"[{timestamp} UTC] {message}{Environment.NewLine}";

            System.IO.File.AppendAllText(path, logEntry);
        }
        catch (Exception ex)
        {
            Console.Error.WriteLine($"[WARN] LogToFile failed: {ex.Message}");
        }
    }

    /// <summary>
    /// Disposes an object, swallowing and logging any exception.
    /// Use instead of try { obj?.Dispose(); } catch { ... } blocks.
    /// </summary>
    public static void SafeDispose(IDisposable? obj, string name)
    {
        try { obj?.Dispose(); }
        catch (Exception ex)
        {
            Console.Error.WriteLine($"[WARN] {name} dispose failed: {ex.Message}");
        }
    }

    /// <summary>
    /// Disposes a Cassandra session AND its owning Cluster. The driver's
    /// connection pool is owned by Cluster, not Session — disposing only
    /// the session leaks sockets and queues. Calling Cluster.Dispose()
    /// shuts down the pool and disposes every session it owns, so we do
    /// not separately call session.Dispose() here. Safe to pass null.
    /// </summary>
    public static void SafeDisposeSession(Cassandra.ISession? session, string name)
    {
        if (session == null) return;
        try { session.Cluster?.Dispose(); }
        catch (Exception ex)
        {
            Console.Error.WriteLine($"[WARN] {name} cluster dispose failed: {ex.Message}");
        }
    }

    /// <summary>
    /// Executes an action, returning a fallback on failure.
    /// Shared helper for the repeated try/catch-warn-return pattern.
    /// </summary>
    public static T SafeExecute<T>(Func<T> action, T fallback, string operation)
    {
        try { return action(); }
        catch (Exception ex)
        {
            Console.WriteLine($"[WARN] {operation}: {ex.Message}");
            return fallback;
        }
    }

    public static void SafeExecuteVoid(Action action, string operation)
    {
        try { action(); }
        catch (Exception ex)
        {
            Console.WriteLine($"[WARN] {operation}: {ex.Message}");
        }
    }

    #endregion

    /// <summary>
    /// UI-side "X sec / X min / Xh Xm" lag formatter. Used by the web app
    /// to render last-checked timestamps. Stays here because it's pure
    /// presentation glue, not a domain concept.
    /// </summary>
    public static string GetTimestampDiff(DateTime timestamp)
    {
        var lag = DateTime.UtcNow - timestamp;
        if (lag.TotalSeconds < 0) return "Invalid";
        if (lag.TotalSeconds < 60)
            return $"{(int)lag.TotalSeconds} sec";
        else if (lag.TotalMinutes < 60)
            return $"{(int)lag.TotalMinutes} min {(int)lag.Seconds} sec";
        else
            return $"{(int)lag.TotalHours}h {(int)lag.Minutes}m";
    }
}
