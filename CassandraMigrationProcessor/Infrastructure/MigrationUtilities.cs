namespace CassandraMigrationProcessor.Infrastructure;

using CassandraMigrationProcessor.Models;

/// <summary>
/// Cross-cutting helpers for process-wide concerns: file logging,
/// safe-dispose/execute wrappers, and the UI-side timestamp formatter.
/// </summary>
public static class MigrationUtilities
{
    private static void WriteWarn(string text, MigrationLog? log)
    {
        if (log != null)
        {
            try { log.WriteLine(text, LogType.Warning); return; }
            catch (Exception ex) when (ex is IOException or ObjectDisposedException or InvalidOperationException)
            {
                Console.Error.WriteLine($"[WARN] Failed to write to log: {ex.Message}");
            }
        }
        Console.Error.WriteLine($"[WARN] {text}");
    }

    #region Logging

    /// <summary>
    /// Out-of-band file trace used only where the structured
    /// <see cref="MigrationLog"/> isn't available yet or has itself
    /// failed (working-folder discovery, persistence-layer crash
    /// fallback). Do not use for regular tracing.
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
    /// Pass <paramref name="log"/> to route the warning through the
    /// structured log when one is available; otherwise falls back to
    /// <see cref="Console.Error"/>.
    /// </summary>
    public static void SafeDispose(IDisposable? obj, string? name, MigrationLog? log = null)
    {
        try { obj?.Dispose(); }
        catch (Exception ex)
        {
            WriteWarn($"{name ?? "<unnamed>"} dispose failed: {ex.Message}", log);
        }
    }

    /// <summary>
    /// Disposes a Cassandra session AND its owning Cluster. The
    /// driver's connection pool is owned by Cluster, not Session —
    /// disposing only the session leaks sockets. Skips
    /// <see cref="CassandraDriver.NullSession"/> (simulated run) whose
    /// Cluster property throws on access.
    /// </summary>
    public static void SafeDisposeSession(Cassandra.ISession? session, string? name, MigrationLog? log = null)
    {
        if (session == null) return;
        if (session is CassandraDriver.NullSession) return;
        try { session.Cluster?.Dispose(); }
        catch (Exception ex)
        {
            WriteWarn($"{name ?? "<unnamed>"} cluster dispose failed: {ex.Message}", log);
        }
    }

    /// <summary>
    /// Executes a function, returning a fallback on failure. Shared
    /// for the try/catch-warn-return pattern across persistence
    /// gateways. I/O exceptions are logged and swallowed.
    /// </summary>
    /// <remarks>
    /// <see cref="OperationCanceledException"/> is rethrown so
    /// cooperative cancellation isn't masked as a silent fallback
    /// return.
    /// </remarks>
    public static T SafeExecute<T>(Func<T> action, T fallback, string? operation, MigrationLog? log = null)
    {
        try { return action(); }
        catch (OperationCanceledException) { throw; }
        catch (Exception ex)
        {
            WriteWarn($"{operation ?? "<unnamed>"}: {ex.Message}", log);
            return fallback;
        }
    }

    public static void SafeExecuteVoid(Action action, string? operation, MigrationLog? log = null)
    {
        try { action(); }
        catch (OperationCanceledException) { throw; }
        catch (Exception ex)
        {
            WriteWarn($"{operation ?? "<unnamed>"}: {ex.Message}", log);
        }
    }

    #endregion

    /// <summary>
    /// UI-side lag formatter ("X sec / X min / Xh Xm").
    /// </summary>
    public static string GetTimestampDiff(DateTime timestamp)
    {
        var lag = DateTime.UtcNow - timestamp;
        if (lag.TotalSeconds < 0) return "Invalid";
        if (lag.TotalSeconds < 60) return $"{(int)lag.TotalSeconds} sec";
        if (lag.TotalMinutes < 60) return $"{(int)lag.TotalMinutes} min {lag.Seconds} sec";
        return $"{(int)lag.TotalHours}h {lag.Minutes}m";
    }
}
