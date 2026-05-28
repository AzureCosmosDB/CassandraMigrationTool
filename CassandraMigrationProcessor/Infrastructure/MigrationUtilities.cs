using CassandraMigrationProcessor.Models;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using System.Text.RegularExpressions;

#pragma warning disable CS8600
#pragma warning disable CS8602
#pragma warning disable CS8604

namespace CassandraMigrationProcessor.Infrastructure;
public static class MigrationUtilities
{
    public static bool IsOnline(Job job)
    {
        if (job == null) return false;
        return job.CDCMode != CDCMode.Offline;
    }

    public static bool IsMigrationUnitValid(TableMigrationSummary mu)
    {
        // Allow both OK and Failed status — Failed tables
        // are retried on resume (e.g. after token expiry).
        // Only NotFound tables are truly invalid.
        return mu.SourceStatus == TableStatus.OK
            || mu.SourceStatus == TableStatus.Failed;
    }

    #region Logging

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

    public static string GenerateMigrationUnitId(
        string keyspaceName, string tableName)
    {
        using (var sha = SHA256.Create())
        {
            byte[] hashBytes = sha.ComputeHash(
                Encoding.UTF8.GetBytes(
                    $"{keyspaceName}.{tableName}"));
            return BitConverter.ToString(hashBytes)
                .Replace("-", "").Substring(0, 16).ToLower();
        }
    }

    public static (long Total, long Inserted, long Failed)
        GetProcessedTotals(TableMigration mu)
    {
        long inserted = mu.CopyChunks?
            .Sum(c => c.TargetInsertedRowCount) ?? 0;
        long failed = mu.CopyChunks?
            .Sum(c => c.TargetFailedRowCount) ?? 0;
        long total = inserted + failed;
        return (total, inserted, failed);
    }

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

    public static bool IsOfflineJobCompleted(Job job)
    {
        if (job == null || job.Tables.Count == 0)
            return false;

        return job.Tables
            .Where(mu => IsMigrationUnitValid(mu))
            .All(mu => mu.CopyComplete);
    }

    public static bool AnyValidTable(Job job)
    {
        if (job == null)
            return false;
        return job.Tables
            .Any(mu => IsMigrationUnitValid(mu));
    }

    /// <summary>
    /// Validates that a string is a safe CQL identifier
    /// (alphanumeric, underscore, or hyphen only).
    /// Throws ArgumentException if invalid.
    /// </summary>
    public static string ValidateCqlIdentifier(string identifier)
    {
        if (string.IsNullOrWhiteSpace(identifier))
            throw new ArgumentException("CQL identifier cannot be empty");
        if (!Regex.IsMatch(identifier, @"^[a-zA-Z0-9_\-]+$"))
            throw new ArgumentException($"Invalid CQL identifier: {identifier}");
        return identifier;
    }
}
