using CassandraMigrationProcessor.Infrastructure;
using CassandraMigrationProcessor.Models;

namespace CassandraMigrationProcessor.Persistence;

/// <summary>
/// Disk-based implementation of <see cref="IDocumentStorage"/> +
/// <see cref="ILogStorage"/>. Stores documents as JSON files on the
/// local file system; log operations are delegated to
/// <see cref="LogPersistence"/>.
/// </summary>
public class DiskPersistence : IDocumentStorage, ILogStorage
{
    private const string FILE_EXTENSION = ".json";

    private static string _storagePath = string.Empty;
    private static LogPersistence? _logPersistence;
    private static readonly object _initLock = new();

    /// <summary>
    /// Initialises the disk persistence layer with the provided storage
    /// path. Thread-safe and idempotent.
    /// </summary>
    public void Initialize(string connectionStringOrPath)
    {
        if (_logPersistence != null) return;

        lock (_initLock)
        {
            if (_logPersistence != null) return;

            if (string.IsNullOrWhiteSpace(connectionStringOrPath))
                throw new ArgumentException("Storage path cannot be null or empty", nameof(connectionStringOrPath));

            try
            {
                _storagePath = connectionStringOrPath;
                FileSystem.EnsureDirectoryExists(_storagePath);
                _logPersistence = new LogPersistence(_storagePath);
            }
            catch (UnauthorizedAccessException ex)
            {
                // Surface an actionable message for the common Azure
                // App Service mis-deployment (write to read-only root).
                throw new InvalidOperationException(
                    $"State-store path '{connectionStringOrPath}' is not writable: {ex.Message}. " +
                    "Set the App Setting 'StateStoreConnectionStringOrPath' to a writable " +
                    "directory (e.g. 'D:\\home\\MigrationDrive' on Azure App Service Windows, " +
                    "'/home/MigrationDrive' on Linux).",
                    ex);
            }
            catch (Exception ex)
            {
                throw new InvalidOperationException($"Failed to initialize DiskPersistence. Details: {ex}", ex);
            }
        }
    }

    private static LogPersistence Logs()
        => _logPersistence ?? throw new InvalidOperationException(
            "DiskPersistence is not initialized. Call Initialize() first with a valid path.");

    /// <summary>
    /// Resolve a document id (e.g. <c>"job1\mu1.json"</c>) to an
    /// absolute file path; creates intermediate directories. Each
    /// segment is sanitised and the resolved path is verified to stay
    /// inside <see cref="_storagePath"/>.
    /// </summary>
    private static string GetFilePath(string id)
    {
        var parts = id.Split('\\', '/');
        var pathParts = new List<string> { _storagePath };
        for (int i = 0; i < parts.Length - 1; i++)
            pathParts.Add(SanitizeFileName(parts[i]));

        if (parts.Length > 1)
        {
            var dir = Path.Join(pathParts.ToArray());
            EnsureWithinStorage(dir, id);
            FileSystem.EnsureDirectoryExists(dir);
        }

        pathParts.Add(SanitizeFileName(parts[^1]));
        var finalPath = Path.Join(pathParts.ToArray());
        EnsureWithinStorage(finalPath, id);
        return finalPath;
    }

    private static string GetDirectoryPath(string id)
    {
        var parts = id.Split('\\', '/');
        var pathParts = new List<string> { _storagePath };
        foreach (var part in parts)
            pathParts.Add(SanitizeFileName(part));
        var finalPath = Path.Join(pathParts.ToArray());
        EnsureWithinStorage(finalPath, id);
        return finalPath;
    }

    /// <summary>
    /// Sanitises a single path segment. Rejects path-traversal
    /// segments and null-byte input so attacker-controlled ids cannot
    /// escape the storage root via <c>Path.Join</c>.
    /// </summary>
    private static string SanitizeFileName(string fileName)
    {
        if (string.IsNullOrEmpty(fileName))
            throw new ArgumentException("Path segment cannot be null or empty.", nameof(fileName));
        if (fileName == "." || fileName == "..")
            throw new ArgumentException($"Path segment '{fileName}' is not allowed (path traversal).", nameof(fileName));
        if (fileName.Contains('\0'))
            throw new ArgumentException("Path segment cannot contain null bytes.", nameof(fileName));

        var invalidChars = Path.GetInvalidFileNameChars();
        var sanitized = string.Join("_", fileName.Split(invalidChars, StringSplitOptions.RemoveEmptyEntries));

        // After stripping invalid chars the segment may have collapsed to
        // "." / ".." / empty; reject those too.
        if (string.IsNullOrEmpty(sanitized) || sanitized == "." || sanitized == "..")
            throw new ArgumentException($"Path segment '{fileName}' collapses to an unsafe value after sanitisation.", nameof(fileName));

        return sanitized;
    }

    /// <summary>
    /// Defence-in-depth: refuse to operate on any path that
    /// canonicalises outside <see cref="_storagePath"/>.
    /// </summary>
    private static void EnsureWithinStorage(string candidatePath, string originalId)
    {
        var rootFull = Path.GetFullPath(_storagePath);
        var candidateFull = Path.GetFullPath(candidatePath);

        // Append a trailing separator so a prefix match can't let
        // "/storage/migrationjobsX/..." pass as "/storage/migrationjobs".
        if (!rootFull.EndsWith(Path.DirectorySeparatorChar))
            rootFull += Path.DirectorySeparatorChar;

        if (!candidateFull.StartsWith(rootFull, StringComparison.Ordinal)
            && candidateFull + Path.DirectorySeparatorChar != rootFull)
        {
            throw new UnauthorizedAccessException(
                $"Refusing path '{originalId}' — resolves outside storage root.");
        }
    }

    private static void RequireJsonId(string id, string param)
    {
        if (string.IsNullOrWhiteSpace(id))
            throw new ArgumentException("ID cannot be null or empty", param);
        if (!id.EndsWith(FILE_EXTENSION))
            throw new ArgumentException($"ID must end with {FILE_EXTENSION} extension", param);
    }

    public bool Write(string id, string jsonContent)
    {
        _ = Logs();
        RequireJsonId(id, nameof(id));
        if (string.IsNullOrWhiteSpace(jsonContent))
            throw new ArgumentException("JSON content cannot be null or empty", nameof(jsonContent));

        return MigrationUtilities.SafeExecute(
            () => FileSystem.WriteAllText(GetFilePath(id), jsonContent),
            false, $"Write({id})");
    }

    public string? Read(string id)
    {
        _ = Logs();
        RequireJsonId(id, nameof(id));

        return MigrationUtilities.SafeExecute<string?>(
            () => FileSystem.ReadAllText(GetFilePath(id)),
            null, $"Read({id})");
    }

    public bool Exists(string id)
    {
        _ = Logs();
        if (string.IsNullOrWhiteSpace(id)) return false;
        if (!id.EndsWith(FILE_EXTENSION))
            throw new ArgumentException($"ID must end with {FILE_EXTENSION} extension", nameof(id));

        return MigrationUtilities.SafeExecute(
            () => FileSystem.Exists(GetFilePath(id)),
            false, $"Exists({id})");
    }

    /// <summary>
    /// Deletes a document (id ending in <c>.json</c>) or an entire folder
    /// (id without extension).
    /// </summary>
    public bool Delete(string id)
    {
        _ = Logs();
        if (string.IsNullOrWhiteSpace(id))
            throw new ArgumentException("ID cannot be null or empty", nameof(id));

        return MigrationUtilities.SafeExecute(() =>
        {
            if (id.EndsWith(FILE_EXTENSION))
            {
                var filePath = GetFilePath(id);
                if (!FileSystem.Exists(filePath)) return false;
                FileSystem.DeleteIfExists(filePath);
                return true;
            }
            return FileSystem.DeleteDirectory(GetDirectoryPath(id), recursive: true);
        }, false, $"Delete({id})");
    }

    public List<string> ListIds()
    {
        _ = Logs();

        return MigrationUtilities.SafeExecute(() =>
        {
            var files = FileSystem.ListFiles(_storagePath, "*" + FILE_EXTENSION, recursive: true);
            return files
                .Select(f => Path.GetRelativePath(_storagePath, f)
                    .Replace('/', '\\')
                    .Replace(Path.DirectorySeparatorChar, '\\'))
                .ToList();
        }, new List<string>(), "ListIds");
    }

    // --- Log operations delegated to LogPersistence ---

    public void PushLogEntry(string jobId, LogObject logObj)
        => Logs().PushLogEntry(jobId, logObj);

    public int GetLogCount(string id)
        => Logs().GetLogCount(id);

    public byte[] DownloadLogsPaginated(string id, int skip, int take)
        => Logs().DownloadLogsPaginated(id, skip, take);

    public byte[] ExportLogsAsBytes(string id, int topEntries = 20, int bottomEntries = 230)
        => Logs().ExportLogsAsBytes(id, topEntries, bottomEntries);

    public LogBucket ReadLogs(string id, out string fileName)
        => Logs().ReadLogs(id, out fileName);

    public long DeleteLogs(string jobId)
        => Logs().DeleteLogs(jobId);
}
