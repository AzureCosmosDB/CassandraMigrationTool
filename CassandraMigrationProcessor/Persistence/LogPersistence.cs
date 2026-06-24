using CassandraMigrationProcessor.Infrastructure;
using System.Text;
using CassandraMigrationProcessor.Models;

namespace CassandraMigrationProcessor.Persistence;
/// <summary>
/// Binary log storage operations: push, read, paginate,
/// export, and delete migration log entries.
/// </summary>
public class LogPersistence
{
    private const int MaxLogFileSize = 1_000_000;
    private const string LogFolder = "migrationlogs";
    private readonly string _storagePath;
    private static readonly object _readLock = new object();

    public LogPersistence(string storagePath)
    {
        if (string.IsNullOrWhiteSpace(storagePath))
            throw new ArgumentException("Storage path cannot be null or empty", nameof(storagePath));
        _storagePath = storagePath;
    }

    private string LogBinPath(string id) =>
        Path.Join(_storagePath, LogFolder, $"{id}.bin");

    public void PushLogEntry(string jobId, LogObject logObject)
    {
        if (string.IsNullOrWhiteSpace(jobId))
            throw new ArgumentException("ID cannot be null or empty", nameof(jobId));
        if (logObject == null)
            throw new ArgumentNullException(nameof(logObject));

        MigrationUtilities.SafeExecuteVoid(() =>
        {
            var binPath = LogBinPath(jobId);
            FileSystem.EnsureDirectoryExists(Path.GetDirectoryName(binPath)!);

            using var fs = FileSystem.OpenAppend(binPath);
            using var bw = new BinaryWriter(fs);
            WriteEntry(bw, logObject);
        }, $"PushLogEntry({jobId})");
    }

    public int GetLogCount(string id)
    {
        var binPath = LogBinPath(id);
        if (!FileSystem.Exists(binPath)) return 0;

        return MigrationUtilities.SafeExecute(() =>
        {
            using var fs = FileSystem.OpenReadShared(binPath);
            if (fs == null) return 0;
            using var br = new BinaryReader(fs);
            return CollectEntryOffsets(br).Count;
        }, 0, $"GetLogCount({id})");
    }

    public byte[] DownloadLogsPaginated(string id, int skip, int take)
    {
        var binPath = LogBinPath(id);
        if (!FileSystem.Exists(binPath))
            return Array.Empty<byte>();

        var logs = new List<LogObject>();
        MigrationUtilities.SafeExecuteVoid(() =>
        {
            using var fs = FileSystem.OpenReadShared(binPath);
            if (fs == null) return;
            using var br = new BinaryReader(fs);

            var offsets = CollectEntryOffsets(br).Skip(skip).Take(take).ToList();
            ReadEntriesAtOffsets(fs, br, offsets, logs);
        }, $"DownloadLogsPaginated({id})");

        return EncodeLogsAsBytes(logs);
    }

    public byte[] ExportLogsAsBytes(string id, int topEntries = 20, int bottomEntries = 230)
    {
        var logs = ParseLogBinFile(LogBinPath(id), topEntries, bottomEntries);
        return EncodeLogsAsBytes(logs.Logs);
    }

    public LogBucket ReadLogs(string id, out string fileName)
    {
        fileName = id;

        try
        {
            lock (_readLock)
            {
                var binPath = LogBinPath(id);
                if (!FileSystem.Exists(binPath))
                    return new LogBucket();

                var logBucket = ParseLogBinFile(binPath);
                if (logBucket.Logs == null || logBucket.Logs.Count == 0)
                    return HandleError(id, binPath, out fileName);
                return logBucket;
            }
        }
        catch (Exception ex)
        {
            throw new InvalidOperationException("MigrationLog Init failed", ex);
        }
    }

    /// <summary>
    /// Deletes all MigrationLog entries for a given JobId by deleting the binary MigrationLog file.
    /// </summary>
    public long DeleteLogs(string jobId)
    {
        if (string.IsNullOrWhiteSpace(jobId))
            throw new ArgumentException("Job ID cannot be null or empty", nameof(jobId));

        return MigrationUtilities.SafeExecute(() =>
        {
            var binPath = LogBinPath(jobId);
            if (!FileSystem.Exists(binPath)) return 0L;
            FileSystem.DeleteIfExists(binPath);
            return 1L;
        }, -1L, $"DeleteLogs({jobId})");
    }

    private LogBucket HandleError(string jobId, string binPath, out string backupFileName)
    {
        backupFileName = CreateFileCopyWithTimestamp(binPath);
        FileSystem.DeleteIfExists(binPath);

        var logs = new List<LogObject>
        {
            new LogObject(LogType.Error, $"Unable to load the MigrationLog file; original file backed up as {backupFileName}"),
        };
        WriteBinaryLog(jobId, logs);
        return ParseLogBinFile(binPath);
    }

    private void WriteBinaryLog(string id, List<LogObject> logs)
    {
        if (logs == null || logs.Count == 0) return;

        var binPath = LogBinPath(id);
        try
        {
            FileSystem.EnsureDirectoryExists(Path.GetDirectoryName(binPath)!);
            using var fs = FileSystem.OpenAppend(binPath);
            using var bw = new BinaryWriter(fs);

            foreach (var logEntry in logs)
            {
                try { WriteEntry(bw, logEntry); }
                catch (Exception ex)
                {
                    Console.Error.WriteLine($"LogPersistence error: {ex.Message}");
                    // Continue writing other logs
                }
            }
            bw.Flush();
            fs.Flush();
        }
        catch (Exception ex)
        {
            MigrationUtilities.LogToFile($"Error writing binary MigrationLog. Details: {ex}", "DiskPersistence.txt");
            throw;
        }
    }

    private static void WriteEntry(BinaryWriter bw, LogObject entry)
    {
        var bytes = Encoding.UTF8.GetBytes(entry.Message ?? string.Empty);
        bw.Write(bytes.Length);
        bw.Write(bytes);
        bw.Write((byte)entry.Type);
        bw.Write(entry.Datetime.ToBinary());
    }

    private LogBucket ParseLogBinFile(string binPath, int topCount = 20, int bottomCount = 280)
    {
        var logBucket = new LogBucket { Logs = new List<LogObject>() };
        if (!FileSystem.Exists(binPath)) return logBucket;

        MigrationUtilities.SafeExecuteVoid(() =>
        {
            using var fs = FileSystem.OpenReadShared(binPath);
            if (fs == null) return;
            using var br = new BinaryReader(fs);

            var offsets = CollectEntryOffsets(br);
            var selected = SelectTopAndBottom(offsets, topCount, bottomCount);
            ReadEntriesAtOffsets(fs, br, selected, logBucket.Logs);
        }, "ParseLogBinFile");

        return logBucket;
    }

    /// <summary>
    /// Return the first <paramref name="topCount"/> and last
    /// <paramref name="bottomCount"/> elements, distinct and in order.
    /// 0+0 returns empty (never reads the whole file).
    /// </summary>
    private static List<long> SelectTopAndBottom(List<long> offsets, int topCount, int bottomCount)
    {
        if (topCount <= 0 && bottomCount <= 0) return new List<long>();
        if (offsets.Count <= topCount + bottomCount) return offsets;

        return offsets.Take(Math.Max(0, topCount))
            .Concat(offsets.Skip(Math.Max(0, offsets.Count - Math.Max(0, bottomCount))))
            .Distinct()
            .OrderBy(i => i)
            .ToList();
    }

    /// <summary>
    /// Walks the binary log from the reader's current position,
    /// returning each well-formed entry's offset. Stops at first
    /// malformed record (treated as soft EOF).
    /// </summary>
    private List<long> CollectEntryOffsets(BinaryReader br)
    {
        var offsets = new List<long>();
        var fs = br.BaseStream;

        while (fs.Position < fs.Length)
        {
            long offset = fs.Position;
            try
            {
                if (fs.Position + 4 > fs.Length) break;

                int msgLen = br.ReadInt32();
                if (msgLen < 0 || msgLen > MaxLogFileSize) break;

                long bytesToSkip = msgLen + 1 + 8;
                if (fs.Position + bytesToSkip > fs.Length) break;

                fs.Seek(bytesToSkip, SeekOrigin.Current);
                offsets.Add(offset);
            }
            catch (Exception ex)
            {
                Console.Error.WriteLine($"LogPersistence read error: {ex.Message}");
                break;
            }
        }

        return offsets;
    }

    /// <summary>
    /// Seeks to each offset and appends the decoded entry. Offsets
    /// that fail to decode are skipped.
    /// </summary>
    private void ReadEntriesAtOffsets(
        Stream fs, BinaryReader br, IEnumerable<long> offsets, List<LogObject> destination)
    {
        foreach (var offset in offsets)
        {
            fs.Position = offset;
            var logEntry = TryReadLogEntry(br);
            if (logEntry != null)
                destination.Add(logEntry);
        }
    }

    /// <summary>
    /// Renders entries as <c>typeChar|MM/dd/yyyy HH:mm:ss|message</c>
    /// UTF-8 lines.
    /// </summary>
    private static byte[] EncodeLogsAsBytes(List<LogObject> logs)
    {
        var sb = new StringBuilder();
        foreach (var logEntry in logs)
        {
            char typeChar = FormatLogTypeChar(logEntry.Type);
            string dateTime = logEntry.Datetime.ToString("MM/dd/yyyy HH:mm:ss");
            sb.AppendLine($"{typeChar}|{dateTime}|{logEntry.Message}");
        }
        return Encoding.UTF8.GetBytes(sb.ToString());
    }

    private LogObject? TryReadLogEntry(BinaryReader br)
    {
        return MigrationUtilities.SafeExecute<LogObject?>(() =>
        {
            if (br.BaseStream.Position + 4 > br.BaseStream.Length) return null;

            int len = br.ReadInt32();
            if (len < 0 || len > MaxLogFileSize) return null;

            long requiredBytes = len + 1 + 8;
            if (br.BaseStream.Position + requiredBytes > br.BaseStream.Length) return null;

            byte[] bytes = br.ReadBytes(len);
            if (bytes.Length != len) return null;

            string msg = Encoding.UTF8.GetString(bytes);
            var type = (LogType)br.ReadByte();
            DateTime datetime = DateTime.FromBinary(br.ReadInt64());

            return new LogObject(type, msg) { Datetime = datetime };
        }, null, "TryReadLogEntry");
    }

    private string CreateFileCopyWithTimestamp(string sourceFilePath)
    {
        if (string.IsNullOrEmpty(sourceFilePath))
            throw new ArgumentException("Source file path cannot be null or empty.", nameof(sourceFilePath));
        if (!FileSystem.Exists(sourceFilePath))
            throw new FileNotFoundException("Source file not found.", sourceFilePath);

        string directory = Path.GetDirectoryName(sourceFilePath) ?? string.Empty;
        string stem = Path.GetFileNameWithoutExtension(sourceFilePath);
        string extension = Path.GetExtension(sourceFilePath);
        // UTC stamp (trailing Z) to match other timestamps in the
        // codebase regardless of host TZ.
        string timestamp = DateTime.UtcNow.ToString("yyyyMMdd_HHmmssZ");
        string newFileName = $"{stem}_{timestamp}{extension}";
        string newFilePath = Path.Join(directory, newFileName);

        if (!FileSystem.Exists(newFilePath))
            FileSystem.CopyFile(sourceFilePath, newFilePath);

        return newFileName;
    }

    private static char FormatLogTypeChar(LogType type) => type switch
    {
        LogType.Error => 'E',
        LogType.Warning => 'W',
        LogType.Info => 'I',
        LogType.Debug => 'D',
        LogType.Verbose => 'V',
        _ => '?'
    };
}
