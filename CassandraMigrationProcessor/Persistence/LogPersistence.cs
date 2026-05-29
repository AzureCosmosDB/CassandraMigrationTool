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
    private readonly string _storagePath;
    private static readonly object _readLock = new object();

    public LogPersistence(string storagePath)
    {
        if (string.IsNullOrWhiteSpace(storagePath))
            throw new ArgumentException("Storage path cannot be null or empty", nameof(storagePath));
        _storagePath = storagePath;
    }

    public void PushLogEntry(string jobId, LogObject logObject)
    {
        if (string.IsNullOrWhiteSpace(jobId))
            throw new ArgumentException("ID cannot be null or empty", nameof(jobId));

        if (logObject == null)
            throw new ArgumentNullException(nameof(logObject));

        MigrationUtilities.SafeExecuteVoid(() =>
        {
            var folder = Path.Combine(_storagePath, "migrationlogs");
            var binPath = Path.Combine(folder, $"{jobId}.bin");

            FileSystem.EnsureDirectoryExists(folder);

            using var fs = FileSystem.OpenAppend(binPath);
            using var bw = new BinaryWriter(fs);

            var messageBytes = Encoding.UTF8.GetBytes(logObject.Message);
            bw.Write(messageBytes.Length);
            bw.Write(messageBytes);
            bw.Write((byte)logObject.Type);
            bw.Write(logObject.Datetime.ToBinary());
        }, $"PushLogEntry({jobId})");
    }

    public int GetLogCount(string id)
    {
        var folder = Path.Combine(_storagePath, "migrationlogs");
        var binPath = Path.Combine(folder, $"{id}.bin");

        if (!FileSystem.Exists(binPath))
            return 0;

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
        var folder = Path.Combine(_storagePath, "migrationlogs");
        var binPath = Path.Combine(folder, $"{id}.bin");
        var logBucket = new LogBucket { Logs = new List<LogObject>() };

        if (!FileSystem.Exists(binPath))
            return Array.Empty<byte>();

        MigrationUtilities.SafeExecuteVoid(() =>
        {
            using var fs = FileSystem.OpenReadShared(binPath);
            if (fs == null) return;
            using var br = new BinaryReader(fs);

            var offsets = CollectEntryOffsets(br);
            var selectedOffsets = offsets.Skip(skip).Take(take).ToList();
            ReadEntriesAtOffsets(fs, br, selectedOffsets, logBucket.Logs);
        }, $"DownloadLogsPaginated({id})");

        return EncodeLogsAsBytes(logBucket.Logs);
    }

    public byte[] ExportLogsAsBytes(string id, int topEntries = 20, int bottomEntries = 230)
    {
        var folder = Path.Combine(_storagePath, "migrationlogs");
        var binPath = Path.Combine(folder, $"{id}.bin");
        var logs = ParseLogBinFile(binPath, topEntries, bottomEntries);
        return EncodeLogsAsBytes(logs.Logs);
    }

    public LogBucket ReadLogs(string id, out string fileName)
    {
        fileName = id;

        try
        {
            lock (_readLock)
            {
                var folder = Path.Combine(_storagePath, "migrationlogs");
                var binPath = Path.Combine(folder, $"{id}.bin");

                if (FileSystem.Exists(binPath))
                {
                    var logBucket = ParseLogBinFile(binPath);
                    if (logBucket.Logs == null || logBucket.Logs.Count == 0)
                    {
                        return HandleError(id, binPath, binPath, out fileName);
                    }
                    return logBucket;
                }

                return new LogBucket();
            }
        }
        catch (Exception ex)
        {
            throw new InvalidOperationException("MigrationLog Init failed", ex);
        }
    }

    /// <summary>
    /// Deletes all MigrationLog entries for a given JobId by deleting the binary MigrationLog file
    /// </summary>
    public long DeleteLogs(string jobId)
    {
        if (string.IsNullOrWhiteSpace(jobId))
            throw new ArgumentException("Job ID cannot be null or empty", nameof(jobId));

        return MigrationUtilities.SafeExecute(() =>
        {
            var folder = Path.Combine(_storagePath, "migrationlogs");
            var binPath = Path.Combine(folder, $"{jobId}.bin");

            if (FileSystem.Exists(binPath))
            {
                FileSystem.DeleteIfExists(binPath);
                return 1L;
            }
            else
            {
                return 0L;
            }
        }, -1L, $"DeleteLogs({jobId})");
    }

    private LogBucket HandleError(string jobId, string binPath, string currentLogFilePath, out string backupFileName)
    {
        backupFileName = CreateFileCopyWithTimestamp(currentLogFilePath);

        FileSystem.DeleteIfExists(currentLogFilePath);

        var logBucket = new LogBucket();
        logBucket.Logs ??= new List<LogObject>();
        logBucket.Logs.Add(new LogObject(LogType.Error, $"Unable to load the MigrationLog file; original file backed up as {backupFileName}"));
        WriteBinaryLog(jobId, logBucket.Logs);
        return ParseLogBinFile(binPath);
    }

    private void WriteBinaryLog(string id, List<LogObject> logs)
    {
        if (logs == null || logs.Count == 0)
            return;

        var folder = Path.Combine(_storagePath, "migrationlogs");
        var binPath = Path.Combine(folder, $"{id}.bin");

        try
        {
            FileSystem.EnsureDirectoryExists(folder);

            using var fs = FileSystem.OpenAppend(binPath);
            using var bw = new BinaryWriter(fs);

            foreach (var logEntry in logs)
            {
                try
                {
                    var message = logEntry.Message ?? string.Empty;
                    var messageBytes = Encoding.UTF8.GetBytes(message);

                    bw.Write(messageBytes.Length);
                    bw.Write(messageBytes);
                    bw.Write((byte)logEntry.Type);
                    bw.Write(logEntry.Datetime.ToBinary());
                }
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

    private LogBucket ParseLogBinFile(string binPath, int topCount = 20, int bottomCount = 280)
    {
        var logBucket = new LogBucket { Logs = new List<LogObject>() };

        if (!FileSystem.Exists(binPath))
            return logBucket;

        MigrationUtilities.SafeExecuteVoid(() =>
        {
            using var fs = FileSystem.OpenReadShared(binPath);
            if (fs == null) return;
            using var br = new BinaryReader(fs);

            var offsets = CollectEntryOffsets(br);

            // Select top N and bottom M
            List<long> selectedOffsets;
            if (offsets.Count > topCount + bottomCount)
            {
                if (topCount > 0 && bottomCount > 0)
                {
                    selectedOffsets = offsets
                        .Take(topCount)
                        .Concat(offsets.Skip(Math.Max(0, offsets.Count - bottomCount)))
                        .Distinct()
                        .OrderBy(i => i)
                        .ToList();
                }
                else if (topCount > 0)
                {
                    selectedOffsets = offsets.Take(topCount).ToList();
                }
                else if (bottomCount > 0)
                {
                    selectedOffsets = offsets.Skip(Math.Max(0, offsets.Count - bottomCount)).ToList();
                }
                else
                {
                    // Caller asked for 0+0 — honour it and return nothing,
                    // rather than reading the whole log into memory (which
                    // for multi-hundred-MB binary logs would be the very
                    // outcome the cap exists to prevent).
                    selectedOffsets = new List<long>();
                }
            }
            else
            {
                selectedOffsets = offsets;
            }

            ReadEntriesAtOffsets(fs, br, selectedOffsets, logBucket.Logs);
        }, "ParseLogBinFile");

        return logBucket;
    }

    /// <summary>
    /// Walks the binary log starting at the reader's current position,
    /// returning the file offset of each well-formed entry. Stops at the
    /// first malformed record (length out of range, truncated payload,
    /// or read error) — callers treat truncation as soft EOF.
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
                if (fs.Position + 4 > fs.Length)
                    break;

                int msgLen = br.ReadInt32();
                if (msgLen < 0 || msgLen > MaxLogFileSize)
                    break;

                long bytesToSkip = msgLen + 1 + 8;
                if (fs.Position + bytesToSkip > fs.Length)
                    break;

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
    /// Seeks to each offset in turn, decodes the entry there via
    /// <see cref="TryReadLogEntry"/>, and appends successfully decoded
    /// entries to <paramref name="destination"/>. Skips offsets that
    /// fail to decode.
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
    /// Renders the decoded log entries as <c>typeChar|MM/dd/yyyy HH:mm:ss|message</c>
    /// lines and returns the UTF-8 bytes. Shared by the paginated download
    /// and the top/bottom export APIs.
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
            if (br.BaseStream.Position + 4 > br.BaseStream.Length)
                return null;

            int len = br.ReadInt32();

            if (len < 0 || len > MaxLogFileSize)
                return null;

            long requiredBytes = len + 1 + 8;
            if (br.BaseStream.Position + requiredBytes > br.BaseStream.Length)
                return null;

            byte[] bytes = br.ReadBytes(len);
            if (bytes.Length != len)
                return null;

            string msg = Encoding.UTF8.GetString(bytes);
            byte typeByte = br.ReadByte();
            var type = (LogType)typeByte;
            long dateBinary = br.ReadInt64();
            DateTime datetime = DateTime.FromBinary(dateBinary);

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
        string fileNameWithoutExtension = Path.GetFileNameWithoutExtension(sourceFilePath);
        string extension = Path.GetExtension(sourceFilePath);
        // UTC stamp (trailing Z) to match the rest of the codebase
        // (MigrationLog timestamps, BulkCopyStartedOn, change-feed start
        // tokens) — otherwise on hosts configured to local time the
        // backup filename is misaligned with the in-file UTC timestamps.
        string timestamp = DateTime.UtcNow.ToString("yyyyMMdd_HHmmssZ");
        string newFileName = $"{fileNameWithoutExtension}_{timestamp}{extension}";
        string newFilePath = Path.Combine(directory, newFileName);

        if (!FileSystem.Exists(newFilePath))
        {
            FileSystem.CopyFile(sourceFilePath, newFilePath);
        }

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
