using CassandraMigrationProcessor.Helpers;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using CassandraMigrationProcessor.Models;

namespace CassandraMigrationProcessor.Persistence
{
    /// <summary>
    /// Binary log storage operations: push, read, paginate,
    /// export, and delete migration log entries.
    /// </summary>
    public partial class DiskPersistence
    {
        private static readonly object _readLock = new object();

        public void PushLogEntry(string jobId, LogObject logObject)
        {
            EnsureInitialized();

            if (string.IsNullOrWhiteSpace(jobId))
                throw new ArgumentException("ID cannot be null or empty", nameof(jobId));

            if (logObject == null)
                throw new ArgumentNullException(nameof(logObject));

            SafeExecuteVoid(() =>
            {
                var folder = Path.Combine(_storagePath, "migrationlogs");
                var binPath = Path.Combine(folder, $"{jobId}.bin");

                StorageStreamFactory.EnsureDirectoryExists(folder);

                using var fs = StorageStreamFactory.OpenAppend(binPath);
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

            if (!StorageStreamFactory.Exists(binPath))
                return 0;

            return SafeExecute(() =>
            {
                int count = 0;
                using var fs = StorageStreamFactory.OpenReadShared(binPath);
                if (fs == null) return 0;
                using var br = new BinaryReader(fs);

                while (fs.Position < fs.Length)
                {
                    try
                    {
                        if (br.BaseStream.Position + 4 > br.BaseStream.Length)
                            break;

                        int msgLen = br.ReadInt32();
                        if (msgLen < 0 || msgLen > 1_000_000)
                            break;

                        long bytesToSkip = msgLen + 1 + 8;
                        if (br.BaseStream.Position + bytesToSkip > br.BaseStream.Length)
                            break;

                        br.BaseStream.Seek(bytesToSkip, SeekOrigin.Current);
                        count++;
                    }
                    catch (Exception)
                    {
                        break;
                    }
                }

                return count;
            }, 0, $"GetLogCount({id})");
        }

        public byte[] DownloadLogsPaginated(string id, int skip, int take)
        {
            var folder = Path.Combine(_storagePath, "migrationlogs");
            var binPath = Path.Combine(folder, $"{id}.bin");
            var logBucket = new LogBucket { Logs = new List<LogObject>() };
            var offsets = new List<long>();

            if (!StorageStreamFactory.Exists(binPath))
                return Array.Empty<byte>();

            SafeExecuteVoid(() =>
            {
                using var fs = StorageStreamFactory.OpenReadShared(binPath);
                if (fs == null) return;
                using var br = new BinaryReader(fs);

                // First pass: collect all offsets
                while (fs.Position < fs.Length)
                {
                    long offset = fs.Position;
                    try
                    {
                        if (br.BaseStream.Position + 4 > br.BaseStream.Length)
                            break;

                        int msgLen = br.ReadInt32();
                        if (msgLen < 0 || msgLen > 1_000_000)
                            break;

                        long bytesToSkip = msgLen + 1 + 8;
                        if (br.BaseStream.Position + bytesToSkip > br.BaseStream.Length)
                            break;

                        br.BaseStream.Seek(bytesToSkip, SeekOrigin.Current);
                        offsets.Add(offset);
                    }
                    catch (Exception)
                    {
                        break;
                    }
                }

                // Apply skip/take pagination
                var selectedOffsets = offsets.Skip(skip).Take(take).ToList();

                // Second pass: read selected entries
                foreach (var offset in selectedOffsets)
                {
                    fs.Position = offset;
                    var MigrationLog = TryReadLogEntry(br);
                    if (MigrationLog != null)
                        logBucket.Logs!.Add(MigrationLog);
                }
            }, $"DownloadLogsPaginated({id})");

            // Format logs
            var sb = new StringBuilder();
            foreach (var MigrationLog in logBucket.Logs)
            {
                char typeChar = FormatLogTypeChar(MigrationLog.Type);
                string dateTime = MigrationLog.Datetime.ToString("MM/dd/yyyy HH:mm:ss");
                sb.AppendLine($"{typeChar}|{dateTime}|{MigrationLog.Message}");
            }

            return Encoding.UTF8.GetBytes(sb.ToString());
        }

        public byte[] DownloadLogsAsJsonBytes(string id, int topEntries = 20, int bottomEntries = 230)
        {
            var folder = Path.Combine(_storagePath, "migrationlogs");
            var binPath = Path.Combine(folder, $"{id}.bin");
            var logs = ParseLogBinFile(binPath, topEntries, bottomEntries);

            var sb = new StringBuilder();
            foreach (var MigrationLog in logs.Logs)
            {
                char typeChar = FormatLogTypeChar(MigrationLog.Type);
                string dateTime = MigrationLog.Datetime.ToString("MM/dd/yyyy HH:mm:ss");
                sb.AppendLine($"{typeChar}|{dateTime}|{MigrationLog.Message}");
            }

            return Encoding.UTF8.GetBytes(sb.ToString());
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

                    if (StorageStreamFactory.Exists(binPath))
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
            EnsureInitialized();

            if (string.IsNullOrWhiteSpace(jobId))
                throw new ArgumentException("Job ID cannot be null or empty", nameof(jobId));

            return SafeExecute(() =>
            {
                var folder = Path.Combine(_storagePath!, "migrationlogs");
                var binPath = Path.Combine(folder, $"{jobId}.bin");

                if (StorageStreamFactory.Exists(binPath))
                {
                    StorageStreamFactory.DeleteIfExists(binPath);
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

            StorageStreamFactory.DeleteIfExists(currentLogFilePath);

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
                StorageStreamFactory.EnsureDirectoryExists(folder);

                using var fs = StorageStreamFactory.OpenAppend(binPath);
                using var bw = new BinaryWriter(fs);

                foreach (var MigrationLog in logs)
                {
                    try
                    {
                        var message = MigrationLog.Message ?? string.Empty;
                        var messageBytes = Encoding.UTF8.GetBytes(message);

                        bw.Write(messageBytes.Length);
                        bw.Write(messageBytes);
                        bw.Write((byte)MigrationLog.Type);
                        bw.Write(MigrationLog.Datetime.ToBinary());
                    }
                    catch (Exception)
                    {
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
            var offsets = new List<long>();

            if (!StorageStreamFactory.Exists(binPath))
                return logBucket;

            SafeExecuteVoid(() =>
            {
                using var fs = StorageStreamFactory.OpenReadShared(binPath);
                if (fs == null) return;
                using var br = new BinaryReader(fs);

                // First pass: collect offsets of valid MigrationLog entries
                while (fs.Position < fs.Length)
                {
                    long offset = fs.Position;

                    try
                    {
                        if (br.BaseStream.Position + 4 > br.BaseStream.Length)
                            break;

                        int msgLen = br.ReadInt32();

                        if (msgLen < 0 || msgLen > 1_000_000)
                            break;

                        long bytesToSkip = msgLen + 1 + 8;
                        if (br.BaseStream.Position + bytesToSkip > br.BaseStream.Length)
                            break;

                        br.BaseStream.Seek(bytesToSkip, SeekOrigin.Current);
                        offsets.Add(offset);
                    }
                    catch (Exception)
                    {
                        break;
                    }
                }

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
                    else
                    {
                        selectedOffsets = offsets
                            .Distinct()
                            .OrderBy(i => i)
                            .ToList();
                    }
                }
                else
                {
                    selectedOffsets = offsets;
                }

                // Second pass: read selected entries
                foreach (var offset in selectedOffsets)
                {
                    fs.Position = offset;
                    var MigrationLog = TryReadLogEntry(br);
                    if (MigrationLog != null)
                        logBucket.Logs!.Add(MigrationLog);
                }
            }, "ParseLogBinFile");

            return logBucket;
        }

        private LogObject? TryReadLogEntry(BinaryReader br)
        {
            const int MaxReasonableLength = 1_000_000;

            return SafeExecute<LogObject?>(() =>
            {
                if (br.BaseStream.Position + 4 > br.BaseStream.Length)
                    return null;

                int len = br.ReadInt32();

                if (len < 0 || len > MaxReasonableLength)
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

            if (!StorageStreamFactory.Exists(sourceFilePath))
                throw new FileNotFoundException("Source file not found.", sourceFilePath);

            string directory = Path.GetDirectoryName(sourceFilePath) ?? string.Empty;
            string fileNameWithoutExtension = Path.GetFileNameWithoutExtension(sourceFilePath);
            string extension = Path.GetExtension(sourceFilePath);
            string timestamp = DateTime.Now.ToString("yyyyMMdd_HHmmss");
            string newFileName = $"{fileNameWithoutExtension}_{timestamp}{extension}";
            string newFilePath = Path.Combine(directory, newFileName);

            if (!StorageStreamFactory.Exists(newFilePath))
            {
                StorageStreamFactory.CopyFile(sourceFilePath, newFilePath);
            }

            return newFileName;
        }

#pragma warning disable CS0618 // LogType.Message is obsolete
        private static char FormatLogTypeChar(LogType type) => type switch
        {
            LogType.Error => 'E',
            LogType.Warning => 'W',
            LogType.Info => 'I',
            LogType.Message => 'L',
            LogType.Debug => 'D',
            LogType.Verbose => 'V',
            _ => '?'
        };
#pragma warning restore CS0618
    }
}
