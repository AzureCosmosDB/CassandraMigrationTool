using CassandraMigrationProcessor.Models;

namespace CassandraMigrationProcessor.Infrastructure;
public class LogBucket
{
    public List<LogObject> Logs { get; set; } = new List<LogObject>();
}

/// <summary>
/// Delegate contracts for log persistence operations, so
/// MigrationLog does not need to reference the Persistence layer.
/// </summary>
public class LogStorageCallbacks
{
    public Func<string, (LogBucket bucket, string backupFile)>? ReadLogs { get; set; }
    public Action<string, LogObject>? PushLogEntry { get; set; }
    public Func<string, int, int, byte[]>? ExportLogsAsBytes { get; set; }
    public Func<string, int>? GetLogCount { get; set; }
    public Func<string, int, int, byte[]>? DownloadLogsPaginated { get; set; }
}

public class MigrationLog : IDisposable
{
    private const int MonitorMessageMinCount = 5;
    private const int MaxLogEntries = 300;
    private const int LogTrimIndex = 20;

    private LogBucket _logBucket = new LogBucket();
    private List<LogObject> _verboseMessages = new List<LogObject>();
    private string _currentId = string.Empty;
    private Job? CurrentlyActiveJob;
    private LogStorageCallbacks? _storage;

    private readonly object _verboseLock = new object();
    private readonly object _writeLock = new object();
    private readonly object _initLock = new object();

    /// <summary>
    /// Set the persistence callbacks for log I/O.
    /// </summary>
    public void SetStorage(LogStorageCallbacks? storage)
    {
        _storage = storage;
    }

    /// <summary>
    /// Set the migration job reference for MigrationLog level filtering
    /// </summary>
    public void SetJob(Job? job)
    {
        CurrentlyActiveJob = job;
    }

    public List<LogObject> GetMonitorMessages()
    {
        lock (_verboseLock)
        {
            try
            {
                if (_verboseMessages.Count == 0)
                {
                    return new List<LogObject>();
                }

                var reversedList = new List<LogObject>(_verboseMessages);
                reversedList.Reverse();
                return PadToMonitorMin(reversedList);
            }
            catch
            {
                return PadToMonitorMin(new List<LogObject>());
            }
        }
    }

    private static List<LogObject> PadToMonitorMin(List<LogObject> list)
    {
        while (list.Count < MonitorMessageMinCount)
            list.Add(new LogObject(LogType.Info, ""));
        return list;
    }

    public string Initialize(string id)
    {
        // _initLock serialises Initialize; _writeLock / _verboseLock
        // cover the same fields WriteLine touches. Lock order matches
        // WriteLine (_writeLock outer, _verboseLock inner).
        lock (_initLock)
        {
            string logBackupFile = string.Empty;
            _currentId = id;

            var freshBucket = ReadLogFile(_currentId, out logBackupFile);

            lock (_writeLock)
            {
                _logBucket = freshBucket;
                lock (_verboseLock)
                {
                    _verboseMessages.Clear();
                }
            }

            return logBackupFile;
        }
    }

    public LogBucket ReadLogFile(string id, out string logBackupFile)
    {
        var result = _storage?.ReadLogs?.Invoke(id)
            ?? (new LogBucket(), string.Empty);
        logBackupFile = result.backupFile;
        return result.bucket;
    }

    public void WriteLine(string message, LogType logType = LogType.Info)
    {
        try
        {
            // A WriteLine issued before Initialize() falls back to
            // Console for Errors and Warnings so operational signal
            // is never silently dropped.
            if (_currentId == string.Empty)
            {
                if (logType == LogType.Error || logType == LogType.Warning)
                    Console.WriteLine($"[MigrationLog uninitialized] {logType}: {message}");
                return;
            }
            // Filter based on minimum MigrationLog level - only MigrationLog if the message type is at or below the minimum level
            // Lower numeric values = more severe (Error=0, Warning=2, Info=3, Debug=4, Verbose=5)
            if (CurrentlyActiveJob != null && (int)logType > (int)CurrentlyActiveJob.LogLevel)
            {
                return; // Skip this MigrationLog entry
            }

            lock (_writeLock)
            {
                var logObj = new LogObject(logType, message);

                // Populate verbose monitor messages
                if (logType == LogType.Verbose || logType == LogType.Info || logType == LogType.Warning || logType == LogType.Error)
                {
                    lock (_verboseLock)
                    {
                        _verboseMessages.Add(logObj);
                        while (_verboseMessages.Count > MaxLogEntries)
                            _verboseMessages.RemoveAt(0);
                    }
                }

                // Add new MigrationLog
                _logBucket.Logs ??= new List<LogObject>();
                _logBucket.Logs.Add(logObj);

                // If more than MaxLogEntries logs, remove at LogTrimIndex to keep it small
                if (_logBucket.Logs.Count > MaxLogEntries && _logBucket.Logs.Count > LogTrimIndex)
                {
                    _logBucket.Logs.RemoveAt(LogTrimIndex);
                }

                // Persist to file
                _storage?.PushLogEntry?.Invoke(_currentId, logObj);
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[CRITICAL] MigrationLog write failed: {message} | Error: {ex}");
        }
    }
    public void Dispose()
    {
        _currentId = string.Empty;
        _verboseMessages.Clear();
    }

    public LogBucket GetCurrentLogBucket(string id)
    {
        if (_currentId != id)
            return new LogBucket();

        // Return a defensive snapshot. _logBucket.Logs is mutated by
        // WriteLine under _writeLock; UI enumeration on the live list
        // would race.
        lock (_writeLock)
        {
            return new LogBucket
            {
                Logs = _logBucket.Logs != null
                    ? new List<LogObject>(_logBucket.Logs)
                    : new List<LogObject>(),
            };
        }
    }

    public byte[] ExportLogsAsBytes(string id, int topEntries = 20, int bottomEntries = 230)
    {
        return _storage?.ExportLogsAsBytes?.Invoke(id, topEntries, bottomEntries)
            ?? Array.Empty<byte>();
    }

    public int GetLogCount(string id)
    {
        return _storage?.GetLogCount?.Invoke(id) ?? 0;
    }

    public byte[] DownloadLogsPaginated(string id, int skip, int take)
    {
        return _storage?.DownloadLogsPaginated?.Invoke(id, skip, take)
            ?? Array.Empty<byte>();
    }
}
