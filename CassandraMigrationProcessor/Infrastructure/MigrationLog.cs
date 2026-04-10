using CassandraMigrationProcessor.Context;
using CassandraMigrationProcessor.Models;
using System;
using System.Collections.Generic;

namespace CassandraMigrationProcessor.Infrastructure
{
    public class LogBucket
    {
        public List<LogObject>? Logs { get; set; } = new List<LogObject>();
    }

    public class MigrationLog : IDisposable
    {
        private const int MonitorMessageMinCount = 5;
        private const int MaxLogEntries = 300;
        private const int LogTrimIndex = 20;

        private LogBucket _logBucket = new LogBucket();
        private List<LogObject> _verboseMessages = new List<LogObject>();
        private string _currentId = string.Empty;
        private MigrationJob? CurrentlyActiveJob;

        private readonly object _verboseLock = new object();
        private readonly object _writeLock = new object();
        private readonly object _initLock = new object();

        public bool IsInitialized { get; set; } = false;

        /// <summary>
        /// Set the migration job reference for MigrationLog level filtering
        /// </summary>
        public void SetJob(MigrationJob? job)
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

                    while (reversedList.Count < MonitorMessageMinCount)
                    {
                        reversedList.Add(new LogObject(LogType.Info, ""));
                    }
                    return reversedList;
                }
                catch
                {
                    var blankList = new List<LogObject>();
                    for (int i = 0; i < MonitorMessageMinCount; i++)
                    {
                        blankList.Add(new LogObject(LogType.Info, ""));
                    }
                    return blankList;
                }
            }
        }

        public string Init(string id)
        {
            lock (_initLock)
            {
                string logBackupFile = string.Empty;
                _currentId = id;

                _logBucket = ReadLogFile(_currentId, out logBackupFile);
                _verboseMessages.Clear();

                IsInitialized = true;
                return logBackupFile;
            }
        }

        public LogBucket ReadLogFile(string id, out string logBackupFile)
        {
            return MigrationJobContext.Store.ReadLogs(id, out logBackupFile);
        }

        public void WriteLine(string message, LogType logType = LogType.Info)
        {
            try
            {
                // Filter based on minimum MigrationLog level - only MigrationLog if the message type is at or below the minimum level
                // Lower numeric values = more severe (Error=0, Message=1, Warning=2, Info=3, Debug=4, Verbose=5)
                if (_currentId == string.Empty || (CurrentlyActiveJob != null && (int)logType > (int)CurrentlyActiveJob.LogLevel))
                {
                    return; // Skip this MigrationLog entry
                }

                lock (_writeLock)
                {
                    if (_logBucket == null)
                    {
                        string logBackupFile = string.Empty;
                        _logBucket = ReadLogFile(_currentId, out logBackupFile);
                    }

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
                    MigrationJobContext.Store.PushLogEntry(_currentId, logObj);
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
            if (_currentId == id && _logBucket != null)
            {
                return _logBucket;
            }
            return new LogBucket();
        }

        public byte[] ExportLogsAsBytes(string id, int topEntries = 20, int bottomEntries = 230)
        {
            return MigrationJobContext.Store.ExportLogsAsBytes(id, topEntries, bottomEntries);
        }

        public int GetLogCount(string id)
        {
            return MigrationJobContext.Store.GetLogCount(id);
        }

        public byte[] DownloadLogsPaginated(string id, int skip, int take)
        {
            return MigrationJobContext.Store.DownloadLogsPaginated(id, skip, take);
        }
    }
}
