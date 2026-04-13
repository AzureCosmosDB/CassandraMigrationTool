using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using CassandraMigrationProcessor.Infrastructure;
using CassandraMigrationProcessor.Models;
namespace CassandraMigrationProcessor.Persistence;
/// <summary>
/// Contract for persistence storage implementations.
/// </summary>
public interface IPersistenceStorage
{
    void Initialize(string connectionStringOrPath);
    bool Write(string id, string jsonContent);
    string? Read(string id);
    bool Exists(string id);
    bool Delete(string id);
    List<string> ListIds();
    LogBucket ReadLogs(string id, out string fileName);
    byte[] ExportLogsAsBytes(
        string id, int topEntries = 20,
        int bottomEntries = 230);
    void PushLogEntry(string jobId, LogObject logObj);
    int GetLogCount(string id);
    byte[] DownloadLogsPaginated(
        string id, int skip, int take);
    long DeleteLogs(string jobId);
}
