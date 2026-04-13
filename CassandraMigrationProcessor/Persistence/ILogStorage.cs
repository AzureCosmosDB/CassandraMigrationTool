using CassandraMigrationProcessor.Infrastructure;
using CassandraMigrationProcessor.Models;
namespace CassandraMigrationProcessor.Persistence;
/// <summary>
/// Contract for log persistence operations.
/// </summary>
public interface ILogStorage
{
    void PushLogEntry(string jobId, LogObject logObj);
    int GetLogCount(string id);
    byte[] DownloadLogsPaginated(string id, int skip, int take);
    byte[] ExportLogsAsBytes(string id, int topEntries = 20, int bottomEntries = 230);
    LogBucket ReadLogs(string id, out string fileName);
    long DeleteLogs(string jobId);
}
