using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace CassandraMigrationProcessor.Persistence
{
    /// <summary>
    /// Contract for persistence storage implementations.
    /// </summary>
    public interface IPersistenceStorage
    {
        void Initialize(string connectionStringOrPath, string appId);
        bool UpsertDocument(string id, string jsonContent);
        string? ReadDocument(string id);
        bool DocumentExists(string id);
        bool DeleteDocument(string id);
        List<string> ListDocumentIds();
        bool TestConnection();
        bool IsInitialized { get; }
        LogBucket ReadLogs(string id, out string fileName);
        byte[] DownloadLogsAsJsonBytes(
            string id, int topEntries = 20,
            int bottomEntries = 230);
        void PushLogEntry(string jobId, LogObject logObj);
        int GetLogCount(string id);
        byte[] DownloadLogsPaginated(
            string id, int skip, int take);
        long DeleteLogs(string jobId);
    }
}
