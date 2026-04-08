using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace CassandraMigrationProcessor.Persistence
{
    /// <summary>
    /// Abstract base class for persistence storage implementations.
    /// Provides a common interface for storing and retrieving documents and log entries.
    /// </summary>
    public abstract class PersistenceStorage
    {
        /// <summary>
        /// Initializes the persistence storage with the provided connection string or path.
        /// This method should be thread-safe and idempotent.
        /// </summary>
        /// <param name="connectionStringOrPath">Connection string or file path for the storage</param>
        public abstract void Initialize(string connectionStringOrPath, string AppId);

        /// <summary>
        /// Synchronous version of UpsertDocumentAsync
        /// </summary>
        /// <param name="id">Unique identifier for the document</param>
        /// <param name="jsonContent">JSON content to store</param>
        /// <returns>True if successful, false otherwise</returns>
        public abstract bool UpsertDocument(string id, string jsonContent);

        /// <summary>
        /// Synchronous version of ReadDocumentAsync
        /// </summary>
        /// <param name="id">Unique identifier of the document</param>
        /// <returns>JSON content if found, null otherwise</returns>
        public abstract string? ReadDocument(string id);

        /// <summary>
        /// Synchronous version of DocumentExistsAsync
        /// </summary>
        public abstract bool DocumentExists(string id);

        /// <summary>
        /// Synchronous version of DeleteDocumentAsync
        /// </summary>
        public abstract bool DeleteDocument(string id);

        /// <summary>
        /// Lists all document IDs in the storage
        /// </summary>
        /// <returns>List of document IDs</returns>
        public abstract List<string> ListDocumentIds();


        /// <summary>
        /// Tests the connection to the storage
        /// </summary>
        /// <returns>True if connection is successful, false otherwise</returns>
        public abstract bool TestConnection();

        /// <summary>
        /// Checks if the storage is initialized
        /// </summary>
        public abstract bool IsInitialized { get; }

        public abstract LogBucket ReadLogs(string id, out string fileName);

        public abstract byte[] DownloadLogsAsJsonBytes(string id, int topEntries = 20, int bottomEntries = 230);

        public abstract void PushLogEntry(string JobId,LogObject logObj);

        /// <summary>
        /// Gets the total count of log entries for a given job ID
        /// </summary>
        /// <param name="id">Job ID to count logs for</param>
        /// <returns>Total number of log entries</returns>
        public abstract int GetLogCount(string id);

        /// <summary>
        /// Downloads a specific page of logs using skip/take pagination
        /// </summary>
        /// <param name="id">Job ID to download logs for</param>
        /// <param name="skip">Number of log entries to skip</param>
        /// <param name="take">Number of log entries to take</param>
        /// <returns>Byte array containing formatted logs</returns>
        public abstract byte[] DownloadLogsPaginated(string id, int skip, int take);

        /// <summary>
        /// Deletes all log entries for a given JobId
        /// </summary>
        /// <param name="jobId">Job ID to delete logs for</param>
        /// <returns>Number of log entries deleted, or -1 if error occurred</returns>
        public abstract long DeleteLogs(string jobId);
    }
}
