using Newtonsoft.Json;
using CassandraMigrationProcessor.Infrastructure;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using CassandraMigrationProcessor.Models;
namespace CassandraMigrationProcessor.Persistence
{
    /// <summary>
    /// Disk-based implementation of PersistenceStorage.
    /// Stores documents as JSON files on the local file system.
    /// </summary>
    public partial class DiskPersistence : IPersistenceStorage
    {
        private static string? _storagePath;
        private static string? _appId;
        private static bool _isInitialized = false;
        private static readonly object _initLock = new object();

        private const string FILE_EXTENSION = ".json";

        /// <summary>
        /// Initializes the disk persistence layer with the provided storage path.
        /// This method is thread-safe and idempotent.
        /// </summary>
        /// <param name="connectionStringOrPath">Directory path where files will be stored</param>
        /// <exception cref="ArgumentException">Thrown when path is null or empty</exception>
        /// <exception cref="InvalidOperationException">Thrown when initialization fails</exception>
        public void Initialize(string connectionStringOrPath, string appId)
        {

            if (_isInitialized)
                return;

            lock (_initLock)
            {
                if (_isInitialized)
                    return;

                if (string.IsNullOrWhiteSpace(connectionStringOrPath))
                    throw new ArgumentException("Storage path cannot be null or empty", nameof(connectionStringOrPath));

                try
                {
                    _storagePath = connectionStringOrPath;
                    _appId = appId;
                    // Create directory if it doesn't exist (no-op for blob storage)
                    FileSystem.EnsureDirectoryExists(_storagePath);

                    _isInitialized = true;
                }
                catch (Exception ex)
                {
                    throw new InvalidOperationException($"Failed to initialize DiskPersistence. Details: {ex}", ex);
                }
            }
        }

        /// <summary>
        /// Ensures the storage is initialized before operations
        /// </summary>
        private static void EnsureInitialized()
        {
            if (!_isInitialized || string.IsNullOrEmpty(_storagePath))
                throw new InvalidOperationException("DiskPersistence is not initialized. Call Initialize() first with a valid path.");
        }

        /// <summary>
        /// Executes an action and returns a fallback value on failure, logging the error.
        /// Consolidates the repeated try/catch-log-return pattern across persistence methods.
        /// </summary>
        private T SafeExecute<T>(Func<T> action, T fallback, string operation)
        {
            try { return action(); }
            catch (Exception ex)
            {
                MigrationUtilities.LogToFile($"[DiskPersistence] {operation}: {ex.Message}", "DiskPersistence.txt");
                return fallback;
            }
        }

        /// <summary>
        /// Executes a void action, logging any error without re-throwing.
        /// </summary>
        private void SafeExecuteVoid(Action action, string operation)
        {
            try { action(); }
            catch (Exception ex)
            {
                MigrationUtilities.LogToFile($"[DiskPersistence] {operation}: {ex.Message}", "DiskPersistence.txt");
            }
        }

        /// <summary>
        /// Gets the file path for a document id.
        /// Handles hierarchical IDs like "job1\mu1.json" by creating folder structure.
        /// The ID should include the .json extension for files.
        /// </summary>
        private static string GetFilePath(string id)
        {
            // Split by backslash or forward slash to handle hierarchical structure
            var parts = id.Split('\\', '/');

            if (parts.Length == 1)
            {
                // Simple ID, just use as filename (already has .json extension)
                var sanitizedId = SanitizeFileName(parts[0]);
                return Path.Combine(_storagePath!, sanitizedId);
            }
            else
            {
                // Hierarchical ID like "job1\mu1.json"
                // Create folder structure: storagePath/job1/mu1.json
                var pathParts = new List<string> { _storagePath! };

                // Add all parts except the last as directories
                for (int i = 0; i < parts.Length - 1; i++)
                {
                    pathParts.Add(SanitizeFileName(parts[i]));
                }

                // Create the directory structure if it doesn't exist (no-op for blob storage)
                var directoryPath = Path.Combine(pathParts.ToArray());
                FileSystem.EnsureDirectoryExists(directoryPath);

                // Add the last part as the filename (already has .json extension)
                var fileName = SanitizeFileName(parts[^1]);
                pathParts.Add(fileName);

                return Path.Combine(pathParts.ToArray());
            }
        }

        /// <summary>
        /// Gets the directory path for a folder id (id without .json extension).
        /// </summary>
        private static string GetDirectoryPath(string id)
        {

            // Split by backslash or forward slash
            var parts = id.Split('\\', '/');

            var pathParts = new List<string> { _storagePath! };

            // Add all parts as directories
            foreach (var part in parts)
            {
                pathParts.Add(SanitizeFileName(part));
            }

            return Path.Combine(pathParts.ToArray());
        }

        /// <summary>
        /// Sanitizes a string to be used as a filename or folder name
        /// </summary>
        private static string SanitizeFileName(string fileName)
        {
            var invalidChars = Path.GetInvalidFileNameChars();
            return string.Join("_", fileName.Split(invalidChars, StringSplitOptions.RemoveEmptyEntries));
        }

        /// <summary>
        /// Upserts a document with the specified id.
        /// Creates a new document if it doesn't exist, updates if it does.
        /// </summary>
        /// <param name="id">Unique identifier for the document (must include .json extension, e.g., "job1\mu1.json")</param>
        /// <param name="jsonContent">JSON content to store</param>
        /// <returns>True if successful, false otherwise</returns>
        public bool Write(string id, string jsonContent)
        {
            EnsureInitialized();

            if (string.IsNullOrWhiteSpace(id))
                throw new ArgumentException("ID cannot be null or empty", nameof(id));

            if (string.IsNullOrWhiteSpace(jsonContent))
                throw new ArgumentException("JSON content cannot be null or empty", nameof(jsonContent));

            if (!id.EndsWith(FILE_EXTENSION))
                throw new ArgumentException($"ID must end with {FILE_EXTENSION} extension", nameof(id));

            return SafeExecute(() =>
            {
                var filePath = GetFilePath(id);
                return FileSystem.WriteAllText(filePath, jsonContent);
            }, false, $"Write({id})");
        }

        /// <summary>
        /// Reads a document by its id
        /// </summary>
        /// <param name="id">Unique identifier of the document (must include .json extension, e.g., "job1\mu1.json")</param>
        /// <returns>JSON content if found, null otherwise</returns>
        public string? Read(string id)
        {
            EnsureInitialized();

            if (string.IsNullOrWhiteSpace(id))
                throw new ArgumentException("ID cannot be null or empty", nameof(id));

            if (!id.EndsWith(FILE_EXTENSION))
                throw new ArgumentException($"ID must end with {FILE_EXTENSION} extension", nameof(id));

            return SafeExecute<string?>(() =>
            {
                var filePath = GetFilePath(id);
                return FileSystem.ReadAllText(filePath);
            }, null, $"Read({id})");
        }

        /// <summary>
        /// Checks if a document exists by its id
        /// </summary>
        /// <param name="id">Unique identifier of the document (must include .json extension, e.g., "job1\mu1.json")</param>
        /// <returns>True if document exists, false otherwise</returns>
        public bool Exists(string id)
        {
            EnsureInitialized();

            if (string.IsNullOrWhiteSpace(id))
                return false;

            if (!id.EndsWith(FILE_EXTENSION))
                throw new ArgumentException($"ID must end with {FILE_EXTENSION} extension", nameof(id));

            return SafeExecute(() =>
            {
                var filePath = GetFilePath(id);
                return FileSystem.Exists(filePath);
            }, false, $"Exists({id})");
        }

        /// <summary>
        /// Deletes a document or folder by its id.
        /// If id ends with .json, deletes the file.
        /// If id doesn't end with .json, deletes the entire folder.
        /// </summary>
        /// <param name="id">Unique identifier of the document/folder (e.g., "job1\mu1.json" for file, "job1" for folder)</param>
        /// <returns>True if deleted, false otherwise</returns>
        public bool Delete(string id)
        {
            EnsureInitialized();

            if (string.IsNullOrWhiteSpace(id))
                throw new ArgumentException("ID cannot be null or empty", nameof(id));

            return SafeExecute(() =>
            {
                if (id.EndsWith(FILE_EXTENSION))
                {
                    var filePath = GetFilePath(id);

                    if (!FileSystem.Exists(filePath))
                        return false;

                    FileSystem.DeleteIfExists(filePath);
                    return true;
                }
                else
                {
                    var directoryPath = GetDirectoryPath(id);
                    return FileSystem.DeleteDirectory(directoryPath, recursive: true);
                }
            }, false, $"Delete({id})");
        }

        /// <summary>
        /// Lists all document IDs in the storage.
        /// Returns IDs with .json extension included.
        /// </summary>
        /// <returns>List of document IDs (e.g., "job1\mu1.json", "settings.json")</returns>
        public List<string> ListIds()
        {
            EnsureInitialized();

            return SafeExecute(() =>
            {
                var ids = new List<string>();

                var files = FileSystem.ListFiles(_storagePath!, "*" + FILE_EXTENSION, recursive: true);

                foreach (var file in files)
                {
                    string relativePath;
                    if (FileSystem.UseBlobStorage)
                    {
                        relativePath = file;
                    }
                    else
                    {
                        relativePath = Path.GetRelativePath(_storagePath!, file);
                    }

                    var id = relativePath.Replace('/', '\\').Replace(Path.DirectorySeparatorChar, '\\');

                    ids.Add(id);
                }

                return ids;
            }, new List<string>(), "ListIds");
        }


        /// <summary>
        /// Tests the connection to the storage
        /// </summary>
        /// <returns>True if connection is successful, false otherwise</returns>
        public bool TestConnection()
        {
            if (!_isInitialized || string.IsNullOrEmpty(_storagePath))
                return false;

            return SafeExecute(() =>
            {
                if (FileSystem.UseBlobStorage)
                    return true;
                return Directory.Exists(_storagePath);
            }, false, "TestConnection");
        }

        /// <summary>
        /// Checks if the storage is initialized
        /// </summary>
        public bool IsInitialized => _isInitialized;

    }
}
