using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;

namespace CassandraMigrationProcessor.Models
{
    public class JobRegistry
    {
        [JsonIgnore]
        public List<MigrationJob> MigrationJobs { get; set; } = new();

        public List<string> MigrationJobIds { get; set; } = new();

        private static readonly object _writeLock = new object();
        private static readonly object _loadLock = new object();

        public class ConnectionAccessor
        {
            private readonly Dictionary<string, string> _dict;

            public ConnectionAccessor(Dictionary<string, string> dict)
            {
                _dict = dict;
            }

            // Indexer to get/set by jobId
            public string this[string jobId]
            {
                get => _dict.TryGetValue(jobId, out var value) ? value : null;
                set => _dict[jobId] = value;
            }

            // Add this property to expose dictionary keys
            public IEnumerable<string> Keys => _dict.Keys;
        }
    }
}