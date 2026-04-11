using System.Collections.Generic;

namespace CassandraMigrationProcessor.Models
{
    public class JobRegistry
    {
        public List<string> MigrationJobIds { get; set; } = new();

        private static readonly object _writeLock = new object();
        private static readonly object _loadLock = new object();
    }
}