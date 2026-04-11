using System.Collections.Generic;

namespace CassandraMigrationProcessor.Models
{
    public class JobRegistry
    {
        public List<string> MigrationJobIds { get; set; } = new();
    }
}