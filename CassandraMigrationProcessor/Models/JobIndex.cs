using System.Collections.Generic;

namespace CassandraMigrationProcessor.Models
{
    public class JobIndex
    {
        public List<string> MigrationJobIds { get; set; } = new();
    }
}