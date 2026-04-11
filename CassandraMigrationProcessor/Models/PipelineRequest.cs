using System.Collections.Generic;

namespace CassandraMigrationProcessor.Models
{
    public record PipelineRequest(
        TableMigration TableMigration,
        int ChunkIndex,
        double InitialPercent,
        double ContributionFactor,
        long TotalRowCount,
        TableContext Context,
        List<string> FeedRanges);
}
