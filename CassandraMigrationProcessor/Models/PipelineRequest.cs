using System.Collections.Generic;

namespace CassandraMigrationProcessor.Models
{
    public record PipelineRequest(
        MigrationUnit MigrationUnit,
        int ChunkIndex,
        double InitialPercent,
        double ContributionFactor,
        long TotalRowCount,
        TableContext Context,
        List<string> FeedRanges);
}
