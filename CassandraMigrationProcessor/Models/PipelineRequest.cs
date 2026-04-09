using System.Collections.Generic;

namespace CassandraMigrationProcessor.Models
{
    public record PipelineRequest(
        MigrationUnit MigrationUnit,
        int ChunkIndex,
        double InitialPercent,
        double ContributionFactor,
        long TotalRowCount,
        ProcessorContext Context,
        List<string> FeedRanges);
}
