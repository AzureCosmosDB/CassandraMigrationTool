using System.Collections.Generic;

namespace CassandraMigrationProcessor.Models;

/// <summary>
/// Immutable request handed to <see cref="DataTransfer.JobPipeline"/>: the table
/// to copy, its progress baseline, and the feed-ranges to partition across workers.
/// </summary>
public record PipelineRequest(
    TableMigration TableMigration,
    int ChunkIndex,
    double InitialPercent,
    double ContributionFactor,
    long TotalRowCount,
    TableCopySpec Spec,
    List<string> FeedRanges);
