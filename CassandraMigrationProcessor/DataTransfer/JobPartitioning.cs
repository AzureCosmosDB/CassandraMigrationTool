using CassandraMigrationProcessor.Models;

namespace CassandraMigrationProcessor.DataTransfer;

/// <summary>
/// Per-chunk partitioning result computed up front at job init. Captures
/// every immutable piece a chunk needs: its <see cref="TableResources"/>
/// (shared by all its partitions) and the readonly list of
/// <see cref="Partition"/>s the workers will drain. The coordinator does
/// not (re)build these at runtime — it only awaits each chunk's
/// <see cref="TableResources.BulkDrainSignal"/>.
/// </summary>
internal sealed record TablePartitioning(
    TableMigration Unit,
    int ChunkIndex,
    TableResources Resources,
    IReadOnlyList<Partition> Partitions,
    bool AllRangesAlreadyComplete);

/// <summary>
/// Job-wide partitioning snapshot. Built once by
/// <see cref="MigrationJobRunner"/> after schema provisioning and
/// handed to <see cref="JobPipeline"/> at construction. All partitions
/// across all tables and chunks are flattened into
/// <see cref="AllPartitions"/> so <see cref="PartitionManager"/> can be
/// initialized in a single ctor call — there is no runtime "seed" path.
/// </summary>
internal sealed class JobPartitioning
{
    public IReadOnlyList<TablePartitioning> Chunks { get; }
    public IReadOnlyList<Partition> AllPartitions { get; }

    public JobPartitioning(IReadOnlyList<TablePartitioning> chunks)
    {
        Chunks = chunks;
        AllPartitions = chunks.SelectMany(c => c.Partitions).ToList();
    }

    /// <summary>
    /// Chunks for a given migration unit, in ChunkIndex order.
    /// </summary>
    public IReadOnlyList<TablePartitioning> ForUnit(string unitId)
        => Chunks.Where(c => c.Unit.Id == unitId).OrderBy(c => c.ChunkIndex).ToList();
}
