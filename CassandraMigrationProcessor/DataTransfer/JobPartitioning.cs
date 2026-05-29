using CassandraMigrationProcessor.Models;

namespace CassandraMigrationProcessor.DataTransfer;

/// <summary>
/// Per-table partitioning result computed up front at job init. Captures
/// every immutable piece a table needs: its <see cref="TableResources"/>
/// (shared by all its partitions) and the readonly list of
/// <see cref="Partition"/>s the workers will drain. The coordinator does
/// not (re)build these at runtime — it only awaits each table's
/// <see cref="TableResources.BulkDrainSignal"/>.
/// </summary>
internal sealed record TablePartitioning(
    TableMigration Unit,
    TableResources Resources,
    IReadOnlyList<Partition> Partitions,
    bool AllRangesAlreadyComplete);

/// <summary>
/// Job-wide partitioning snapshot. Built once by
/// <see cref="MigrationJobRunner"/> after schema provisioning and
/// handed to <see cref="JobPipeline"/> at construction. All partitions
/// across all tables are flattened into <see cref="AllPartitions"/> so
/// <see cref="PartitionManager"/> can be initialized in a single ctor
/// call — there is no runtime "seed" path.
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
    /// Partitioning entries for a given migration unit. In current
    /// code each unit produces exactly one entry; the list shape is
    /// kept so callers can iterate without caring about cardinality.
    /// </summary>
    public IReadOnlyList<TablePartitioning> ForUnit(string unitId)
        => Chunks.Where(c => c.Unit.Id == unitId).ToList();
}
