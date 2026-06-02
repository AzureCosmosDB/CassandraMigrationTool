using CassandraMigrationProcessor.CassandraDriver;

namespace CassandraMigrationProcessor.DataTransfer;

/// <summary>
/// Shared (job-wide) state passed to every worker. Holds the
/// <see cref="DataTransfer.PartitionManager"/> that all tables seed into and
/// every worker pulls from (and which owns the cooldown scheduler for
/// delayed recycles), the connection capability used by readers and
/// writers, reader / writer tunables, the replay configuration knobs, and
/// the unified <see cref="JobControl"/> (cancellation + first-fault).
/// Per-table state is resolved through <see cref="Partition"/>
/// pass-through accessors.
/// </summary>
internal record PipelineContext(
    PartitionManager Partitions,
    ISessionFactory SessionFactory,
    ReaderConfig ReaderConfig,
    WriterConfig WriterConfig,
    bool EnableReplay,
    JobControl Control);
