using CassandraMigrationProcessor.CassandraDriver;

namespace CassandraMigrationProcessor.DataTransfer;

/// <summary>
/// No-op write strategy used when the target session is a
/// <see cref="NullSession"/> (simulated run). Counts every row as
/// successfully written so progress / checkpoint advance the same
/// way they would against a real target, but does not bind or
/// execute anything against the target driver.
/// </summary>
internal sealed class SimulatedRowWriteStrategy : IRowWriteStrategy
{
    public Task<WriteOutcome> WriteRowAsync(
        object[] sourceRow,
        WriteCounters counters,
        CdcRowMetadata? metadata,
        CancellationToken cancellationToken)
    {
        if (cancellationToken.IsCancellationRequested)
            return Task.FromResult(WriteOutcome.Failed);

        Interlocked.Increment(ref counters.Done);
        return Task.FromResult(WriteOutcome.Success);
    }

    public Task<WriteOutcome> WriteJsonRowAsync(
        string cleanedJson,
        WriteCounters counters,
        CdcRowMetadata? metadata,
        CancellationToken cancellationToken)
    {
        if (cancellationToken.IsCancellationRequested)
            return Task.FromResult(WriteOutcome.Failed);

        Interlocked.Increment(ref counters.Done);
        return Task.FromResult(WriteOutcome.Success);
    }
}
