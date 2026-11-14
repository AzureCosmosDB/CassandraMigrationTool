using CassandraMigrationProcessor.DataTransfer;
using CassandraMigrationProcessor.Infrastructure;

namespace CassandraMigrationProcessor.Tests;

public sealed class RowWriteRetryTests
{
    private static readonly RetryPolicy Policy = RetryPolicy.Linear(3, TimeSpan.Zero);

    private static WorkerLog NewLog() => new(new MigrationLog(), 0);

    [Fact]
    public async Task NonJobCancellation_IsRetried_AndSucceeds()
    {
        int calls = 0;
        var counters = new WriteCounters();

        var outcome = await RowWriteRetry.ExecuteAsync(
            () => ++calls < 3 ? throw new TaskCanceledException("driver timeout") : Task.CompletedTask,
            Policy, NewLog(), "row", counters, CancellationToken.None);

        Assert.Equal(WriteOutcome.Success, outcome);
        Assert.Equal(3, calls);
        Assert.Equal(1, counters.Done);
    }

    [Fact]
    public async Task NonJobCancellation_ExhaustsRetries_ReturnsFailedWithoutThrowing()
    {
        int calls = 0;
        var counters = new WriteCounters();

        var outcome = await RowWriteRetry.ExecuteAsync(
            () => { calls++; throw new TaskCanceledException("driver timeout"); },
            Policy, NewLog(), "row", counters, CancellationToken.None);

        Assert.Equal(WriteOutcome.Failed, outcome);
        Assert.Equal(3, calls);
        Assert.Equal(1, counters.Failed);
        Assert.IsType<TaskCanceledException>(counters.LastException);
    }

    [Fact]
    public async Task JobCancellation_Propagates()
    {
        using var cts = new CancellationTokenSource();
        int calls = 0;

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => RowWriteRetry.ExecuteAsync(
            () => { calls++; cts.Cancel(); throw new OperationCanceledException(cts.Token); },
            Policy, NewLog(), "row", new WriteCounters(), cts.Token));

        Assert.Equal(1, calls);
    }
}
