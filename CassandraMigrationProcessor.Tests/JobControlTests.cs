using CassandraMigrationProcessor.DataTransfer;

namespace CassandraMigrationProcessor.Tests;

public sealed class JobControlTests
{
    [Fact]
    public void CancelThenCutover_StaysCancelled()
    {
        using var control = new JobControl();

        Assert.True(control.RequestStop());
        Assert.False(control.RequestCutover());

        Assert.Equal(JobCommand.StopRequested, control.Requested);
        Assert.True(control.ShouldStop);
    }

    [Fact]
    public void CutoverThenCancel_StaysCutover()
    {
        using var control = new JobControl();

        Assert.True(control.RequestCutover());
        Assert.False(control.RequestStop());

        Assert.Equal(JobCommand.CutoverRequested, control.Requested);
    }

    [Theory]
    [InlineData(JobCommand.StopRequested)]
    [InlineData(JobCommand.CutoverRequested)]
    public void PendingPause_CanBeUpgradedToTerminalIntent(JobCommand terminal)
    {
        using var control = new JobControl();

        Assert.True(control.RequestPause());
        Assert.True(terminal == JobCommand.StopRequested
            ? control.RequestStop()
            : control.RequestCutover());

        Assert.Equal(terminal, control.Requested);
    }

    [Fact]
    public void PauseAfterStop_IsIgnored()
    {
        using var control = new JobControl();

        control.RequestStop();
        Assert.False(control.RequestPause());

        Assert.Equal(JobCommand.StopRequested, control.Requested);
    }
}
