using CassandraMigrationProcessor.DataTransfer;
using CassandraMigrationProcessor.Models;

namespace CassandraMigrationProcessor.Tests;

public sealed class MigrationJobRunnerFinalStatusTests
{
    private static Job OfflineJob(params (bool copyComplete, TableStatus status)[] tables)
    {
        var job = new Job { Id = "job-1", CDCMode = CDCMode.Offline };
        int i = 0;
        foreach (var (copyComplete, status) in tables)
        {
            job.Tables.Add(new TableMigrationSummary
            {
                KeyspaceName = "ks",
                TableName = $"t{i++}",
                CopyComplete = copyComplete,
                SourceStatus = status,
            });
        }
        return job;
    }

    [Fact]
    public void FullyCopiedOfflineJob_WithNoRemainingWork_IsCompleted()
    {
        // Resume after a restart where every table had already finished
        // bulk copy: no pipeline runs, but the job must still finalise.
        var job = OfflineJob((true, TableStatus.OK), (true, TableStatus.OK));

        Assert.Equal(JobStatus.Completed,
            MigrationJobRunner.ResolveFinalStatus(job, JobCommand.None, faulted: false));
    }

    [Fact]
    public void PartiallyCopiedOfflineJob_IsPending()
    {
        var job = OfflineJob((true, TableStatus.OK), (false, TableStatus.OK));

        Assert.Equal(JobStatus.Pending,
            MigrationJobRunner.ResolveFinalStatus(job, JobCommand.None, faulted: false));
    }

    [Fact]
    public void OnlineJob_WithAllTablesCopied_IsPendingUntilCutover()
    {
        var job = OfflineJob((true, TableStatus.OK));
        job.CDCMode = CDCMode.Online;

        Assert.Equal(JobStatus.Pending,
            MigrationJobRunner.ResolveFinalStatus(job, JobCommand.None, faulted: false));
        Assert.Equal(JobStatus.Completed,
            MigrationJobRunner.ResolveFinalStatus(job, JobCommand.CutoverRequested, faulted: false));
    }

    [Fact]
    public void FailedTable_OutranksCompletion()
    {
        var job = OfflineJob((true, TableStatus.OK), (false, TableStatus.Failed));

        Assert.Equal(JobStatus.Faulted,
            MigrationJobRunner.ResolveFinalStatus(job, JobCommand.None, faulted: false));
    }

    [Fact]
    public void ZeroUnits_FullyCopiedOfflineJob_IsNotAFault()
    {
        var job = OfflineJob((true, TableStatus.OK), (true, TableStatus.OK));

        Assert.Null(MigrationJobRunner.ResolveZeroUnitFault(job));
    }

    [Fact]
    public void ZeroUnits_TablesThatDoNotExist_IsAFault()
    {
        var job = OfflineJob((false, TableStatus.NotFound));

        Assert.Contains("did not match any source tables", MigrationJobRunner.ResolveZeroUnitFault(job));
    }

    [Theory]
    [InlineData(CDCMode.Offline, false)]
    [InlineData(CDCMode.Online, true)]
    public void ZeroUnits_TableWithRemainingWorkButNoSavedUnit_IsAFault(CDCMode mode, bool copyComplete)
    {
        // Units are loaded per eligible table; reaching zero units while a
        // table still has work means its saved unit could not be loaded.
        var job = OfflineJob((true, TableStatus.OK), (copyComplete, TableStatus.OK));
        job.CDCMode = mode;

        Assert.Contains("ks.t1", MigrationJobRunner.ResolveZeroUnitFault(job));
    }
}
