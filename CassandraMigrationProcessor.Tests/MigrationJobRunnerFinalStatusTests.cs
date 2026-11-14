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
}
