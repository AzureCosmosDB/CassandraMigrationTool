using CassandraMigrationProcessor.Infrastructure;
using CassandraMigrationProcessor.Models;

namespace CassandraMigrationProcessor.Tests;

public sealed class TableMigrationMapperTests
{
    [Fact]
    public void ToSummary_PreservesCounterTableClassification()
    {
        var job = new Job { Id = "job-1" };
        var migration = new TableMigration(job, "ks", "counter_table")
        {
            IsCounterTable = true
        };

        var summary = TableMigrationMapper.ToSummary(migration);

        Assert.True(summary.IsCounterTable);
    }
}
