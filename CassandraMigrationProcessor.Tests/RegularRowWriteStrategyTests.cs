using System.Text.Json;
using CassandraMigrationProcessor.CassandraDriver;
using CassandraMigrationProcessor.DataTransfer;

namespace CassandraMigrationProcessor.Tests;

public sealed class RegularRowWriteStrategyTests
{
    private static readonly CassandraColumn[] PrimaryKeyColumns =
    [
        new("tenant_id", "text", "partition_key", "none", 0),
        new("cat_id", "text", "clustering", "asc", 0)
    ];

    private static readonly HashSet<string> PrimaryKeyColumnNames =
        new(PrimaryKeyColumns.Select(c => c.Name), StringComparer.Ordinal);

    [Fact]
    public void BuildSplitWritePlan_KeepsRowMetadataColumnsInBaselineInsert()
    {
        const string json =
            """
            {
              "cat_id": "cat-1",
              "favorite_toy": "Ball",
              "tenant_id": "tenant-1",
              "name": "Milo",
              "vaccinated": null
            }
            """;
        var metadata = new CdcRowMetadata(
            WritetimeMicros: 100,
            ExpiryEpochSeconds: null,
            PerColumn: new Dictionary<string, CdcCellMetadata>
            {
                ["favorite_toy"] = new(200, 300)
            });

        var plan = RegularRowWriteStrategy.BuildSplitWritePlan(
            json, metadata, PrimaryKeyColumns, PrimaryKeyColumnNames);

        using var baseline = JsonDocument.Parse(plan.BaselineJson);
        var root = baseline.RootElement;
        Assert.Equal("tenant-1", root.GetProperty("tenant_id").GetString());
        Assert.Equal("cat-1", root.GetProperty("cat_id").GetString());
        Assert.Equal("Milo", root.GetProperty("name").GetString());
        Assert.Equal(JsonValueKind.Null, root.GetProperty("vaccinated").ValueKind);
        Assert.False(root.TryGetProperty("favorite_toy", out _));

        Assert.Equal(["\"tenant-1\"", "\"cat-1\""], plan.PrimaryKeyValues);
        var update = Assert.Single(plan.CellWrites);
        Assert.Equal("favorite_toy", update.Column);
        Assert.Equal("\"Ball\"", update.JsonValue);
        Assert.Equal(new RegularRowWriteStrategy.GroupKey(200, 300), update.Key);
    }

    [Fact]
    public void BuildSplitWritePlan_CreatesKeyOnlyBaselineWhenEveryCellDiverges()
    {
        const string json =
            """
            {
              "tenant_id": "tenant-1",
              "cat_id": "cat-1",
              "favorite_toy": "Ball"
            }
            """;
        var metadata = new CdcRowMetadata(
            WritetimeMicros: 100,
            ExpiryEpochSeconds: null,
            PerColumn: new Dictionary<string, CdcCellMetadata>
            {
                ["favorite_toy"] = new(200, 300)
            });

        var plan = RegularRowWriteStrategy.BuildSplitWritePlan(
            json, metadata, PrimaryKeyColumns, PrimaryKeyColumnNames);

        using var baseline = JsonDocument.Parse(plan.BaselineJson);
        Assert.Equal(2, baseline.RootElement.EnumerateObject().Count());
        Assert.True(baseline.RootElement.TryGetProperty("tenant_id", out _));
        Assert.True(baseline.RootElement.TryGetProperty("cat_id", out _));
    }

    [Fact]
    public void BuildUpdateJsonColumnCql_UsesUpdateAndJsonConversionForKeysAndValue()
    {
        var cql = CassandraQueries.BuildUpdateJsonColumnCql(
            "ttl_liveness_repro",
            "cats",
            "favorite_toy",
            PrimaryKeyColumns);

        Assert.Equal(
            "UPDATE \"ttl_liveness_repro\".\"cats\" " +
            "USING TIMESTAMP ? AND TTL ? " +
            "SET \"favorite_toy\" = fromJson(?) " +
            "WHERE \"tenant_id\" = fromJson(?) AND \"cat_id\" = fromJson(?)",
            cql);
        Assert.DoesNotContain("INSERT", cql, StringComparison.OrdinalIgnoreCase);
    }
}
