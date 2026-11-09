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

    private static readonly CassandraColumn FavoriteToyColumn =
        new("favorite_toy", "text", "regular", "none", -1);

    private static readonly CassandraColumn MetadataColumn =
        new("metadata", "text", "static", "none", -1);

    private static readonly Dictionary<string, CassandraColumn> ColumnsByName =
        PrimaryKeyColumns
            .Append(FavoriteToyColumn)
            .Append(MetadataColumn)
            .Append(new CassandraColumn("name", "text", "regular", "none", -1))
            .Append(new CassandraColumn("vaccinated", "boolean", "regular", "none", -1))
            .ToDictionary(c => c.Name, StringComparer.Ordinal);

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
            json,
            metadata,
            PrimaryKeyColumns,
            PrimaryKeyColumnNames,
            ColumnsByName);

        using var baseline = JsonDocument.Parse(plan.BaselineJson);
        var root = baseline.RootElement;
        Assert.Equal("tenant-1", root.GetProperty("tenant_id").GetString());
        Assert.Equal("cat-1", root.GetProperty("cat_id").GetString());
        Assert.Equal("Milo", root.GetProperty("name").GetString());
        Assert.Equal(JsonValueKind.Null, root.GetProperty("vaccinated").ValueKind);
        Assert.False(root.TryGetProperty("favorite_toy", out _));

        Assert.Equal(["\"tenant-1\"", "\"cat-1\""], plan.PrimaryKeyValues);
        var update = Assert.Single(plan.CellWrites);
        Assert.Equal(FavoriteToyColumn, update.Column);
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
            json,
            metadata,
            PrimaryKeyColumns,
            PrimaryKeyColumnNames,
            ColumnsByName);

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
            FavoriteToyColumn,
            PrimaryKeyColumns);

        Assert.Equal(
            "UPDATE \"ttl_liveness_repro\".\"cats\" " +
            "USING TIMESTAMP ? AND TTL ? " +
            "SET \"favorite_toy\" = fromJson(?) " +
            "WHERE \"tenant_id\" = fromJson(?) AND \"cat_id\" = fromJson(?)",
            cql);
        Assert.DoesNotContain("INSERT", cql, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void BuildUpdateJsonColumnCql_StaticColumnUsesPartitionKeyOnly()
    {
        var cql = CassandraQueries.BuildUpdateJsonColumnCql(
            "ttl_liveness_repro",
            "cats",
            MetadataColumn,
            PrimaryKeyColumns);

        Assert.Equal(
            "UPDATE \"ttl_liveness_repro\".\"cats\" " +
            "USING TIMESTAMP ? AND TTL ? " +
            "SET \"metadata\" = fromJson(?) " +
            "WHERE \"tenant_id\" = fromJson(?)",
            cql);
        Assert.DoesNotContain("\"cat_id\"", cql, StringComparison.Ordinal);
    }

    [Fact]
    public void Parse_PermanentRowMarkerStaysPermanentWhenOneCellHasTtl()
    {
        const string json =
            """
            {
              "tenant_id": "tenant-1",
              "cat_id": "cat-1",
              "name": "Milo",
              "favorite_toy": "Ball",
              "__sys_rw_ts": 100,
              "__sys_rw_ttl": [0, 0],
              "__sys_clttl": [2000, {
                "name": [0, 0],
                "favorite_toy": [30, 0]
              }],
              "__sys_clts": {
                "name": 100,
                "favorite_toy": 200
              }
            }
            """;

        var (_, metadata) = CdcJsonRowParser.ParseOnce(
            json, preserveCellLevel: true);

        Assert.Null(metadata.ExpiryEpochSeconds);
        var favoriteToy = Assert.Single(metadata.PerColumn!);
        Assert.Equal("favorite_toy", favoriteToy.Key);
        Assert.Equal(new CdcCellMetadata(200, 2000), favoriteToy.Value);
    }

    [Fact]
    public void Parse_RowMarkerExpiryUsesCellBaseWhenRowTtlIsPresent()
    {
        const string json =
            """
            {
              "tenant_id": "tenant-1",
              "cat_id": "cat-1",
              "favorite_toy": "Ball",
              "__sys_rw_ts": 100,
              "__sys_rw_ttl": [30, 0],
              "__sys_clttl": [2000, {
                "favorite_toy": [30, 0]
              }],
              "__sys_clts": {
                "favorite_toy": 100
              }
            }
            """;

        var (_, metadata) = CdcJsonRowParser.ParseOnce(
            json, preserveCellLevel: true);

        Assert.Equal(2000, metadata.ExpiryEpochSeconds);
        Assert.Null(metadata.PerColumn);
    }
}
