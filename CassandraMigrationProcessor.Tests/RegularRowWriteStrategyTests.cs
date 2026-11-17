using System.Text.Json;
using CassandraMigrationProcessor.CassandraDriver;
using CassandraMigrationProcessor.DataTransfer;

namespace CassandraMigrationProcessor.Tests;

public sealed class CdcJsonRowParserTests
{
    // Payloads below were captured from a Cosmos DB Cassandra API source for
    // CREATE TABLE t (id text PRIMARY KEY, a text, b text, l list<int>,
    //   s set<int>, m map<text,int>, u addr) with a non-frozen UDT addr.

    [Fact]
    public void Parse_UniformUdtRowHasNoDivergence()
    {
        const string json = """{"id": "udt1", "a": null, "b": null, "l": null, "s": null, "m": null, "u": {"street": "s", "zip": 1}, "__sys_clts": {"a": null, "b": null, "l": null, "s": null, "m": null, "u": {"street": 1794900042988616, "zip": 1794900042988616}}, "__sys_rw_ts": 1794900042988616, "__sys_clts_tmbstn": {"a": null, "b": null, "l": null, "s": null, "m": null, "u": [1794900042988615, {"street": null, "zip": null}]}, "__sys_rw_tmbstn": null, "__sys_clttl": [0, {"a": null, "b": null, "l": null, "s": null, "m": null, "u": {"street": [0, 0], "zip": [0, 0]}}], "__sys_rw_ttl": [0, 0], "__sys_clts_listorder": null}""";

        var (cleaned, metadata) = CdcJsonRowParser.ParseOnce(json);

        Assert.Equal(1794900042988616, metadata.WritetimeMicros);
        Assert.Null(metadata.ExpiryEpochSeconds);
        Assert.Null(metadata.PerColumn);
        Assert.DoesNotContain("__sys_", cleaned, StringComparison.Ordinal);
        Assert.Contains("\"u\":{\"street\":\"s\",\"zip\":1}", cleaned, StringComparison.Ordinal);
    }

    [Fact]
    public void Parse_UdtFieldsWithDifferentTtlsReportFieldExpiries()
    {
        const string json = """{"id": "udt2", "a": null, "b": null, "l": null, "s": null, "m": null, "u": {"street": "s", "zip": 2}, "__sys_clts": {"a": null, "b": null, "l": null, "s": null, "m": null, "u": {"street": 1794900043317551, "zip": 1794900043405062}}, "__sys_rw_ts": 1794900043317551, "__sys_clts_tmbstn": {"a": null, "b": null, "l": null, "s": null, "m": null, "u": [1794900043317550, {"street": null, "zip": null}]}, "__sys_rw_tmbstn": null, "__sys_clttl": [1794903043, {"a": null, "b": null, "l": null, "s": null, "m": null, "u": {"street": [0, 0], "zip": [3000, 0]}}], "__sys_rw_ttl": [0, 0], "__sys_clts_listorder": null}""";

        var (_, metadata) = CdcJsonRowParser.ParseOnce(json);

        var u = Assert.Single(metadata.PerColumn!);
        Assert.Equal("u", u.Key);
        Assert.Equal(1794900043405062, u.Value.WritetimeMicros);
        Assert.Null(u.Value.ExpiryEpochSeconds);
        Assert.Null(u.Value.ElementExpiries);
        Assert.Equal(2, u.Value.FieldExpiries!.Count);
        Assert.Null(u.Value.FieldExpiries["street"]);
        Assert.Equal(1794903043, u.Value.FieldExpiries["zip"]);
    }

    [Fact]
    public void Parse_CollectionElementsWithDifferentTtlsReportElementExpiries()
    {
        const string json = """{"id": "coll1", "a": null, "b": null, "l": [1, 2, 3], "s": [1, 2, 3], "m": {"a": 1, "b": 2, "c": 3}, "u": null, "__sys_clts": {"a": null, "b": null, "l": [1794900043519733, 1794900043622825, 1794900043721403], "s": [1794900043519733, 1794900043622825, 1794900043721403], "m": [1794900043519733, 1794900043622825, 1794900043721403], "u": null}, "__sys_rw_ts": 1794900043519733, "__sys_clts_tmbstn": {"a": null, "b": null, "l": [1794900043519732, {}], "s": [1794900043519732, {}], "m": [1794900043519732, {}], "u": null}, "__sys_rw_tmbstn": null, "__sys_clttl": [1794903043, {"a": null, "b": null, "l": [[0, 0], [0, 0], [3000, 0]], "s": [[0, 0], [0, 0], [3000, 0]], "m": [[0, 0], [0, 0], [3000, 0]], "u": null}], "__sys_rw_ttl": [0, 0], "__sys_clts_listorder": {"l": ["0x0111f1e257668da787a6018127caed028f", "0x0111f1e257669c9eee8dbbf5a780cb9971", "0x0111f1e25766aa02b09ed0dd391377bc27"]}}""";

        var (cleaned, metadata) = CdcJsonRowParser.ParseOnce(json);

        Assert.Equal(3, metadata.PerColumn!.Count);
        foreach (var column in new[] { "l", "s", "m" })
        {
            var cell = metadata.PerColumn[column];
            Assert.Equal(1794900043721403, cell.WritetimeMicros);
            Assert.Null(cell.ExpiryEpochSeconds);
            Assert.Equal(new long?[] { null, null, 1794903043 }, cell.ElementExpiries);
        }
        Assert.DoesNotContain("__sys_", cleaned, StringComparison.Ordinal);
    }

    [Fact]
    public void Parse_UpdateOnlyRowReportsEveryLiveCell()
    {
        const string json = """{"id": "updonly", "a": null, "b": null, "l": [9], "s": null, "m": null, "u": null, "__sys_clts": {"a": null, "b": null, "l": [1794900043808113], "s": null, "m": null, "u": null}, "__sys_rw_ts": null, "__sys_clts_tmbstn": null, "__sys_rw_tmbstn": null, "__sys_clttl": [1794902043, {"a": null, "b": null, "l": [[2000, 0]], "s": null, "m": null, "u": null}], "__sys_rw_ttl": null, "__sys_clts_listorder": {"l": ["0x0111f1e25766b73c14ba578bfdc45797fa"]}}""";

        var (_, metadata) = CdcJsonRowParser.ParseOnce(json);

        Assert.False(metadata.HasRowMarker);
        Assert.Equal(1794900043808113, metadata.RowWriteTimestampMicros);
        var l = Assert.Single(metadata.PerColumn!);
        Assert.Equal("l", l.Key);
        Assert.Equal(new CdcCellMetadata(1794900043808113, 1794902043), l.Value);
    }

    [Fact]
    public void Parse_CellsKeepTheirOwnTtlWhenTheyDifferFromTheRow()
    {
        const string json = """{"id": "ttlmix", "a": "x", "b": "immortal", "l": null, "s": null, "m": null, "u": null, "__sys_clts": {"a": 1794900043895627, "b": 1794900043982718, "l": null, "s": null, "m": null, "u": null}, "__sys_rw_ts": 1794900043895627, "__sys_clts_tmbstn": null, "__sys_rw_tmbstn": null, "__sys_clttl": [1794904043, {"a": [4000, 0], "b": [0, 0], "l": null, "s": null, "m": null, "u": null}], "__sys_rw_ttl": [4000, 0], "__sys_clts_listorder": null}""";

        var (_, metadata) = CdcJsonRowParser.ParseOnce(json);

        Assert.Equal(1794900043895627, metadata.WritetimeMicros);
        Assert.Equal(1794904043, metadata.ExpiryEpochSeconds);
        var b = Assert.Single(metadata.PerColumn!);
        Assert.Equal("b", b.Key);
        Assert.Equal(new CdcCellMetadata(1794900043982718, null), b.Value);
    }

    [Fact]
    public void Parse_PermanentRowMarkerStaysPermanentWhenOneCellHasTtl()
    {
        const string json =
            """
            {
              "tenant_id": "tenant-1", "cat_id": "cat-1", "name": "Milo", "favorite_toy": "Ball",
              "__sys_rw_ts": 100,
              "__sys_rw_ttl": [0, 0],
              "__sys_clttl": [2000, { "name": [0, 0], "favorite_toy": [30, 0] }],
              "__sys_clts": { "name": 100, "favorite_toy": 200 }
            }
            """;

        var (_, metadata) = CdcJsonRowParser.ParseOnce(json);

        Assert.Null(metadata.ExpiryEpochSeconds);
        var favoriteToy = Assert.Single(metadata.PerColumn!);
        Assert.Equal("favorite_toy", favoriteToy.Key);
        Assert.Equal(new CdcCellMetadata(200, 2000), favoriteToy.Value);
    }

    [Fact]
    public void Parse_NullCellMetadataForUnsetColumnIsIgnored()
    {
        const string json =
            """
            {
              "tenant_id": "tenant-1", "cat_id": "cat-1", "name": "Milo", "compromised": null,
              "__sys_rw_ts": 100,
              "__sys_rw_ttl": [0, 0],
              "__sys_clttl": [0, { "name": [0, 0], "compromised": null }],
              "__sys_clts": { "name": 100, "compromised": null }
            }
            """;

        var (cleaned, metadata) = CdcJsonRowParser.ParseOnce(json);

        Assert.Contains("\"compromised\":null", cleaned);
        Assert.Equal(100, metadata.WritetimeMicros);
        Assert.Null(metadata.PerColumn);
    }

    [Fact]
    public void Parse_RowMarkerExpiryUsesCellBase()
    {
        const string json =
            """
            {
              "tenant_id": "tenant-1", "cat_id": "cat-1", "favorite_toy": "Ball",
              "__sys_rw_ts": 100,
              "__sys_rw_ttl": [30, 0],
              "__sys_clttl": [2000, { "favorite_toy": [30, 0] }],
              "__sys_clts": { "favorite_toy": 100 }
            }
            """;

        var (_, metadata) = CdcJsonRowParser.ParseOnce(json);

        Assert.Equal(2000, metadata.ExpiryEpochSeconds);
        Assert.Null(metadata.PerColumn);
    }

    [Fact]
    public void Parse_TtlWithoutExpiryBaseIsAnchoredOnCellWritetime()
    {
        // Static cells can be reported with an expiry base of 0.
        const string json =
            """
            {
              "pk": "p", "ck": null, "st": "x",
              "__sys_rw_ts": null,
              "__sys_clttl": [0, { "st": [30, 0] }],
              "__sys_clts": { "st": 2000000000 }
            }
            """;

        var (_, metadata) = CdcJsonRowParser.ParseOnce(json);

        Assert.Equal(new CdcCellMetadata(2000000000, 2030), metadata.PerColumn!["st"]);
    }

    [Fact]
    public void Parse_EmptyCollectionMetadataDoesNotSynthesizeCellTtl()
    {
        const string json =
            """
            {
              "tenant_id": "tenant-1", "cat_id": "cat-1", "tags": [],
              "__sys_rw_ts": 100,
              "__sys_rw_ttl": [0, 0],
              "__sys_clttl": [2000, { "tags": [] }],
              "__sys_clts": { "tags": [] }
            }
            """;

        var (_, metadata) = CdcJsonRowParser.ParseOnce(json);

        Assert.Null(metadata.ExpiryEpochSeconds);
        Assert.Null(metadata.PerColumn);
    }

    [Fact]
    public void Parse_UpdatedCellsUseTheirNewestWritetime()
    {
        const string json =
            """
            {
              "tenant_id": "tenant-1", "cat_id": "cat-1", "name": "Alpha", "favorite_toy": "Ball", "tags": ["a", "b"],
              "__sys_rw_ts": 100,
              "__sys_rw_ttl": [0, 0],
              "__sys_clttl": [0, {}],
              "__sys_clts": { "name": 300, "favorite_toy": 100, "tags": [100, 250] }
            }
            """;

        var (_, metadata) = CdcJsonRowParser.ParseOnce(json);

        Assert.Equal(300, metadata.RowWriteTimestampMicros);
        Assert.Equal(2, metadata.PerColumn!.Count);
        Assert.Equal(new CdcCellMetadata(300, null), metadata.PerColumn["name"]);
        Assert.Equal(new CdcCellMetadata(250, null), metadata.PerColumn["tags"]);
    }

    [Fact]
    public void Parse_DivergentCellWithoutTtlDetailKeepsRowExpiry()
    {
        const string json =
            """
            {
              "tenant_id": "tenant-1", "cat_id": "cat-1", "payload": "old", "other": "new",
              "__sys_rw_ts": 1000,
              "__sys_rw_ttl": [86400, 0],
              "__sys_clttl": [1780655059, {}],
              "__sys_clts": { "payload": 500, "other": 1000 }
            }
            """;

        var (_, metadata) = CdcJsonRowParser.ParseOnce(json);

        Assert.Equal(1780655059, metadata.ExpiryEpochSeconds);
        var cell = Assert.Single(metadata.PerColumn!);
        Assert.Equal("payload", cell.Key);
        Assert.Equal(new CdcCellMetadata(500, 1780655059), cell.Value);
    }

    [Theory]
    [InlineData("null")]
    [InlineData("{}")]
    [InlineData("""{ "name": null, "tags": [] }""")]
    [InlineData("\"unexpected\"")]
    public void Parse_ToleratesCellWritetimeShapesWithoutValues(string clts)
    {
        var json =
            "{\"tenant_id\":\"tenant-1\",\"cat_id\":\"cat-1\",\"name\":\"Milo\"," +
            $"\"__sys_rw_ts\":100,\"__sys_clts\":{clts},\"__sys_rw_ttl\":[0,0]}}";

        var (cleaned, metadata) = CdcJsonRowParser.ParseOnce(json);

        Assert.Equal(100, metadata.RowWriteTimestampMicros);
        Assert.DoesNotContain("__sys_", cleaned, StringComparison.Ordinal);
    }

    [Fact]
    public void Parse_NegativeTtlDurationIsRejected()
    {
        const string json =
            """{"id":"x","a":"v","__sys_rw_ts":100,"__sys_clttl":[0,{"a":[-1,0]}]}""";

        Assert.Throws<JsonException>(() => CdcJsonRowParser.ParseOnce(json));
    }

    // Captured for CREATE TABLE f (id text PRIMARY KEY, fl frozen<list<int>>,
    //   fm frozen<map<text,text>>, l list<text>, v text).
    [Fact]
    public void Parse_FrozenCollectionWithoutCellIsNullNotEmpty()
    {
        const string json = """{"id": "nomarker", "fl": [], "fm": {}, "l": ["t"], "v": null, "__sys_clts": {"fl": null, "fm": null, "l": [1794902238466226], "v": null}, "__sys_rw_ts": null, "__sys_clts_tmbstn": null, "__sys_rw_tmbstn": null, "__sys_clttl": [1794905238, {"fl": null, "fm": null, "l": [[3000, 0]], "v": null}], "__sys_rw_ttl": null, "__sys_clts_listorder": {"l": ["0x0111f1e25c82da73469e652b80cacab028"]}}""";

        var (cleaned, _) = CdcJsonRowParser.ParseOnce(json);

        using var doc = JsonDocument.Parse(cleaned);
        Assert.Equal(JsonValueKind.Null, doc.RootElement.GetProperty("fl").ValueKind);
        Assert.Equal(JsonValueKind.Null, doc.RootElement.GetProperty("fm").ValueKind);
        Assert.Equal("[\"t\"]", doc.RootElement.GetProperty("l").GetRawText());
        Assert.Equal("\"nomarker\"", doc.RootElement.GetProperty("id").GetRawText());
    }

    [Fact]
    public void Parse_ExplicitlyEmptyFrozenCollectionIsKept()
    {
        const string json = """{"id": "empty", "fl": [], "fm": {}, "l": null, "v": "x", "__sys_clts": {"fl": 1794902238251749, "fm": 1794902238251749, "l": null, "v": 1794902238251749}, "__sys_rw_ts": 1794902238251749, "__sys_clts_tmbstn": null, "__sys_rw_tmbstn": null, "__sys_clttl": [0, {"fl": [0, 0], "fm": [0, 0], "l": null, "v": [0, 0]}], "__sys_rw_ttl": [0, 0], "__sys_clts_listorder": null}""";

        var (cleaned, _) = CdcJsonRowParser.ParseOnce(json);

        using var doc = JsonDocument.Parse(cleaned);
        Assert.Equal("[]", doc.RootElement.GetProperty("fl").GetRawText());
        Assert.Equal("{}", doc.RootElement.GetProperty("fm").GetRawText());
    }

    // Captured for CREATE TABLE d (id text PRIMARY KEY, a text, b text,
    //   s set<int>, m map<text,int>, fl frozen<list<int>>, u frozen<list<int>>)
    // after: INSERT all; SET b = null; s = s - {2}, m = m - {'y'}; DELETE fl.
    [Fact]
    public void Parse_DeletionsReportNewestTombstonePerColumn()
    {
        const string json = """{"id": "r1", "a": "a", "b": null, "s": [1], "m": {"x": 1}, "fl": [], "u": [], "__sys_clts": {"a": 1794902680767536, "b": null, "s": [1794902680767536], "m": [1794902680767536], "fl": null, "u": null}, "__sys_rw_ts": 1794902680767536, "__sys_clts_tmbstn": {"a": null, "b": 1794902680992867, "s": [1794902680767535, {"2": 1794902681088410}], "m": [1794902680767535, {"y": 1794902681088410}], "fl": 1794902681190639, "u": null}, "__sys_rw_tmbstn": null, "__sys_clttl": [0, {"a": [0, 0], "b": null, "s": [[0, 0]], "m": [[0, 0]], "fl": null, "u": null}], "__sys_rw_ttl": [0, 0]}""";

        var (cleaned, metadata) = CdcJsonRowParser.ParseOnce(json);

        var deletions = Assert.IsAssignableFrom<IReadOnlyDictionary<string, CdcCellDeletion>>(metadata.Deletions);
        Assert.Equal(new CdcCellDeletion(1794902680992867, false), deletions["b"]);
        Assert.Equal(new CdcCellDeletion(1794902681088410, true), deletions["s"]);
        Assert.Equal(new CdcCellDeletion(1794902681088410, true), deletions["m"]);
        Assert.Equal(new CdcCellDeletion(1794902681190639, false), deletions["fl"]);
        Assert.False(deletions.ContainsKey("a"));
        Assert.False(deletions.ContainsKey("u"));
        Assert.DoesNotContain("__sys_", cleaned, StringComparison.Ordinal);
    }
}

public sealed class RegularRowWriteStrategyTests
{
    private const long Now = 1_000_000_000;

    private static readonly CassandraColumn[] PrimaryKeyColumns =
    [
        new("pk", "text", "partition_key", "none", 0),
        new("ck", "int", "clustering", "asc", 0)
    ];

    private static readonly Dictionary<string, CassandraColumn> ColumnsByName =
        PrimaryKeyColumns
            .Append(new CassandraColumn("v", "text", "regular", "none", -1))
            .Append(new CassandraColumn("w", "text", "regular", "none", -1))
            .Append(new CassandraColumn("st", "text", "static", "none", -1))
            .Append(new CassandraColumn("l", "list<int>", "regular", "none", -1))
            .Append(new CassandraColumn("fl", "frozen<list<int>>", "regular", "none", -1))
            .Append(new CassandraColumn("s", "set<int>", "regular", "none", -1))
            .Append(new CassandraColumn("m", "map<text, int>", "regular", "none", -1))
            .Append(new CassandraColumn("u", "addr", "regular", "none", -1))
            .ToDictionary(c => c.Name, StringComparer.Ordinal);

    private static RegularRowWriteStrategy.RowWritePlan Plan(
        string json, CdcRowMetadata metadata, bool isReplay = false) =>
        RegularRowWriteStrategy.BuildRowWritePlan(
            json, metadata, isReplay, PrimaryKeyColumns, ColumnsByName, Now);

    private static Dictionary<string, CdcCellMetadata> Cells(
        params (string Name, CdcCellMetadata Cell)[] cells) =>
        cells.ToDictionary(c => c.Name, c => c.Cell, StringComparer.Ordinal);

    [Fact]
    public void UniformRowWithMarkerIsOneFullInsert()
    {
        var plan = Plan(
            """{"pk":"p","ck":1,"v":"a","w":null,"st":"s","fl":[1,2]}""",
            new CdcRowMetadata(100, 2000));

        Assert.True(plan.UseFullInsert);
        Assert.False(plan.IsStaticOnly);
        Assert.Equal(100, plan.BaselineWritetimeMicros);
        Assert.Equal(2000, plan.BaselineExpiryEpochSeconds);
        using var baseline = JsonDocument.Parse(plan.BaselineJson!);
        Assert.Equal(6, baseline.RootElement.EnumerateObject().Count());
        Assert.Empty(plan.Groups);
    }

    [Fact]
    public void DivergentCellIsWrittenSeparatelyFromBaseline()
    {
        var plan = Plan(
            """{"pk":"p","ck":1,"v":"a","w":null}""",
            new CdcRowMetadata(100, null, Cells(("v", new CdcCellMetadata(200, 300)))));

        Assert.False(plan.UseFullInsert);
        using var baseline = JsonDocument.Parse(plan.BaselineJson!);
        Assert.False(baseline.RootElement.TryGetProperty("v", out _));
        Assert.Equal(JsonValueKind.Null, baseline.RootElement.GetProperty("w").ValueKind);
        Assert.Equal(["\"p\"", "1"], plan.PrimaryKeyValues);

        var group = Assert.Single(plan.Groups);
        Assert.Equal(200, group.WritetimeMicros);
        Assert.Equal(300, group.ExpiryEpochSeconds);
        Assert.False(group.IsStatic);
        Assert.Equal("v", Assert.Single(group.Columns).Name);
        Assert.Equal("\"a\"", Assert.Single(group.JsonValues));
    }

    [Fact]
    public void RowWithoutMarkerIsWrittenWithUpdatesOnly()
    {
        var plan = Plan(
            """{"pk":"p","ck":1,"v":"a","w":null}""",
            new CdcRowMetadata(null, null, Cells(("v", new CdcCellMetadata(500, 900))), 500));

        Assert.Null(plan.BaselineJson);
        Assert.False(plan.UseFullInsert);
        Assert.Equal(2, plan.Groups.Count);
        Assert.Contains(plan.Groups, g =>
            g.Columns.Single().Name == "v" && g.WritetimeMicros == 500 && g.ExpiryEpochSeconds == 900);
        Assert.Contains(plan.Groups, g =>
            g.Columns.Single().Name == "w" && g.WritetimeMicros == 500 && g.ExpiryEpochSeconds is null);
    }

    [Fact]
    public void CellsSharingWritetimeAndTtlAreGroupedButStaticCellsAreNot()
    {
        var cell = new CdcCellMetadata(200, 300);
        var plan = Plan(
            """{"pk":"p","ck":1,"v":"a","w":"b","st":"s"}""",
            new CdcRowMetadata(100, null, Cells(("v", cell), ("w", cell), ("st", cell))));

        Assert.Equal(2, plan.Groups.Count);
        var regular = plan.Groups.Single(g => !g.IsStatic);
        Assert.Equal(["v", "w"], regular.Columns.Select(c => c.Name));
        var stat = plan.Groups.Single(g => g.IsStatic);
        Assert.Equal("st", stat.Columns.Single().Name);
        Assert.Equal(["\"p\""], plan.KeyValuesFor(isStatic: true));
        Assert.Equal(["\"p\"", "1"], plan.KeyValuesFor(isStatic: false));
    }

    [Fact]
    public void StaticOnlyPartitionWritesOnlyStaticCellsWithoutBaseline()
    {
        var plan = Plan(
            """{"pk":"p","ck":null,"v":null,"st":"s"}""",
            new CdcRowMetadata(null, null, Cells(("st", new CdcCellMetadata(200, 2030))), 200));

        Assert.True(plan.IsStaticOnly);
        Assert.Null(plan.BaselineJson);
        Assert.False(plan.UseFullInsert);
        Assert.Equal(["\"p\""], plan.PrimaryKeyValues);
        var group = Assert.Single(plan.Groups);
        Assert.True(group.IsStatic);
        Assert.Equal(2030, group.ExpiryEpochSeconds);
    }

    [Fact]
    public void NonFrozenListIsSplitIntoRunsOfEqualExpiry()
    {
        var plan = Plan(
            """{"pk":"p","ck":1,"v":"a","l":[1,2,3,4]}""",
            new CdcRowMetadata(100, null, Cells(
                ("l", new CdcCellMetadata(150, null, ElementExpiries: [null, null, 900, null])))));

        Assert.False(plan.UseFullInsert);
        var list = Assert.Single(plan.Lists);
        Assert.Equal("l", list.Column.Name);
        Assert.Equal(
            [new(null, "[1,2]"), new(900, "[3]"), new(null, "[4]")],
            list.Runs);
    }

    [Fact]
    public void UniformListIsStillRewrittenAsAList()
    {
        var plan = Plan(
            """{"pk":"p","ck":1,"l":[1,2]}""",
            new CdcRowMetadata(100, 2000));

        Assert.False(plan.UseFullInsert);
        var run = Assert.Single(Assert.Single(plan.Lists).Runs);
        Assert.Equal(2000, run.ExpiryEpochSeconds);
        Assert.Equal("[1,2]", run.JsonValue);
        using var baseline = JsonDocument.Parse(plan.BaselineJson!);
        Assert.False(baseline.RootElement.TryGetProperty("l", out _));
    }

    [Fact]
    public void NullListIsClearedExplicitlyOnlyOnReplay()
    {
        const string json = """{"pk":"p","ck":1,"v":"a","l":null}""";
        var metadata = new CdcRowMetadata(100, null);

        var bulk = Plan(json, metadata);
        Assert.True(bulk.UseFullInsert);
        Assert.Empty(bulk.Lists);

        var replay = Plan(json, metadata, isReplay: true);
        Assert.False(replay.UseFullInsert);
        Assert.Empty(Assert.Single(replay.Lists).Runs);
    }

    [Fact]
    public void SetAndMapElementsWithDifferentTtlsAreAddedSeparately()
    {
        var plan = Plan(
            """{"pk":"p","ck":1,"s":[1,2,3],"m":{"a":1,"b":2,"c":3}}""",
            new CdcRowMetadata(100, null, Cells(
                ("s", new CdcCellMetadata(150, null, ElementExpiries: [null, 900, null])),
                ("m", new CdcCellMetadata(150, null, ElementExpiries: [900, null, 900])))));

        Assert.Equal(2, plan.Groups.Count);
        var set = plan.Groups.Single(g => g.Columns.Single().Name == "s");
        Assert.Null(set.ExpiryEpochSeconds);
        Assert.Equal("[1,3]", set.JsonValues.Single());
        var map = plan.Groups.Single(g => g.Columns.Single().Name == "m");
        Assert.Equal(900, map.ExpiryEpochSeconds);
        Assert.Equal("""{"a":1,"c":3}""", map.JsonValues.Single());
        Assert.Equal(2, plan.Adds.Count);
        Assert.Contains(plan.Adds, a => a.Column.Name == "s" && a.ExpiryEpochSeconds == 900 && a.JsonValue == "[2]");
        Assert.Contains(plan.Adds, a => a.Column.Name == "m" && a.ExpiryEpochSeconds is null && a.JsonValue == """{"b":2}""");
    }

    [Fact]
    public void ElementCountMismatchFallsBackToUniformWrite()
    {
        var plan = Plan(
            """{"pk":"p","ck":1,"s":[1,2,3]}""",
            new CdcRowMetadata(100, null, Cells(
                ("s", new CdcCellMetadata(150, 900, ElementExpiries: [null, 900])))));

        var group = Assert.Single(plan.Groups);
        Assert.Equal(900, group.ExpiryEpochSeconds);
        Assert.Equal("[1,2,3]", Assert.Single(group.JsonValues));
        Assert.Empty(plan.Adds);
    }

    [Fact]
    public void UdtFieldsWithDifferentTtlsAreWrittenPerField()
    {
        var plan = Plan(
            """{"pk":"p","ck":1,"u":{"street":"s","zip":2,"note":null}}""",
            new CdcRowMetadata(100, null, Cells(
                ("u", new CdcCellMetadata(150, null,
                    FieldExpiries: new Dictionary<string, long?> { ["street"] = null, ["zip"] = 900 })))));

        var group = Assert.Single(plan.Groups);
        Assert.Null(group.ExpiryEpochSeconds);
        Assert.Equal("""{"street":"s","note":null}""", Assert.Single(group.JsonValues));
        var field = Assert.Single(plan.FieldWrites);
        Assert.Equal("zip", field.Field);
        Assert.Equal(900, field.ExpiryEpochSeconds);
        Assert.Equal("2", field.JsonValue);
        Assert.Equal(150, field.WritetimeMicros);
    }

    [Fact]
    public void DeletedCellsAreWrittenAtTheSourceDeletionTimestamp()
    {
        var plan = Plan(
            """{"pk":"p","ck":1,"v":null,"w":null,"fl":null,"s":null}""",
            new CdcRowMetadata(100, null, Deletions: new Dictionary<string, CdcCellDeletion>
            {
                ["v"] = new(300, IsComplex: false),
                ["w"] = new(50, IsComplex: false),
                ["s"] = new(400, IsComplex: true),
            }));

        using var baseline = JsonDocument.Parse(plan.BaselineJson!);
        Assert.False(baseline.RootElement.TryGetProperty("v", out _));
        Assert.False(baseline.RootElement.TryGetProperty("s", out _));
        Assert.Equal(JsonValueKind.Null, baseline.RootElement.GetProperty("w").ValueKind);
        Assert.Equal(JsonValueKind.Null, baseline.RootElement.GetProperty("fl").ValueKind);
        Assert.Contains(plan.Groups, g =>
            g.Columns.Single().Name == "v" && g.WritetimeMicros == 300 && g.JsonValues.Single() == "null");
        Assert.Contains(plan.Groups, g =>
            g.Columns.Single().Name == "s" && g.WritetimeMicros == 401 && g.JsonValues.Single() == "null");
    }

    [Fact]
    public void CollectionIsRewrittenAboveItsNewestElementDeletion()
    {
        var plan = Plan(
            """{"pk":"p","ck":1,"v":"a","s":[1],"m":{"x":1}}""",
            new CdcRowMetadata(100, null, Deletions: new Dictionary<string, CdcCellDeletion>
            {
                ["v"] = new(90, IsComplex: false),
                ["s"] = new(500, IsComplex: true),
                ["m"] = new(99, IsComplex: true),
            }));

        using var baseline = JsonDocument.Parse(plan.BaselineJson!);
        Assert.Equal("\"a\"", baseline.RootElement.GetProperty("v").GetRawText());
        Assert.Equal("{\"x\":1}", baseline.RootElement.GetProperty("m").GetRawText());
        Assert.False(baseline.RootElement.TryGetProperty("s", out _));
        var group = Assert.Single(plan.Groups);
        Assert.Equal("s", group.Columns.Single().Name);
        Assert.Equal(501, group.WritetimeMicros);
        Assert.Equal("[1]", group.JsonValues.Single());
    }

    [Fact]
    public void UnknownColumnIsRejected()
    {
        Assert.Throws<JsonException>(() =>
            Plan("""{"pk":"p","ck":1,"nope":1}""", new CdcRowMetadata(100, null)));
    }

    [Theory]
    [InlineData(null, 0, false)]
    [InlineData(Now - 5, 1, false)]
    [InlineData(Now + 60, 60, false)]
    [InlineData(Now + 700_000_000L, RegularRowWriteStrategy.MaxTtlSeconds, true)]
    public void ResolveTtlSecondsHandlesExpiredImmortalAndOversizedTtls(
        long? expiry, int expected, bool expectedClamped)
    {
        int ttl = RegularRowWriteStrategy.ResolveTtlSeconds(expiry, Now, clampToLegacyExpiry: false, out bool clamped);

        Assert.Equal(expected, ttl);
        Assert.Equal(expectedClamped, clamped);
    }

    [Fact]
    public void ResolveTtlSecondsClampsExpiryBefore2038ForLegacyTargets()
    {
        long now = 2_000_000_000;
        long expiry = now + 400_000_000;

        int ttl = RegularRowWriteStrategy.ResolveTtlSeconds(expiry, now, clampToLegacyExpiry: true, out bool clamped);

        Assert.True(clamped);
        Assert.True(now + ttl < int.MaxValue);
        Assert.Equal(400_000_000,
            RegularRowWriteStrategy.ResolveTtlSeconds(expiry, now, clampToLegacyExpiry: false, out _));
    }

    [Fact]
    public void MonotonicTimestampNeverRepeatsAndIsNotBehindWallClock()
    {
        long before = MonotonicTimestamp.NowMicros();

        long first = MonotonicTimestamp.Reserve(3);
        long next = MonotonicTimestamp.Reserve(1);

        Assert.True(first >= before);
        Assert.True(next > first + 2);
    }
}
