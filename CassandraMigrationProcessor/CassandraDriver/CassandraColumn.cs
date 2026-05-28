namespace CassandraMigrationProcessor.CassandraDriver;

/// <summary>
/// One row of <c>system_schema.columns</c> for a Cassandra table.
/// </summary>
/// <param name="Name">Column name (case-sensitive, unquoted).</param>
/// <param name="Type">CQL type string (e.g. <c>text</c>, <c>list&lt;int&gt;</c>,
/// <c>frozen&lt;my_udt&gt;</c>).</param>
/// <param name="Kind"><c>partition_key</c>, <c>clustering</c>,
/// <c>regular</c>, or <c>static</c>.</param>
/// <param name="ClusteringOrder"><c>asc</c>, <c>desc</c>, or <c>none</c>
/// for non-clustering columns.</param>
/// <param name="Position">Ordinal within the column's key group
/// (partition key / clustering key); 0 for regular and static columns.</param>
public record CassandraColumn(
    string Name,
    string Type,
    string Kind,
    string ClusteringOrder,
    int Position);
