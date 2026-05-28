using Cassandra;
using Cassandra.DataStax.Graph;
using Cassandra.Metrics;
using System.Net;

namespace CassandraMigrationProcessor.CassandraDriver;

/// <summary>
/// No-op ISession for simulated/dry-run mode.
/// Accepts all operations without connecting to any cluster.
/// Execute methods return empty RowSets; everything else throws
/// NotSupportedException so misuse is caught immediately.
/// </summary>
internal sealed class NullSession : ISession
{
    public ICluster Cluster => throw new NotSupportedException("NullSession has no cluster");
    public int BinaryProtocolVersion => 4;
    public bool IsDisposed => false;
    public string Keyspace => string.Empty;
    public UdtMappingDefinitions UserDefinedTypes => throw new NotSupportedException("NullSession has no UDT mappings");
    public string SessionName => "NullSession";

    // ── Execute (no-op) ──

    public RowSet Execute(IStatement statement) => new RowSet();
    public RowSet Execute(IStatement statement, string executionProfileName) => new RowSet();
    public RowSet Execute(string cqlQuery) => new RowSet();
    public RowSet Execute(string cqlQuery, string executionProfileName) => new RowSet();
    public RowSet Execute(string cqlQuery, ConsistencyLevel consistency) => new RowSet();
    public RowSet Execute(string cqlQuery, int pageSize) => new RowSet();

    public Task<RowSet> ExecuteAsync(IStatement statement) => Task.FromResult(new RowSet());
    public Task<RowSet> ExecuteAsync(IStatement statement, string executionProfileName) => Task.FromResult(new RowSet());

    // ── Prepare (not supported) ──

    public PreparedStatement Prepare(string cqlQuery) =>
        throw new NotSupportedException("NullSession cannot prepare statements");
    public PreparedStatement Prepare(string cqlQuery, IDictionary<string, byte[]> customPayload) =>
        throw new NotSupportedException("NullSession cannot prepare statements");
    public PreparedStatement Prepare(string cqlQuery, string keyspace) =>
        throw new NotSupportedException("NullSession cannot prepare statements");
    public PreparedStatement Prepare(string cqlQuery, string keyspace, IDictionary<string, byte[]> customPayload) =>
        throw new NotSupportedException("NullSession cannot prepare statements");

    public Task<PreparedStatement> PrepareAsync(string cqlQuery) =>
        throw new NotSupportedException("NullSession cannot prepare statements");
    public Task<PreparedStatement> PrepareAsync(string cqlQuery, IDictionary<string, byte[]> customPayload) =>
        throw new NotSupportedException("NullSession cannot prepare statements");
    public Task<PreparedStatement> PrepareAsync(string cqlQuery, string keyspace) =>
        throw new NotSupportedException("NullSession cannot prepare statements");
    public Task<PreparedStatement> PrepareAsync(string cqlQuery, string keyspace, IDictionary<string, byte[]> customPayload) =>
        throw new NotSupportedException("NullSession cannot prepare statements");

    // ── APM pattern (not supported) ──

    public IAsyncResult BeginExecute(IStatement statement, AsyncCallback callback, object state) =>
        throw new NotSupportedException();
    public IAsyncResult BeginExecute(string cqlQuery, ConsistencyLevel consistency, AsyncCallback callback, object state) =>
        throw new NotSupportedException();
    public RowSet EndExecute(IAsyncResult ar) => new RowSet();
    public IAsyncResult BeginPrepare(string cqlQuery, AsyncCallback callback, object state) =>
        throw new NotSupportedException();
    public PreparedStatement EndPrepare(IAsyncResult ar) =>
        throw new NotSupportedException();

    // ── Keyspace management (no-op) ──

    public void ChangeKeyspace(string keyspace) { }
    public void CreateKeyspace(string keyspace, Dictionary<string, string> replication = null, bool durableWrites = true) { }
    public void CreateKeyspaceIfNotExists(string keyspace, Dictionary<string, string> replication = null, bool durableWrites = true) { }
    public void DeleteKeyspace(string keyspace) { }
    public void DeleteKeyspaceIfExists(string keyspace) { }

    // ── Schema agreement (no-op) ──

    public void WaitForSchemaAgreement(RowSet rs) { }
    public bool WaitForSchemaAgreement(IPEndPoint hostAddress) => true;

    // ── Graph (not supported) ──

    public GraphResultSet ExecuteGraph(IGraphStatement statement) =>
        throw new NotSupportedException("NullSession does not support graph queries");
    public GraphResultSet ExecuteGraph(IGraphStatement statement, string executionProfileName) =>
        throw new NotSupportedException("NullSession does not support graph queries");
    public Task<GraphResultSet> ExecuteGraphAsync(IGraphStatement statement) =>
        throw new NotSupportedException("NullSession does not support graph queries");
    public Task<GraphResultSet> ExecuteGraphAsync(IGraphStatement statement, string executionProfileName) =>
        throw new NotSupportedException("NullSession does not support graph queries");

    // ── Metrics / Shutdown ──

    public IDriverMetrics GetMetrics() => throw new NotSupportedException("NullSession has no metrics");
    public Task ShutdownAsync() => Task.CompletedTask;

    // ── IDisposable ──

    public void Dispose() { }
}
