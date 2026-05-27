using Cassandra;
using CassandraMigrationProcessor.Infrastructure;
using CassandraMigrationProcessor.CassandraDriver;
using CassandraMigrationProcessor.Models;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace CassandraMigrationProcessor.DataTransfer.BulkCopy;
/// <summary>
/// Reads a single page from the source Cassandra cluster,
/// extracting row values for downstream writing.
/// </summary>
internal class PageReader : IDisposable
{
    private readonly WorkerLog _log;
    private readonly CancellationToken _ct;
    private readonly ISession _sourceSession;
    private readonly List<string> _columnNames;
    private readonly int _pageSize;
    private readonly int _maxReadRetries;

    private const int ReadTimeoutMs = 60_000;
    private const int RetryDelayMs = 5000;

    private PageReader(WorkerLog log,
        WorkerConfig config, int pageSize,
        int maxReadRetries,
        CancellationToken cancellationToken)
    {
        _log = log;
        _ct = cancellationToken;
        _columnNames = config.Columns.Select(c => c.Name).ToList();
        _pageSize = pageSize;
        _maxReadRetries = maxReadRetries;
        _sourceSession = CassandraClientFactory.CreateSourceSession(log.Inner, config.SourceConnection, config.Context.KeyspaceName);
    }

    /// <summary>
    /// Async factory. Creates the source session, then registers dynamic UDT
    /// mappings so UDT-typed columns decode into real CLR instances (instead
    /// of raw byte[]) and can be bound back into the target's prepared insert
    /// without serialization errors. Only the UDTs actually referenced by
    /// this table's columns are registered.
    /// </summary>
    public static async Task<PageReader> CreateAsync(WorkerLog log,
        WorkerConfig config, int pageSize, int maxReadRetries,
        CancellationToken cancellationToken)
    {
        var reader = new PageReader(log, config, pageSize, maxReadRetries, cancellationToken);
        try
        {
            var allUdts = await SchemaManager.GetUserDefinedTypesAsync(reader._sourceSession, config.Context.KeyspaceName);
            var requiredUdts = SchemaManager.FilterUdtsReferencedByTable(
                allUdts, config.Columns.Select(c => c.Type));
            await DynamicUdtRegistrar.RegisterAsync(reader._sourceSession, config.Context.KeyspaceName, requiredUdts);
        }
        catch (Exception ex)
        {
            log.WriteLine($"UDT mapping registration on source failed: {ex.Message}", LogType.Warning);
        }
        return reader;
    }

    public void Dispose() => MigrationUtilities.SafeDispose(_sourceSession, "PageReader source session");

    /// <summary>Result of a page read attempt. IsEmptyPage=true means
    /// rows.Count==0 (no new data this poll). Cosmos still returns a
    /// non-null PagingState which is stored on the partition as the
    /// forward anchor for the next poll.</summary>
    internal record ReadResult(List<object[]> Rows, WorkChunk WorkChunk, bool IsEmptyPage);

    /// <summary>
    /// Reads a single page, updates partition state and tracker.
    /// </summary>
    public async Task<ReadResult?>
        ReadAsync(Partition partition, PipelineContext ctx)
    {
        var stopwatch = Stopwatch.StartNew();
        var stmt = new SimpleStatement(BuildSelectCql(ctx.Worker.Context, partition.FeedRange));
        stmt.SetPageSize(_pageSize);
        stmt.SetAutoPage(false);
        stmt.SetReadTimeoutMillis(ReadTimeoutMs);
        stmt.SetConsistencyLevel(ConsistencyLevel.One);

        if (partition.LastPagingState != null)
            stmt.SetPagingState(partition.LastPagingState);

        RowSet? resultSet = null;
        for (int attempt = 1; attempt <= _maxReadRetries; attempt++)
        {
            try
            {
                resultSet = await _sourceSession.ExecuteAsync(stmt);
                break;
            }
            catch (System.Exception ex) when (attempt < _maxReadRetries
                && ExceptionClassifier.IsTransient(ex))
            {
                _log.WriteLine($"Read timeout (attempt {attempt}/{_maxReadRetries})",
                    LogType.Warning);
                await Task.Delay(attempt * RetryDelayMs, _ct);
            }
        }

        if (resultSet == null)
        {
            ctx.Counters.WorkerErrors.Add(TaskResult.Retry);
            return null;
        }

        byte[]? nextPaging = resultSet.PagingState;
        var rows = new List<object[]>();
        int available = resultSet.GetAvailableWithoutFetching();
        int consumed = 0;
        foreach (var row in resultSet)
        {
            if (consumed >= available) break;
            consumed++;
            var rowValues = new object[_columnNames.Count];
            for (int i = 0; i < _columnNames.Count; i++)
                rowValues[i] = row[_columnNames[i]];
            rows.Add(rowValues);
        }

        stopwatch.Stop();
        // Cosmos's change-feed query (FROM_START + feed range) always
        // returns a non-null PagingState — even on empty pages — so
        // "no more new rows right now" is signalled by rows.Count==0
        // alone. The same query reissued with the returned PagingState
        // returns either new events (when they arrive) or another empty
        // page. Bulk and replay differ only in what the worker does
        // with an empty page (transition vs cooldown vs complete).
        bool isEmptyPage = rows.Count == 0;

        // Update partition and tracker — caller doesn't need to.
        // In Replay phase, a partition is NEVER exhausted (the change
        // feed is open-ended); IsExhausted=true is reserved for the
        // bulk-phase drain trigger.
        ctx.Tracker.AddRead(rows.Count);
        ctx.Tracker.AddReadTime(stopwatch.ElapsedMilliseconds);
        var workChunk = partition.AddChunkAndTrim(nextPaging);
        bool markExhausted = isEmptyPage && partition.Phase == PartitionPhase.Bulk;
        partition.SetPageState(nextPaging, markExhausted);

        return new ReadResult(rows, workChunk, isEmptyPage);
    }

    internal static string BuildSelectCql(TableContext context, string range)
    {
        MigrationUtilities.ValidateCqlIdentifier(context.KeyspaceName);
        MigrationUtilities.ValidateCqlIdentifier(context.TableName);
        // range is a Cosmos DB feed range token (JSON), not a CQL identifier
        return
            $"SELECT * FROM \"{context.KeyspaceName}\".\"{context.TableName}\"" +
            $" WHERE COSMOS_CHANGEFEED_FROM_START() = true AND COSMOS_FEEDRANGE() = '{range}'";
    }
}
