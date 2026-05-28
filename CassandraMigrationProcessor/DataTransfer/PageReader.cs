using Cassandra;
using CassandraMigrationProcessor.Infrastructure;
using CassandraMigrationProcessor.CassandraDriver;
using CassandraMigrationProcessor.Models;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace CassandraMigrationProcessor.DataTransfer;

/// <summary>
/// Tunable knobs for a single <see cref="PageReader"/>: how many rows
/// to pull per page and how many times to retry a transient read
/// failure. Carried as a record so the caller passes one capability
/// instead of two loose ints.
/// </summary>
internal record ReaderConfig(int PageSize, int MaxReadRetries);

/// <summary>
/// Reads a single page from the source Cassandra cluster. The reader's
/// source session is keyspace-agnostic; per-table state (columns,
/// identifiers, UDT registrations) is resolved from
/// <see cref="Partition"/> at read time. UDT registration is
/// cached per keyspace so the first partition for each table pays the
/// cost and subsequent partitions reuse it.
/// </summary>
internal class PageReader : IDisposable
{
    private readonly WorkerLog _log;
    private readonly CancellationToken _ct;
    private readonly ISession _sourceSession;
    private readonly int _pageSize;
    private readonly int _maxReadRetries;
    private readonly ConcurrentDictionary<string, Task> _udtRegistrations = new();

    private const int ReadTimeoutMs = 60_000;
    private const int RetryDelayMs = 5000;

    private PageReader(WorkerLog log, ISessionFactory sessionFactory, ReaderConfig config, CancellationToken cancellationToken)
    {
        _log = log;
        _ct = cancellationToken;
        _pageSize = config.PageSize;
        _maxReadRetries = config.MaxReadRetries;
        _sourceSession = sessionFactory.CreateSourceSession();
    }

    public static Task<PageReader> CreateAsync(WorkerLog log,
        ISessionFactory sessionFactory, ReaderConfig config,
        CancellationToken cancellationToken)
    {
        return Task.FromResult(new PageReader(log, sessionFactory, config, cancellationToken));
    }

    public void Dispose() => MigrationUtilities.SafeDisposeSession(_sourceSession, "PageReader source session");

    /// <summary>Lazy, idempotent UDT registration for a table's keyspace.</summary>
    private Task EnsureUdtsRegisteredAsync(Partition partition)
    {
        return _udtRegistrations.GetOrAdd(partition.Spec.KeyspaceName, async ks =>
        {
            try
            {
                var allUdts = await SchemaManager.GetUserDefinedTypesAsync(_sourceSession, ks);
                var requiredUdts = SchemaManager.FilterUdtsReferencedByTable(
                    allUdts, partition.Columns.Select(c => c.Type));
                await DynamicUdtRegistrar.RegisterAsync(_sourceSession, ks, requiredUdts);
            }
            catch (Exception ex)
            {
                // Do NOT swallow: UDT mapping is required for correct row
                // decoding. Surfacing as fatal aborts the worker via the
                // outer catch and stops the job before silently emitting
                // mis-shaped rows.
                _log.WriteLine($"FATAL: UDT mapping registration on source failed for {ks}: {ex.Message}", LogType.Error);
                throw;
            }
        });
    }

    internal record ReadResult(List<object[]> Rows, WorkChunk WorkChunk, bool IsEmptyPage);

    public async Task<ReadResult?> ReadAsync(Partition partition, PipelineContext ctx)
    {
        await EnsureUdtsRegisteredAsync(partition);

        var stopwatch = Stopwatch.StartNew();
        var stmt = new SimpleStatement(BuildSelectCql(partition.Spec, partition.FeedRange));
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
                resultSet = await _sourceSession.ExecuteAsync(stmt).WaitAsync(_ct);
                break;
            }
            catch (System.Exception ex) when (attempt < _maxReadRetries
                && ExceptionClassifier.IsTransient(ex))
            {
                _log.WriteLine($"Read timeout for {partition.TableId} (attempt {attempt}/{_maxReadRetries})",
                    LogType.Warning);
                await Task.Delay(attempt * RetryDelayMs, _ct);
            }
        }

        if (resultSet == null)
        {
            ctx.Flags.WorkerErrors.Add(TaskResult.Retry);
            return null;
        }

        byte[]? nextPaging = resultSet.PagingState;
        var columnNames = partition.Columns.Select(c => c.Name).ToList();
        var rows = new List<object[]>();
        int available = resultSet.GetAvailableWithoutFetching();
        int consumed = 0;
        foreach (var row in resultSet)
        {
            if (consumed >= available) break;
            consumed++;
            var rowValues = new object[columnNames.Count];
            for (int i = 0; i < columnNames.Count; i++)
                rowValues[i] = row[columnNames[i]];
            rows.Add(rowValues);
        }

        stopwatch.Stop();
        bool isEmptyPage = rows.Count == 0;

        partition.Tracker.AddRead(rows.Count);
        partition.Tracker.AddReadTime(stopwatch.ElapsedMilliseconds);
        var workChunk = partition.AddChunkAndTrim(nextPaging);
        bool markExhausted = isEmptyPage && partition.Phase == PartitionPhase.Bulk;
        partition.SetPageState(nextPaging, markExhausted);

        return new ReadResult(rows, workChunk, isEmptyPage);
    }

    internal static string BuildSelectCql(TableCopySpec context, string range)
    {
        MigrationUtilities.ValidateCqlIdentifier(context.KeyspaceName);
        MigrationUtilities.ValidateCqlIdentifier(context.TableName);
        return
            $"SELECT * FROM \"{context.KeyspaceName}\".\"{context.TableName}\"" +
            $" WHERE COSMOS_CHANGEFEED_FROM_START() = true AND COSMOS_FEEDRANGE() = '{range}'";
    }
}
