using Cassandra;
using CassandraMigrationProcessor.Infrastructure;
using CassandraMigrationProcessor.CassandraDriver;
using CassandraMigrationProcessor.Models;
using System.Collections.Concurrent;
using System.Diagnostics;

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

    /// <summary>
    /// Most recent transient exception observed during retry-exhausted
    /// page reads. Workers may attach this as the inner exception when
    /// promoting a per-partition exhaustion to a job-wide fatal.
    /// </summary>
    internal Exception? LastRetryExhaustionException { get; private set; }

    private const int ReadTimeoutMs = 60_000;

    // Upper bound on the per-attempt sleep. Server-provided
    // RetryAfterMs is honoured; clamp to protect against malformed
    // hints parking a worker for minutes.
    private const int MaxRetryDelayMs = 30_000;

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
        return _udtRegistrations.GetOrAdd(partition.Table.Spec.KeyspaceName, async ks =>
        {
            try
            {
                var allUdts = await SchemaManager.GetUserDefinedTypesAsync(_sourceSession, ks);
                var requiredUdts = SchemaManager.FilterUdtsReferencedByTable(
                    allUdts, partition.Table.Columns.Select(c => c.Type));
                await DynamicUdtRegistrar.RegisterAsync(_sourceSession, ks, requiredUdts);
            }
            catch (Exception ex)
            {
                // Do NOT swallow: UDT mapping is required for correct
                // row decoding. Surface as fatal.
                _log.WriteLine($"FATAL: UDT mapping registration on source failed for {ks}: {ex.Message}", LogType.Error);
                throw;
            }
        });
    }

    internal record ReadResult(List<object[]> Rows, WorkChunk WorkChunk, bool IsEmptyPage);

    public async Task<ReadResult?> ReadAsync(Partition partition)
    {
        await EnsureUdtsRegisteredAsync(partition);

        var stopwatch = Stopwatch.StartNew();
        var stmt = new SimpleStatement(BuildSelectCql(partition.Table.Spec, partition.FeedRange));
        stmt.SetPageSize(_pageSize);
        stmt.SetAutoPage(false);
        stmt.SetReadTimeoutMillis(ReadTimeoutMs);
        stmt.SetConsistencyLevel(ConsistencyLevel.One);

        if (partition.LastPagingState is { Length: > 0 })
            stmt.SetPagingState(partition.LastPagingState);

        RowSet? resultSet = null;
        for (int attempt = 1; attempt <= _maxReadRetries; attempt++)
        {
            try
            {
                resultSet = await _sourceSession.ExecuteAsync(stmt).WaitAsync(_ct);
                break;
            }
            catch (Exception ex) when (ExceptionClassifier.IsTransient(ex))
            {
                LastRetryExhaustionException = ex;
                _log.WriteLine(
                    $"Read attempt {attempt}/{_maxReadRetries} on {partition.Table.FullTableName} failed: " +
                    $"{ex.GetType().Name}: {ex.Message}",
                    LogType.Warning);

                if (attempt >= _maxReadRetries)
                {
                    // Final attempt failed on a transient/throttle.
                    // Surface null so the worker re-queues this
                    // partition via cooldown — LastPagingState is
                    // intact and will retry the same page once the
                    // source stops throttling.
                    break;
                }

                // Honour server's RetryAfterMs hint when present;
                // clamp to MaxRetryDelayMs.
                int delayMs = Math.Min(
                    ExceptionClassifier.GetRetryDelayMs(ex, attempt),
                    MaxRetryDelayMs);
                await Task.Delay(delayMs, _ct);
            }
        }

        if (resultSet == null)
        {
            // Read exhausted retries; worker re-queues via cooldown.
            return null;
        }

        byte[]? nextPaging = resultSet.PagingState;
        var columnNames = partition.Table.Columns.Select(c => c.Name).ToList();
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

        partition.Table.Tracker.AddReadTime(stopwatch.ElapsedMilliseconds);
        var workChunk = partition.AddChunkAndTrim(nextPaging);
        partition.SetLastPagingState(nextPaging);

        if (isEmptyPage)
        {
            // Cosmos change-feed tip-of-stream / 304. Debug log so
            // operators can confirm the replay loop is alive.
            bool hasAnchor = partition.LastPagingState is { Length: > 0 };
            _log.WriteLine(
                $"Empty page (304) on {partition.Table.FullTableName}/{partition.FeedRange} " +
                $"phase={partition.Phase} anchor={(hasAnchor ? "preserved" : "none")} " +
                $"read={stopwatch.ElapsedMilliseconds}ms",
                LogType.Debug);
        }

        return new ReadResult(rows, workChunk, isEmptyPage);
    }

    private static string BuildSelectCql(TableCopySpec context, string range)
    {
        CqlIdentifier.Validate(context.KeyspaceName);
        CqlIdentifier.Validate(context.TableName);
        // Defensive: feed-range tokens currently come from
        // system_cosmos.feedranges (server-managed, never contain a
        // single quote), but the column is plain text — escape per
        // CQL string-literal rules (double up apostrophes) so a future
        // schema change in that system table cannot turn the
        // interpolation into invalid CQL or an injection surface.
        var escapedRange = range.Replace("'", "''");
        return
            $"SELECT * FROM \"{context.KeyspaceName}\".\"{context.TableName}\"" +
            $" WHERE COSMOS_CHANGEFEED_FROM_START() = true AND COSMOS_FEEDRANGE() = '{escapedRange}'";
    }
}
