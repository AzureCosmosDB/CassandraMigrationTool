using Cassandra;
using CassandraMigrationProcessor.Infrastructure;
using CassandraMigrationProcessor.CassandraDriver;
using CassandraMigrationProcessor.Models;
using System.Diagnostics;

namespace CassandraMigrationProcessor.DataTransfer;

/// <summary>
/// Tunable knobs for a single <see cref="PageReader"/>: how many rows
/// to pull per page and how many times to retry a transient read
/// failure. Regular tables use <c>SELECT JSON *</c> when
/// <see cref="ReaderConfig.UseJsonCopy"/> is enabled so per-row system
/// metadata (writetime + per-row TTL) is surfaced to the writer; fast
/// binary mode and counter tables use typed <c>SELECT *</c> reads.
/// </summary>
internal record ReaderConfig(int PageSize, int MaxReadRetries, bool PreserveCellTtlAndWritetime = false, bool UseJsonCopy = true);

/// <summary>
/// Reads a single page from the source Cassandra cluster. The reader's
/// source session is keyspace-agnostic; per-table state (columns,
/// identifiers, UDT registrations) is resolved from
/// <see cref="Partition"/> at read time. UDT registration is
/// cached per keyspace so the first partition for each table pays the
/// cost and subsequent partitions reuse it.
/// </summary>
internal class PageReader
{
    private readonly WorkerLog _log;
    private readonly CancellationToken _ct;
    private readonly SourceSessionWrapper _sourceSession;
    private readonly int _pageSize;
    private readonly int _maxReadRetries;
    private readonly bool _preserveCellTtl;
    private readonly bool _useJsonCopy;

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

    private PageReader(
        WorkerLog log,
        SourceSessionWrapper sourceSession,
        ReaderConfig config,
        CancellationToken cancellationToken)
    {
        _log = log;
        _ct = cancellationToken;
        _pageSize = config.PageSize;
        _maxReadRetries = config.MaxReadRetries;
        _preserveCellTtl = config.PreserveCellTtlAndWritetime;
        _useJsonCopy = config.UseJsonCopy;
        _sourceSession = sourceSession
            ?? throw new ArgumentNullException(nameof(sourceSession));
    }

    public static Task<PageReader> CreateAsync(WorkerLog log,
        SourceSessionWrapper sourceSession,
        ReaderConfig config,
        CancellationToken cancellationToken)
    {
        return Task.FromResult(new PageReader(
            log,
            sourceSession,
            config,
            cancellationToken));
    }

    /// <summary>
    /// One page of source rows together with the chunk and per-row
    /// CDC metadata (writetime + TTL expiry). Exactly one of
    /// <see cref="Rows"/> and <see cref="JsonRows"/> is non-empty:
    /// counter tables and fast binary jobs use the typed
    /// <see cref="Rows"/> path; metadata-preserving jobs use
    /// <see cref="JsonRows"/> so the writer can re-INSERT each row via
    /// <c>INSERT JSON</c>. <see cref="Metadata"/> is index-aligned with
    /// the populated row list (or <c>null</c> when unavailable).
    /// </summary>
    internal record ReadResult(
        List<object[]> Rows,
        IReadOnlyList<string>? JsonRows,
        IReadOnlyList<CdcRowMetadata?>? Metadata,
        WorkChunk WorkChunk,
        bool IsEmptyPage);

    /// <summary>
    /// Read one page from <paramref name="partition"/>. Dispatches to the
    /// typed binary path when the table is a counter table (counters cannot
    /// honour <c>USING TIMESTAMP/TTL</c> and use UPDATE) or when the job
    /// selected the non-JSON fast copy path (<see cref="ReaderConfig.UseJsonCopy"/>
    /// is false). Otherwise uses the JSON metadata-preserving path.
    /// </summary>
    public Task<ReadResult?> ReadAsync(Partition partition)
        => partition.Table.IsCounterTable || !_useJsonCopy
            ? ReadTypedPageAsync(partition)
            : ReadJsonPageAsync(partition);

    /// <summary>
    /// Read a page via <c>SELECT JSON *</c>. The destination writer
    /// will re-INSERT each row via <c>INSERT JSON</c> and let the
    /// destination server handle type coercion, so UDT, tuple,
    /// decimal, varint, duration, and nested collections all preserve
    /// TTL/writetime end-to-end.
    /// </summary>
    private async Task<ReadResult?> ReadJsonPageAsync(Partition partition)
    {
        var stopwatch = Stopwatch.StartNew();
        var (resultSet, elapsed) = await ExecutePageAsync(partition, useJson: true, stopwatch);
        if (resultSet == null) return null;

        byte[]? nextPaging = resultSet.PagingState;
        int available = resultSet.GetAvailableWithoutFetching();
        var jsonRows = new List<string>(available);
        var metadata = new List<CdcRowMetadata?>(available);
        // One reusable parser per page: its internal buffers are reset (not
        // reallocated) per row, so a page of N rows amortizes the parser's
        // allocations across the whole page instead of paying them per row.
        var parser = new CdcJsonRowParser(_preserveCellTtl);
        int consumed = 0;
        foreach (var row in resultSet)
        {
            if (consumed >= available) break;
            consumed++;
            // SELECT JSON returns a single synthetic column literally
            // named "[json]" — see CASSANDRA-7970.
            var json = row.GetValue<string>("[json]");
            var (cleaned, meta) = parser.Parse(json);
            jsonRows.Add(cleaned);
            metadata.Add(meta);
        }

        return FinalizePage(partition, nextPaging, rows: new List<object[]>(),
            jsonRows: jsonRows, metadata: metadata, elapsed);
    }

    /// <summary>
    /// Read a page via the typed binary <c>SELECT *</c> path. Used for
    /// counter tables and for regular tables when the job selected the
    /// non-JSON fast copy path; the writer materializes rows via typed
    /// prepared INSERT (regular) or counter UPDATE (counters).
    /// </summary>
    private async Task<ReadResult?> ReadTypedPageAsync(Partition partition)
    {
        var stopwatch = Stopwatch.StartNew();
        var (resultSet, elapsed) = await ExecutePageAsync(partition, useJson: false, stopwatch);
        if (resultSet == null) return null;

        byte[]? nextPaging = resultSet.PagingState;
        int available = resultSet.GetAvailableWithoutFetching();
        var columnNames = partition.Table.Columns.Select(c => c.Name).ToList();
        var rows = new List<object[]>(available);
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

        return FinalizePage(partition, nextPaging, rows: rows,
            jsonRows: null, metadata: null, elapsed);
    }

    private async Task<(RowSet? ResultSet, Stopwatch Stopwatch)> ExecutePageAsync(
        Partition partition, bool useJson, Stopwatch stopwatch)
    {
        var stmt = new SimpleStatement(BuildSelectCql(partition.Table.Spec, partition.FeedRange, useJson));
        stmt.SetPageSize(_pageSize);
        stmt.SetAutoPage(false);
        stmt.SetReadTimeoutMillis(ReadTimeoutMs);
        stmt.SetConsistencyLevel(ConsistencyLevel.One);

        if (partition.LastPagingState is { Length: > 0 })
            stmt.SetPagingState(partition.LastPagingState);

        // Retry exhaustion surfaces as a null RowSet so the worker can
        // re-queue this partition via cooldown — LastPagingState is
        // intact and will retry the same page once the source stops
        // throttling.
        var resultSet = await RetryExecutor.ExecuteOrDefaultAsync<RowSet>(
            operation: async _ =>
            {
                var sourceSession = useJson
                    ? _sourceSession.GetSession()
                    : await _sourceSession.GetTypedSessionAsync(
                        partition.Table.Spec.KeyspaceName).ConfigureAwait(false);
                return await sourceSession.ExecuteAsync(stmt)
                    .WaitAsync(_ct)
                    .ConfigureAwait(false);
            },
            maxAttempts: _maxReadRetries,
            shouldRetry: ex => ex is not SourceUdtRegistrationException
                && ExceptionClassifier.IsTransient(ex),
            delayFor: (ex, attempt) => TimeSpan.FromMilliseconds(
                Math.Min(ExceptionClassifier.GetRetryDelayMs(ex, attempt), MaxRetryDelayMs)),
            onRetry: (ex, attempt) =>
            {
                LastRetryExhaustionException = ex;
                _log.WriteLine(
                    $"Read attempt {attempt}/{_maxReadRetries} on {partition.Table.FullTableName} [{partition.FeedRange}] failed: " +
                    $"{ex.GetType().Name}: {ex.Message}",
                    LogType.Warning);
            },
            cancellationToken: _ct);

        return (resultSet, stopwatch);
    }

    private ReadResult FinalizePage(Partition partition, byte[]? nextPaging,
        List<object[]> rows, IReadOnlyList<string>? jsonRows,
        IReadOnlyList<CdcRowMetadata?>? metadata, Stopwatch stopwatch)
    {
        stopwatch.Stop();
        int rowCount = jsonRows?.Count ?? rows.Count;
        bool isEmptyPage = rowCount == 0;

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

        return new ReadResult(rows, jsonRows, metadata, workChunk, isEmptyPage);
    }

    private static string BuildSelectCql(TableCopySpec context, string range, bool useJson)
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
        // SELECT JSON is required to surface per-row TTL/writetime
        // metadata on changefeed responses; plain SELECT * strips
        // the synthetic __sys_* columns the writer needs to honour
        // USING TIMESTAMP/USING TTL.
        var projection = useJson ? "SELECT JSON *" : "SELECT *";
        return
            $"{projection} FROM \"{context.KeyspaceName}\".\"{context.TableName}\"" +
            $" WHERE COSMOS_CHANGEFEED_FROM_START() = true AND COSMOS_FEEDRANGE() = '{escapedRange}'";
    }
}
