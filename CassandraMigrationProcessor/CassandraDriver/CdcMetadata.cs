namespace CassandraMigrationProcessor.CassandraDriver;

/// <summary>
/// Per-row change-feed system metadata extracted from a
/// <c>SELECT JSON *</c> response. The source surfaces these
/// system columns only when both <c>JSON</c> projection and a
/// change-feed clause (e.g. <c>COSMOS_CHANGEFEED_FROM_START()</c>)
/// are present on the query. Plain <c>SELECT *</c> over the change
/// feed strips them, which is why migrations historically lost TTL
/// and writetime on every replicated row.
/// </summary>
/// <remarks>
/// JSON shape (sampled from a live Cosmos source):
/// <code>
/// {
///   "id":"...",
///   "__sys_rw_ts":1780568659180854,
///   "__sys_clts":{"payload":1780568659180854},
///   "__sys_rw_ttl":[86400,0],
///   "__sys_clttl":[1780655059,{"payload":[86400,0]}],
///   "__sys_rw_tmbstn":null,
///   "__sys_clts_tmbstn":null
/// }
/// </code>
/// Field semantics:
/// <list type="bullet">
///   <item><c>__sys_rw_ts</c> — row-liveness marker writetime,
///         microseconds since Unix epoch. An UPDATE does not advance it,
///         so cells whose <c>__sys_clts</c> writetime differs are
///         replayed separately at their own timestamp
///         (<see cref="PerColumn"/>).</item>
///   <item><c>__sys_clts</c> — per-cell writetime map
///         <c>{col: micros | [per-element micros]}</c>.</item>
///   <item><c>__sys_rw_ttl[0]</c> identifies whether the row-liveness
///         marker itself has a TTL. When it is positive,
///         <c>__sys_clttl[0]</c> supplies its absolute expiry epoch.
///         When it is zero, the row marker is permanent even if
///         individual cells have TTLs.</item>
///   <item><c>__sys_clttl[0]</c> is the absolute cell-expiry base in
///         seconds. It is also the row-marker expiry only when
///         <c>__sys_rw_ttl[0]</c> is positive. The destination's
///         <c>USING TTL</c> takes a duration, so the writer computes
///         <c>remaining = expiry - now_epoch_seconds</c>.</item>
///   <item>Both <c>*ttl</c> arrays are <c>[0, 0]</c> when the source
///         row has no TTL — that's how we distinguish "no expiry"
///         from "already expired".</item>
/// </list>
/// </remarks>
internal sealed record CdcRowMetadata(
    long? WritetimeMicros,
    long? ExpiryEpochSeconds,
    IReadOnlyDictionary<string, CdcCellMetadata>? PerColumn = null,
    long? LatestCellWritetimeMicros = null)
{
    /// <summary>
    /// Timestamp for a single-statement whole-row write: the newest of the
    /// row-marker writetime (<c>__sys_rw_ts</c>) and every cell writetime
    /// (<c>__sys_clts</c>). Rows whose cell writetimes diverge are split
    /// (<see cref="HasPerColumnDivergence"/>), so on the single-statement
    /// path this normally equals <c>__sys_rw_ts</c>; it supplies the
    /// timestamp for UPDATE-only rows that have no row marker. <c>null</c>
    /// when the source exposed neither value.
    /// </summary>
    public long? RowWriteTimestampMicros =>
        (WritetimeMicros, LatestCellWritetimeMicros) switch
        {
            (long row, long cell) => Math.Max(row, cell),
            (long row, null) => row,
            (null, long cell) => cell,
            _ => null,
        };

    /// <summary>
    /// True when the source row has a row-liveness marker. A null
    /// <c>__sys_rw_ts</c> denotes an UPDATE-only row whose cells must be
    /// replayed without an INSERT, otherwise the destination would gain a
    /// marker that outlives its cells.
    /// </summary>
    public bool HasRowMarker => WritetimeMicros.HasValue;

    /// <summary>
    /// True when the source row carries per-column metadata that diverges
    /// from the row-level values: writetime and TTL with cell-level
    /// preservation, writetime only (with the row TTL) in row-level mode.
    /// When false the writer takes the single-statement fast
    /// path (one <c>INSERT … JSON ? USING TIMESTAMP ? AND TTL ?</c>);
    /// when true it restores the row-level liveness marker with one
    /// partial INSERT and applies divergent cells with UPDATE statements
    /// so each cell lands with its own timestamp without replacing
    /// that marker. Writing such a row at one timestamp would either tie
    /// an UPDATE-changed cell with its stale copy on the destination or
    /// lift untouched cells above concurrent changes. <see cref="PerColumn"/>
    /// is populated only for the columns whose metadata differs; every
    /// other column remains in the row-level INSERT.
    /// </summary>
    public bool HasPerColumnDivergence => PerColumn is { Count: > 0 };

    /// <summary>
    /// Sentinel used to mark rows that have no TTL on the source.
    /// Distinct from "expired" so the writer can omit <c>USING TTL</c>
    /// entirely instead of writing with a positive (and therefore
    /// incorrect) duration.
    /// </summary>
    public bool HasTtl => ExpiryEpochSeconds.HasValue;

    /// <summary>
    /// True iff the source row's expiry has already elapsed by
    /// <paramref name="nowEpochSeconds"/>. Callers in the bulk-copy
    /// phase typically skip such rows; replay-phase callers write
    /// them with <c>TTL 1</c> + source <c>USING TIMESTAMP</c> so the
    /// destination converges on the expired state (see Option B
    /// discussion in the PR description).
    /// </summary>
    public bool IsExpiredAt(long nowEpochSeconds) =>
        HasTtl && ExpiryEpochSeconds!.Value <= nowEpochSeconds;

    /// <summary>
    /// Duration (seconds) to bind into the destination INSERT's
    /// <c>USING TTL ?</c>. Returns <c>null</c> when the source row had
    /// no TTL (caller should bind <c>0</c> to mean "no TTL" or, if the
    /// prepared statement was built without the TTL slot, skip
    /// binding). For expired rows the caller chooses between skipping
    /// (bulk) and clamping to 1 (replay) — this method just reports
    /// the raw signed remainder so the caller's intent is explicit.
    /// </summary>
    public long? ComputeRemainingTtlSeconds(long nowEpochSeconds)
    {
        if (!HasTtl) return null;
        return ExpiryEpochSeconds!.Value - nowEpochSeconds;
    }
}

/// <summary>
/// Per-column (per-cell) change-feed metadata decoded from the source's
/// <c>__sys_clts</c> (writetime) and <c>__sys_clttl[1]</c> (per-column TTL)
/// maps. Populated only when the row actually diverges from its row-level
/// metadata; in row-level mode only writetime is compared and every cell
/// carries the row-level expiry.
/// </summary>
/// <remarks>
/// Non-frozen collection columns expose <em>per-element</em> writetime/TTL
/// (arrays rather than scalars in those maps). CQL cannot re-apply that
/// metadata per element, so they use the cell-expiry base as a
/// whole-column TTL approximation and are replayed through UPDATE.
/// </remarks>
/// <param name="WritetimeMicros">Cell writetime in microseconds since the
/// Unix epoch (from <c>__sys_clts[col]</c>), or <c>null</c> to inherit the
/// row writetime.</param>
/// <param name="ExpiryEpochSeconds">Absolute expiry epoch in seconds for
/// the cell (<c>__sys_clttl[0] + offset</c>), or <c>null</c> when the cell
/// has no TTL.</param>
internal readonly record struct CdcCellMetadata(
    long? WritetimeMicros,
    long? ExpiryEpochSeconds);
