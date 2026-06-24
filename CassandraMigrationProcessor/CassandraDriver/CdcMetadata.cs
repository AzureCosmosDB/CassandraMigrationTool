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
///   <item><c>__sys_rw_ts</c> — row writetime, microseconds since
///         Unix epoch. Suitable for <c>USING TIMESTAMP</c> on the
///         destination INSERT so LWW conflict-resolution matches the
///         source.</item>
///   <item><c>__sys_clttl[0]</c> — absolute expiry epoch in seconds
///         (not a duration). The destination's <c>USING TTL</c> takes
///         a duration, so we compute
///         <c>remaining = expiry - now_epoch_seconds</c> at write
///         time.</item>
///   <item><c>__sys_rw_ttl[0]</c> — original TTL duration in seconds
///         as declared at write time on the source. We do NOT use this
///         for the destination TTL because re-applying the original
///         duration would silently extend the row's lifetime past its
///         intended source expiry.</item>
///   <item>Both <c>*ttl</c> arrays are <c>[0, 0]</c> when the source
///         row has no TTL — that's how we distinguish "no expiry"
///         from "already expired".</item>
/// </list>
/// </remarks>
internal sealed record CdcRowMetadata(
    long? WritetimeMicros,
    long? ExpiryEpochSeconds)
{
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
