namespace CassandraMigrationProcessor.CassandraDriver;

/// <summary>
/// Per-row change-feed system metadata extracted from a
/// <c>SELECT JSON *</c> response. The source surfaces these system columns
/// only when both <c>JSON</c> projection and a change-feed clause are present
/// on the query.
/// </summary>
/// <remarks>
/// JSON shape (sampled from a live source):
/// <code>
/// {
///   "id":"...", "a":"x", "b":"y", "l":[1,2],
///   "__sys_rw_ts":1780568659180854,
///   "__sys_rw_ttl":[4000,0],
///   "__sys_clts":{"a":1780568659180854,"b":1780568660000000,"l":[t1,t2],"u":{"f":t}},
///   "__sys_clttl":[1780572659,{"a":[4000,0],"b":[0,0],"l":[[0,0],[300,-3700]],"u":{"f":[0,0]}}]
/// }
/// </code>
/// <list type="bullet">
///   <item><c>__sys_rw_ts</c> — row-liveness (primary-key) marker writetime in
///         microseconds. <c>null</c> for rows created only by UPDATE.</item>
///   <item><c>__sys_rw_ttl</c> — <c>[duration, offset]</c> of the marker;
///         <c>null</c> when there is no marker.</item>
///   <item><c>__sys_clts</c> — per-cell writetime: a number for simple and
///         frozen columns, an element array for non-frozen collections, and a
///         field object for non-frozen UDTs.</item>
///   <item><c>__sys_clttl</c> — <c>[base, detail]</c>; every
///         <c>[duration, offset]</c> pair in <c>detail</c> (mirroring the
///         <c>__sys_clts</c> shape) expires at <c>base + offset</c> when
///         <c>duration &gt; 0</c>. A zero duration means "no TTL".</item>
///   <item><c>__sys_clts_tmbstn</c> — per-column deletion timestamps: a
///         number for simple and frozen columns, and
///         <c>[collectionDeletion, {element: micros}]</c> for non-frozen
///         collections.</item>
/// </list>
/// </remarks>
/// <param name="WritetimeMicros">Row-marker writetime, or <c>null</c> when the
/// source row has no marker.</param>
/// <param name="ExpiryEpochSeconds">Row-marker absolute expiry, or <c>null</c>
/// when the marker has no TTL (or there is no marker).</param>
/// <param name="PerColumn">Columns whose writetime/expiry differ from the
/// marker, or whose elements/fields carry differing expiries. Every live
/// column is listed when the row has no marker.</param>
/// <param name="LatestCellWritetimeMicros">Newest cell writetime in the row.</param>
/// <param name="Deletions">Newest source deletion per column, decoded from
/// <c>__sys_clts_tmbstn</c>; <c>null</c> when no column carries one.</param>
internal sealed record CdcRowMetadata(
    long? WritetimeMicros,
    long? ExpiryEpochSeconds,
    IReadOnlyDictionary<string, CdcCellMetadata>? PerColumn = null,
    long? LatestCellWritetimeMicros = null,
    IReadOnlyDictionary<string, CdcCellDeletion>? Deletions = null)
{
    /// <summary>
    /// Newest of the marker writetime and every cell writetime; <c>null</c>
    /// when the source exposed neither.
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
    /// True when the source row has a row-liveness marker. Rows without one
    /// are replayed through UPDATE only, so the destination does not gain a
    /// marker that would keep an otherwise empty row alive.
    /// </summary>
    public bool HasRowMarker => WritetimeMicros.HasValue;

    /// <summary>True when at least one column diverges from the marker.</summary>
    public bool HasPerColumnDivergence => PerColumn is { Count: > 0 };

    /// <summary>True when the row marker carries a TTL.</summary>
    public bool HasTtl => ExpiryEpochSeconds.HasValue;

    public bool IsExpiredAt(long nowEpochSeconds) =>
        HasTtl && ExpiryEpochSeconds!.Value <= nowEpochSeconds;

    public long? ComputeRemainingTtlSeconds(long nowEpochSeconds)
    {
        if (!HasTtl) return null;
        return ExpiryEpochSeconds!.Value - nowEpochSeconds;
    }
}

/// <summary>
/// Per-column change-feed metadata decoded from <c>__sys_clts</c> and
/// <c>__sys_clttl</c>.
/// </summary>
/// <param name="WritetimeMicros">Cell writetime; the newest element/field
/// writetime for non-frozen collections and UDTs.</param>
/// <param name="ExpiryEpochSeconds">Absolute expiry shared by the whole
/// column, or <c>null</c> for no TTL. When elements/fields have differing
/// expiries this is the longest one (<c>null</c> if any is immortal) and is
/// only used as a fallback.</param>
/// <param name="ElementExpiries">Per-element expiries, in value order, when a
/// non-frozen list/set/map has elements with differing expiries or
/// writetimes.</param>
/// <param name="FieldExpiries">Per-field expiries (bare field names, fields
/// with a cell only) when a non-frozen UDT has fields with differing
/// expiries or writetimes.</param>
/// <param name="ElementWritetimes">Per-element writetimes matching
/// <paramref name="ElementExpiries"/>; <c>null</c> means every element was
/// written at <paramref name="WritetimeMicros"/>.</param>
/// <param name="FieldWritetimes">Per-field writetimes matching
/// <paramref name="FieldExpiries"/>; <c>null</c> means every field was
/// written at <paramref name="WritetimeMicros"/>.</param>
internal readonly record struct CdcCellMetadata(
    long? WritetimeMicros,
    long? ExpiryEpochSeconds,
    IReadOnlyList<long?>? ElementExpiries = null,
    IReadOnlyDictionary<string, long?>? FieldExpiries = null,
    IReadOnlyList<long?>? ElementWritetimes = null,
    IReadOnlyDictionary<string, long?>? FieldWritetimes = null)
{
    /// <summary>
    /// True when elements or fields cannot all be written with one
    /// writetime and TTL.
    /// </summary>
    public bool HasPerElementMetadata =>
        ElementExpiries is not null || FieldExpiries is not null;
}

/// <summary>
/// Newest deletion recorded by the source for one column.
/// </summary>
/// <param name="DeletedAtMicros">Deletion timestamp; for multi-cell columns
/// the newest of the collection deletion and every element/field deletion.</param>
/// <param name="IsComplex">True for a non-frozen collection or UDT, whose
/// deletions cover individual elements/fields rather than a single cell.</param>
internal readonly record struct CdcCellDeletion(long DeletedAtMicros, bool IsComplex)
{
    /// <summary>
    /// Smallest write timestamp whose implicit overwrite covers this
    /// deletion: a simple cell is deleted by a tombstone at the same
    /// timestamp, while assigning a multi-cell column deletes its previous
    /// contents at <c>timestamp - 1</c>.
    /// </summary>
    public long CoveringWriteMicros => IsComplex ? DeletedAtMicros + 1 : DeletedAtMicros;
}
