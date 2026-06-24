namespace CassandraMigrationProcessor.Models;

/// <summary>
/// Mutable user-facing application configuration (page sizes, change-feed
/// poll interval, feed-range parallelism) with default-and-clamp semantics.
/// Persistence is delegated to <see cref="Context.SettingsManager"/>.
/// </summary>
public class AppSettings
{
    // Default values
    internal const int DefaultCqlCopyPageSize = 500;
    internal const int DefaultChangeFeedPollIntervalMs = 5000;
    internal const int DefaultLogPageSize = 5000;

    // Clamping bounds
    internal const int MinLogPageSize = 1000;
    internal const int MaxLogPageSize = 100000;
    internal const int MinCqlCopyPageSize = 1;
    internal const int MaxCqlCopyPageSize = 100000;
    internal const int MinChangeFeedPollIntervalMs = 100;
    internal const int MaxChangeFeedPollIntervalMs = 600_000;
    internal const int MinFeedRangeParallelism = 1;
    internal const int MaxFeedRangeParallelismCap = 4096;

    public int LogPageSize { get; set; }
    public int CqlCopyPageSize { get; set; }
    public int ChangeFeedPollIntervalMs { get; set; }
    public int MaxFeedRangeParallelism { get; set; }

    public AppSettings()
    {
    }

    /// <summary>
    /// Returns a shallow copy. The type only holds value-type fields,
    /// so MemberwiseClone is a complete and correct copy — and it
    /// avoids the JSON round-trip that the previous ICloneable
    /// implementation paid on every settings edit.
    /// </summary>
    public AppSettings Copy() => (AppSettings)MemberwiseClone();

    private static int DefaultOrValue(int loaded, int defaultVal)
        => loaded == 0 ? defaultVal : loaded;

    private static readonly int _defaultParallelism
        = Math.Max(4, Environment.ProcessorCount * 2);

    internal static int DefaultParallelism() => _defaultParallelism;

    /// <summary>
    /// Populate every setting from a freshly-constructed instance (i.e. all
    /// zeroes) — <see cref="ApplyLoaded"/> coerces 0 → default and then
    /// clamps, which is exactly the behaviour we want for "all defaults".
    /// </summary>
    internal void ApplyDefaults() => ApplyLoaded(new AppSettings());

    internal void ClampValues()
    {
        LogPageSize              = Clamp(LogPageSize,              MinLogPageSize,              MaxLogPageSize);
        CqlCopyPageSize          = Clamp(CqlCopyPageSize,          MinCqlCopyPageSize,          MaxCqlCopyPageSize);
        ChangeFeedPollIntervalMs = Clamp(ChangeFeedPollIntervalMs, MinChangeFeedPollIntervalMs, MaxChangeFeedPollIntervalMs);
        MaxFeedRangeParallelism  = Clamp(MaxFeedRangeParallelism,  MinFeedRangeParallelism,     MaxFeedRangeParallelismCap);
    }

    private static int Clamp(int value, int min, int max)
        => value < min ? min : (value > max ? max : value);

    internal void ApplyLoaded(AppSettings loaded)
    {
        CqlCopyPageSize = DefaultOrValue(loaded.CqlCopyPageSize, DefaultCqlCopyPageSize);
        LogPageSize = DefaultOrValue(loaded.LogPageSize, DefaultLogPageSize);
        ChangeFeedPollIntervalMs = DefaultOrValue(loaded.ChangeFeedPollIntervalMs, DefaultChangeFeedPollIntervalMs);
        MaxFeedRangeParallelism = DefaultOrValue(loaded.MaxFeedRangeParallelism, DefaultParallelism());
        ClampValues();
    }
}
