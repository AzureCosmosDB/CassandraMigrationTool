using Newtonsoft.Json;

namespace CassandraMigrationProcessor.Models;

/// <summary>
/// Mutable user-facing application configuration (page sizes, change-feed
/// poll interval, feed-range parallelism) with default-and-clamp semantics.
/// Persistence is delegated to <see cref="Context.SettingsManager"/>.
/// </summary>
public class AppSettings : ICloneable
{
    // Default values
    internal const int DefaultCqlCopyPageSize = 500;
    internal const int DefaultChangeFeedPollIntervalMs = 5000;
    internal const int DefaultLogPageSize = 5000;

    // Clamping bounds
    internal const int MinLogPageSize = 1000;
    internal const int MaxLogPageSize = 100000;

    public int LogPageSize { get; set; }
    public int CqlCopyPageSize { get; set; }
    public int ChangeFeedPollIntervalMs { get; set; }
    public int MaxFeedRangeParallelism { get; set; }

    public AppSettings()
    {
    }

    public object Clone()
    {
        var json = JsonConvert.SerializeObject(this);
        return JsonConvert.DeserializeObject<AppSettings>(json)
            ?? new AppSettings();
    }

    private static int DefaultOrValue(int loaded, int defaultVal)
        => loaded == 0 ? defaultVal : loaded;

    internal static int DefaultParallelism()
        => Math.Max(4, Environment.ProcessorCount * 2);

    internal void ApplyDefaults()
    {
        CqlCopyPageSize = DefaultCqlCopyPageSize;
        ChangeFeedPollIntervalMs = DefaultChangeFeedPollIntervalMs;
        MaxFeedRangeParallelism = DefaultParallelism();
        LogPageSize = DefaultLogPageSize;
        ClampValues();
    }

    internal void ClampValues()
    {
        if (LogPageSize < MinLogPageSize)
            LogPageSize = MinLogPageSize;
        if (LogPageSize > MaxLogPageSize)
            LogPageSize = MaxLogPageSize;
    }

    internal void ApplyLoaded(AppSettings loaded)
    {
        CqlCopyPageSize = DefaultOrValue(loaded.CqlCopyPageSize, DefaultCqlCopyPageSize);
        LogPageSize = DefaultOrValue(loaded.LogPageSize, DefaultLogPageSize);
        ChangeFeedPollIntervalMs = DefaultOrValue(loaded.ChangeFeedPollIntervalMs, DefaultChangeFeedPollIntervalMs);
        MaxFeedRangeParallelism = DefaultOrValue(loaded.MaxFeedRangeParallelism, DefaultParallelism());
        ClampValues();
    }
}
