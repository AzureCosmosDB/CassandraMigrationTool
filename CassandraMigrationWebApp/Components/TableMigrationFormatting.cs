using System.Globalization;
using CassandraMigrationProcessor.Models;

namespace CassandraMigrationWebApp.Components;

/// <summary>
/// Shared formatting helpers used by views that render
/// <see cref="TableMigration"/> metrics (e.g. JobReport.razor,
/// CollectionDetails.razor).
/// </summary>
internal static class TableMigrationFormatting
{
    /// <summary>
    /// Format the elapsed time between <paramref name="start"/> and
    /// <paramref name="end"/> as "Xh Ym Zs". Returns "N/A" when
    /// either timestamp is missing or <see cref="DateTime.MinValue"/>.
    /// </summary>
    public static string GetDuration(DateTime? start, DateTime? end)
    {
        if (!start.HasValue || !end.HasValue
            || start.Value == DateTime.MinValue
            || end.Value == DateTime.MinValue)
            return "N/A";
        var d = end.Value - start.Value;
        return $"{(int)d.TotalHours}h {d.Minutes}m {d.Seconds}s";
    }

    /// <summary>
    /// Returns <c>true</c> when the migration unit's effective target
    /// keyspace or table differs from the source — i.e. the user has
    /// configured a target-namespace mapping that the UI should surface.
    /// </summary>
    public static bool HasTargetNamespaceMapping(TableMigration mu)
    {
        return !string.Equals(mu.KeyspaceName, mu.GetEffectiveTargetKeyspaceName(), StringComparison.OrdinalIgnoreCase)
            || !string.Equals(mu.TableName, mu.GetEffectiveTargetTableName(), StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// Returns the effective "keyspace.table" target name for display.
    /// </summary>
    public static string GetTargetNamespace(TableMigration mu)
    {
        return $"{mu.GetEffectiveTargetKeyspaceName()}.{mu.GetEffectiveTargetTableName()}";
    }

    /// <summary>
    /// Formats a 64-bit integer with thousands separators using the
    /// invariant culture (so reports look the same regardless of the
    /// browser locale).
    /// </summary>
    public static string FormatLong(long value)
        => string.Format(CultureInfo.InvariantCulture, "{0:N0}", value);
}
