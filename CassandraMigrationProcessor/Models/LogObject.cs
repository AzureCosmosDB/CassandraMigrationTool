using System.Text.Json.Serialization;

namespace CassandraMigrationProcessor.Models;

/// <summary>Immutable log entry record: severity, message, and UTC timestamp.</summary>
public record LogObject(
    [property: JsonConverter(typeof(LogTypeConverter))] LogType Type,
    string Message)
{
    public DateTime Datetime { get; init; } = DateTime.UtcNow;
}
