using System;
using System.Text.Json.Serialization;

namespace CassandraMigrationProcessor.Models;
public record LogObject(
    [property: JsonConverter(typeof(LogTypeConverter))] LogType Type,
    string Message)
{
    public DateTime Datetime { get; init; } = DateTime.UtcNow;
}
