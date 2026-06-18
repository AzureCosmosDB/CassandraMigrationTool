using Newtonsoft.Json;

namespace CassandraMigrationProcessor.Models;
/// <summary>
/// Log type enumeration with severity levels (lower = more severe).
/// Numeric value 1 is intentionally left as a gap to reserve room for a
/// future "Critical" level between <see cref="Error"/> and
/// <see cref="Warning"/> without renumbering existing persisted values.
/// </summary>
public enum LogType
{
    /// <summary>Error messages only.</summary>
    Error = 0,

    // 1 reserved for a future Critical level.

    /// <summary>Warning messages.</summary>
    Warning = 2,

    /// <summary>Informational messages (includes errors and warnings).</summary>
    Info = 3,

    /// <summary>Debug messages (includes errors, warnings, and info).</summary>
    Debug = 4,

    /// <summary>Verbose/detailed messages (includes all).</summary>
    Verbose = 5
}

/// <summary>
/// Newtonsoft.Json converter for <see cref="LogType"/>. Serialises as the
/// member name; on read accepts either a string member name or its
/// numeric value, and throws on unrecognised input rather than silently
/// resetting to <see cref="LogType.Info"/> — a corrupt config file
/// should fail loudly so the operator notices, not silently downgrade
/// the log level.
/// </summary>
public class LogTypeConverter : JsonConverter<LogType>
{
    public override LogType ReadJson(
        JsonReader reader,
        Type objectType,
        LogType existingValue,
        bool hasExistingValue,
        JsonSerializer serializer)
    {
        if (reader.TokenType == JsonToken.String)
        {
            var raw = (string?)reader.Value;
            if (!string.IsNullOrWhiteSpace(raw) &&
                Enum.TryParse<LogType>(raw, ignoreCase: true, out var parsed) &&
                Enum.IsDefined(typeof(LogType), parsed))
                return parsed;
            throw new JsonSerializationException(
                $"Unrecognised LogType value '{raw}'. Expected one of: " +
                string.Join(", ", Enum.GetNames(typeof(LogType))) + ".");
        }
        if (reader.TokenType == JsonToken.Integer)
        {
            int n = Convert.ToInt32(reader.Value);
            if (Enum.IsDefined(typeof(LogType), n))
                return (LogType)n;
            throw new JsonSerializationException(
                $"Unrecognised LogType numeric value {n}. Defined values: " +
                string.Join(", ", Enum.GetValues(typeof(LogType))
                    .Cast<int>()) + ".");
        }
        if (reader.TokenType == JsonToken.Null)
            return LogType.Info;
        throw new JsonSerializationException(
            $"Unexpected token {reader.TokenType} when parsing LogType.");
    }

    public override void WriteJson(JsonWriter writer, LogType value, JsonSerializer serializer)
    {
        writer.WriteValue(value.ToString());
    }
}
