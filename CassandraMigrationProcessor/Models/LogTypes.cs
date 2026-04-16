using System;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace CassandraMigrationProcessor.Models;
/// <summary>
/// Log type enumeration with severity levels (lower = more severe)
/// </summary>
public enum LogType
{
    /// <summary>Error messages only</summary>
    Error = 0,

    /// <summary>Warning messages</summary>
    Warning = 2,

    /// <summary>Informational messages (includes errors and warnings)</summary>
    Info = 3,

    /// <summary>Debug messages (includes errors, warnings, and info)</summary>
    Debug = 4,

    /// <summary>Verbose/detailed messages (includes all)</summary>
    Verbose = 5
}

/// <summary>
/// Custom JSON converter for LogType enum.
/// </summary>
public class LogTypeConverter : JsonConverter<LogType>
{
    public override LogType Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
    {
        if (reader.TokenType == JsonTokenType.String)
        {
            string? value = reader.GetString();
            if (Enum.TryParse<LogType>(value, true, out var result))
                return result;
            return LogType.Info;
        }
        else if (reader.TokenType == JsonTokenType.Number)
        {
            int numValue = reader.GetInt32();
            if (Enum.IsDefined(typeof(LogType), numValue))
                return (LogType)numValue;
            return LogType.Info;
        }

        return LogType.Info;
    }

    public override void Write(Utf8JsonWriter writer, LogType value, JsonSerializerOptions options)
    {
        writer.WriteStringValue(value.ToString());
    }
}
