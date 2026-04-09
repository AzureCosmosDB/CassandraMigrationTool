using System;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace CassandraMigrationProcessor.Models
{
    /// <summary>
    /// Log type enumeration with severity levels (lower = more severe)
    /// </summary>
    public enum LogType
    {
        /// <summary>Error messages only</summary>
        Error = 0,

        /// <summary>
        /// [DEPRECATED] Use Info instead. Kept for backward compatibility with old log files.
        /// Will be removed in a future version.
        /// </summary>
        [Obsolete("Use Info instead. This value is kept only for backward compatibility with old log files.")]
        Message = 1,

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
    /// Custom JSON converter for LogType enum that handles backward compatibility.
    /// Message enum value is deprecated but kept for old log files.
    /// </summary>
    public class LogTypeConverter : JsonConverter<LogType>
    {
        public override LogType Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
        {
            if (reader.TokenType == JsonTokenType.String)
            {
                string? value = reader.GetString();

                // Try to parse as enum (handles both old "Message" and new values)
                if (Enum.TryParse<LogType>(value, true, out var result))
                {
                    // If it's the deprecated Message, treat as Info for filtering purposes
#pragma warning disable CS0618 // Type or member is obsolete
                    return result == LogType.Message ? LogType.Info : result;
#pragma warning restore CS0618
                }

                // Default to Info if parsing fails
                return LogType.Info;
            }
            else if (reader.TokenType == JsonTokenType.Number)
            {
                int numValue = reader.GetInt32();

                if (Enum.IsDefined(typeof(LogType), numValue))
                {
                    var result = (LogType)numValue;

                    // If it's the deprecated Message, treat as Info for filtering purposes
#pragma warning disable CS0618 // Type or member is obsolete
                    return result == LogType.Message ? LogType.Info : result;
#pragma warning restore CS0618
                }

                return LogType.Info;
            }

            return LogType.Info;
        }

        public override void Write(Utf8JsonWriter writer, LogType value, JsonSerializerOptions options)
        {
            // Convert deprecated Message to Info when writing new logs
#pragma warning disable CS0618 // Type or member is obsolete
            var writeValue = value == LogType.Message ? LogType.Info : value;
#pragma warning restore CS0618

            // Always write as string for readability
            writer.WriteStringValue(writeValue.ToString());
        }
    }

    public class LogObject
    {
        public LogObject(LogType type, string message)
        {
            Message = message;
            Type = type;
            Datetime = DateTime.UtcNow;
        }

        public string Message { get; set; }

        [JsonConverter(typeof(LogTypeConverter))]
        public LogType Type { get; set; }

        public DateTime Datetime { get; set; }
    }
}
