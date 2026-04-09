using System;
using System.Text.Json.Serialization;

namespace CassandraMigrationProcessor.Models
{
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