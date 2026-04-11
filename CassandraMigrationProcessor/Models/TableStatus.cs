using Newtonsoft.Json;
using Newtonsoft.Json.Converters;

namespace CassandraMigrationProcessor.Models
{
    /// <summary>
    /// Status of a table during migration.
    /// </summary>
    [JsonConverter(typeof(StringEnumConverter))]
    public enum TableStatus
    {
        Unknown,
        OK,
        NotFound,
        Failed,
    }
}