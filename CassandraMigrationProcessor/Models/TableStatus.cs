using Newtonsoft.Json;
using Newtonsoft.Json.Converters;

namespace CassandraMigrationProcessor.Models
{
    /// <summary>
    /// Status of a table during migration.
    /// Serialized with the old "CollectionStatus" name for
    /// backward compatibility with existing job files.
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