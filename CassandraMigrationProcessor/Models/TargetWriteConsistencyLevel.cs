using Newtonsoft.Json;
using Newtonsoft.Json.Converters;

namespace CassandraMigrationProcessor.Models;

/// <summary>
/// Cassandra consistency levels valid for regular target writes.
/// Counter-table read/modify/write operations always use LOCAL_QUORUM
/// to preserve retry correctness.
/// </summary>
[JsonConverter(typeof(StringEnumConverter))]
public enum TargetWriteConsistencyLevel
{
    LocalOne = 0,
    One = 1,
    Two = 2,
    Three = 3,
    Quorum = 4,
    LocalQuorum = 5,
    EachQuorum = 6,
    All = 7,
    Any = 8
}
