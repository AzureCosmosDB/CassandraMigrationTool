using Newtonsoft.Json;
using Newtonsoft.Json.Converters;

namespace CassandraMigrationProcessor.Models;

/// <summary>
/// Progress of bulk-copy for a single TableMigration unit. Ordered: a
/// unit can only ever advance toward Completed. Pause/Resume are
/// orthogonal — they are job-execution commands and do not change this
/// value; on resume the worker dispatches on the persisted phase.
/// </summary>
[JsonConverter(typeof(StringEnumConverter))]
public enum BulkCopyPhase
{
    NotStarted = 0,
    PreparingSchema = 1,
    Copying = 2,
    Completed = 3,
}
