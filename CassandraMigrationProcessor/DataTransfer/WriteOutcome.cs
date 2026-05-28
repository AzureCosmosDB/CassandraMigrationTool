namespace CassandraMigrationProcessor.DataTransfer;

/// <summary>
/// Result of a single row write attempt loop. <see cref="Fatal"/> means
/// the page-wide writer should trip the job's fatal flag — the row
/// itself has already been accounted for in the
/// <see cref="WriteCounters"/>.
/// </summary>
internal enum WriteOutcome
{
    Success,
    Failed,
    Fatal,
}
