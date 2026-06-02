namespace CassandraMigrationProcessor.Models;

/// <summary>Kind of migration a <see cref="Job"/> performs (currently only CQL row copy).</summary>
public enum JobType
{
    CqlCopy,
}
