namespace CassandraMigrationProcessor.Models
{
    /// <summary>
    /// CDC mode for Cassandra migration (change feed).
    /// </summary>
    public enum CDCMode
    {
        Offline,
        Online
    }
}
