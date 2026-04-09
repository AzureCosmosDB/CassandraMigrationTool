namespace CassandraMigrationProcessor.Models;

public record ConnectionOptions(
    string Host,
    int Port,
    string? Username,
    string? Password,
    bool UseSsl = true,
    int MaxConnectionsPerHost = 0);
