using Cassandra;
using System.Security.Authentication;
using CassandraMigrationProcessor.Infrastructure;
using CassandraMigrationProcessor.Models;
namespace CassandraMigrationProcessor.CassandraDriver;
/// <summary>
/// Creates Cassandra ISession instances for source (Cosmos DB)
/// and target (OSS Cassandra) clusters.
/// Delegates AAD token management to TokenRefreshManager and
/// ARM credential discovery to ArmCredentialDiscovery.
/// </summary>
public static class CassandraClientFactory
{
    private const int ReadTimeoutMs = 120000;
    private const int ConnectTimeoutMs = 30000;
    private const int ReconnectBaseDelayMs = 2000;
    private const int ReconnectMaxDelayMs = 60000;

    // Microsoft recommendation for the DataStax C# driver against
    // Cosmos DB API for Cassandra: keep Core==Max==10 connections per
    // host, and raise MaxRequestsPerConnection to 32768 (Cosmos DB
    // does not enforce the per-connection limits the OSS driver
    // defaults assume).
    private const int CosmosDbRecommendedConnectionsPerHost = 10;
    private const int CosmosDbRecommendedMaxRequestsPerConnection = 32768;

    private static bool IsCosmosDbCassandraEndpoint(string? contactPoint)
    {
        if (string.IsNullOrWhiteSpace(contactPoint)) return false;
        return contactPoint.EndsWith(".cassandra.cosmos.azure.com",
                StringComparison.OrdinalIgnoreCase)
            || contactPoint.EndsWith(".cassandra.cosmosdb.azure.com",
                StringComparison.OrdinalIgnoreCase)
            || contactPoint.EndsWith(".cassandra.cosmos.azure.us",
                StringComparison.OrdinalIgnoreCase)
            || contactPoint.EndsWith(".cassandra.cosmos.azure.cn",
                StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// Create a session to a Cosmos DB Cassandra API account.
    /// Uses SSL on port 10350 with PlainTextAuthProvider.
    /// Starts proactive token refresh if the password is a
    /// JWT/AAD token.
    /// Retries on 429/OverloadedException with backoff.
    /// </summary>
    public static ISession CreateSourceSession(
        MigrationLog MigrationLog,
        string contactPoint,
        int port,
        string username,
        string password,
        TokenRefreshManager? tokenRefreshManager = null,
        int maxConnectionsPerHost = 0)
    {
        // Cache parameters for token refresh reconnection
        tokenRefreshManager?.CacheSourceConnectionParams(
            contactPoint, port, username);

        // Source always uses SSL (Cosmos DB requires it)
        var builder = CreateBaseBuilder(
            contactPoint, port, username, password,
            useSsl: true, maxConnectionsPerHost);

        const int MaxRetries = 5;
        for (int attempt = 1; attempt <= MaxRetries; attempt++)
        {
            try
            {
                var session = ConnectCluster(builder);

                if (TokenRefreshManager.IsLikelyAadToken(password))
                {
                    tokenRefreshManager?.SetManagedSourceSession(session);
                    tokenRefreshManager?.StartTokenRefreshTimer(password);
                }

                return session;
            }
            catch (Exception ex) when (
                IsRetryableException(ex)
                && attempt < MaxRetries)
            {
                int delayMs = GetRetryDelayMs(ex, attempt);
                MigrationLog.WriteLine(
                    $"Source connect retry " +
                    $"{attempt}: {ex.Message}",
                    LogType.Warning);
                Thread.Sleep(delayMs);
            }
        }

        // Final attempt — let exception propagate
        var finalSession = ConnectCluster(builder);

        if (TokenRefreshManager.IsLikelyAadToken(password))
        {
            tokenRefreshManager?.SetManagedSourceSession(finalSession);
            tokenRefreshManager?.StartTokenRefreshTimer(password);
        }

        return finalSession;
    }

    /// <summary>
    /// Determine if an exception is retryable (429, overload,
    /// transient connection errors).
    /// </summary>
    internal static bool IsRetryableException(Exception ex)
    {
        if (ex is OverloadedException)
            return true;

        var msg = ex.Message;
        var inner = ex.InnerException?.Message ?? string.Empty;
        var fullMsg = msg + " " + inner;

        return fullMsg.Contains("429")
            || fullMsg.Contains("TooManyRequests")
            || fullMsg.Contains("OverloadedException")
            || fullMsg.Contains("Request rate is large")
            || fullMsg.Contains("RetryAfterMs")
            || fullMsg.Contains("rate limit",
                StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// Extract RetryAfterMs from error message if present,
    /// otherwise use exponential backoff.
    /// </summary>
    internal static int GetRetryDelayMs(
        Exception ex, int attempt)
    {
        // Try to extract RetryAfterMs=NNN from message
        var msg = (ex.Message ?? "") + " "
            + (ex.InnerException?.Message ?? "");
        var idx = msg.IndexOf("RetryAfterMs=",
            StringComparison.OrdinalIgnoreCase);
        if (idx >= 0)
        {
            var start = idx + "RetryAfterMs=".Length;
            var end = start;
            while (end < msg.Length
                && char.IsDigit(msg[end])) end++;
            if (end > start
                && int.TryParse(
                    msg.Substring(start, end - start),
                    out var retryMs)
                && retryMs > 0)
            {
                // Add jitter: retryMs + 100-500ms
                return retryMs + Random.Shared.Next(100, 500);
            }
        }

        // Exponential backoff: 1s, 2s, 4s, 8s, 16s
        return (int)(Math.Pow(2, attempt - 1) * 1000)
            + Random.Shared.Next(100, 500);
    }

    /// <summary>
    /// Create a session to an OSS Apache Cassandra cluster.
    /// Tries SSL first, falls back to plain if SSL fails.
    /// </summary>
    private static ISession CreateTargetSession(string contactPoint,
        int port,
        string username,
        string password,
        bool useSsl = true,
        int maxConnectionsPerHost = 0)
    {
        // Try SSL first, then fall back to plain
        Exception? sslException = null;
        if (useSsl)
        {
            try
            {
                return ConnectCluster(
                    CreateBaseBuilder(
                        contactPoint, port, username, password,
                        useSsl: true, maxConnectionsPerHost));
            }
            catch (Exception ex)
            {
                sslException = ex;
            }
        }

        try
        {
            return ConnectCluster(
                CreateBaseBuilder(
                    contactPoint, port, username, password,
                    useSsl: false, maxConnectionsPerHost));
        }
        catch (Exception ex)
        {
            // Throw the SSL exception if both fail
            throw new AggregateException(
                $"Failed to connect to {contactPoint}:{port}. " +
                $"SSL error: {sslException?.Message}. " +
                $"Plain error: {ex.Message}",
                sslException ?? ex, ex);
        }
    }

    private static Builder CreateBaseBuilder(
        string contactPoint, int port,
        string? username, string? password,
        bool useSsl, int maxConnectionsPerHost = 0)
    {
        var builder = Cluster.Builder()
            .AddContactPoint(contactPoint)
            .WithPort(port)
            .WithSocketOptions(new SocketOptions()
                .SetReadTimeoutMillis(ReadTimeoutMs)
                .SetConnectTimeoutMillis(ConnectTimeoutMs))
            .WithQueryOptions(new QueryOptions()
                .SetConsistencyLevel(
                    ConsistencyLevel.LocalQuorum))
            .WithReconnectionPolicy(
                new ExponentialReconnectionPolicy(
                    ReconnectBaseDelayMs, ReconnectMaxDelayMs));

        if (!string.IsNullOrWhiteSpace(username))
            builder = builder.WithAuthProvider(
                new PlainTextAuthProvider(username, password));

        if (useSsl)
        {
            var sslOptions = new SSLOptions(
                SslProtocols.None, false,
                (_, _, _, _) => true);
            sslOptions.SetHostNameResolver(_ => contactPoint);
            builder = builder.WithSSL(sslOptions);
        }

        // Apply pooling. For Cosmos DB API for Cassandra, Microsoft
        // recommends Core==Max==10 on Local + a generous
        // MaxRequestsPerConnection so the driver does not bottleneck on
        // a single TCP connection (Cosmos DB scales horizontally on the
        // service side and does not impose the per-connection request
        // ceiling that OSS Cassandra does).
        // Ref: github.com/Azure-Samples/azure-cosmos-cassandra-extensions
        // and learn.microsoft.com Cosmos DB for Cassandra perf guidance.
        // For non-Cosmos endpoints we keep the older "halve for Remote"
        // shape so an explicit user override still applies.
        bool isCosmos = IsCosmosDbCassandraEndpoint(contactPoint);
        int effectiveMax = maxConnectionsPerHost > 0
            ? maxConnectionsPerHost
            : (isCosmos ? CosmosDbRecommendedConnectionsPerHost : 0);

        if (effectiveMax > 0)
        {
            var pooling = new PoolingOptions();
            if (isCosmos)
            {
                pooling
                    .SetCoreConnectionsPerHost(HostDistance.Local, effectiveMax)
                    .SetMaxConnectionsPerHost(HostDistance.Local, effectiveMax)
                    .SetMaxRequestsPerConnection(CosmosDbRecommendedMaxRequestsPerConnection);
            }
            else
            {
                int localMax = effectiveMax;
                int localCore = Math.Max(1, localMax / 2);
                int remoteMax = Math.Max(1, localMax / 2);
                int remoteCore = Math.Max(1, remoteMax / 2);
                pooling
                    .SetMaxConnectionsPerHost(HostDistance.Local, localMax)
                    .SetCoreConnectionsPerHost(HostDistance.Local, localCore)
                    .SetMaxConnectionsPerHost(HostDistance.Remote, remoteMax)
                    .SetCoreConnectionsPerHost(HostDistance.Remote, remoteCore);
            }
            builder = builder.WithPoolingOptions(pooling);
        }

        return builder;
    }

    private static ISession ConnectCluster(Builder builder)
    {
        Cluster? cluster = null;
        try
        {
            cluster = builder.Build();
            // Sessions are intentionally keyspace-agnostic: every query
            // in the migration uses fully-qualified `"keyspace"."table"`
            // identifiers, so binding a default keyspace at connect time
            // would add nothing and would force callers to track a
            // keyspace they never use.
            return cluster.Connect();
        }
        catch
        {
            cluster?.Dispose();
            throw;
        }
    }

    /// <summary>
    /// Create source session from a Job's properties.
    /// If SourceUseAad is true or password is missing (e.g.
    /// on resume after [JsonIgnore]), fetches a fresh AAD
    /// token automatically.
    /// </summary>
    public static ISession CreateSourceSession(
        MigrationLog MigrationLog, Job job,
        TokenRefreshManager? tokenRefreshManager = null)
    {
        if (string.IsNullOrEmpty(job.SourceContactPoint))
            throw new ArgumentException("Source contact point is required", nameof(job));

        string password = job.SourcePassword ?? string.Empty;

        // If password is empty (resume) or AAD is enabled,
        // fetch a fresh token via managed identity
        if (string.IsNullOrEmpty(password) || job.SourceUseAad)
        {
            password = tokenRefreshManager?.GetFreshAadToken()
                ?? TokenRefreshManager.AcquireAadToken();
            // Cache it in memory (not persisted)
            job.SourcePassword = password;
            job.SourceUseAad = true;
        }

        // For AAD auth, derive username from hostname if
        // not explicitly provided (account name = first
        // segment of the contact point FQDN).
        string username = job.SourceUsername ?? string.Empty;
        if (string.IsNullOrWhiteSpace(username)
            && job.SourceUseAad
            && !string.IsNullOrEmpty(job.SourceContactPoint))
        {
            username = job.SourceContactPoint
                .Split('.')[0];
        }

        return CreateSourceSession(
            MigrationLog,
            job.SourceContactPoint,
            job.SourcePort,
            username,
            password,
            tokenRefreshManager,
            maxConnectionsPerHost: job.MaxConnectionsPerHost);
    }

    /// <summary>
    /// Async version — Create target session from a Job's properties.
    /// Prefer this over the sync overload to avoid blocking on ARM discovery.
    /// </summary>
    public static async Task<ISession> CreateTargetSessionAsync(
        MigrationLog MigrationLog, Job job)
    {
        if (string.IsNullOrEmpty(job.TargetContactPoint))
            throw new ArgumentException("Target contact point is required", nameof(job));

        string password = job.TargetPassword ?? string.Empty;
        string username = job.TargetUsername ?? string.Empty;

        // If password is empty, try ARM-based credential discovery
        if (string.IsNullOrEmpty(password))
        {
            try
            {
                var armResult = await ArmCredentialDiscovery
                    .DiscoverTargetCredentialsViaArm(
                        job.TargetContactPoint,
                        job.TargetPort);

                if (armResult.AuthMethod == "None")
                {
                    username = string.Empty;
                    password = string.Empty;
                }
                else if (!string.IsNullOrEmpty(armResult.Password))
                {
                    username = armResult.Username ?? username;
                    password = armResult.Password;
                }
                else
                {
                    username = string.Empty;
                    password = string.Empty;
                }
            }
            catch (Exception ex)
            {
                MigrationLog?.WriteLine($"ARM credential discovery failed: {ex.Message}", LogType.Debug);
            }
        }

        return CreateTargetSession(job.TargetContactPoint,
            job.TargetPort,
            username,
            password,
            maxConnectionsPerHost: job.MaxConnectionsPerHost);
    }
}
