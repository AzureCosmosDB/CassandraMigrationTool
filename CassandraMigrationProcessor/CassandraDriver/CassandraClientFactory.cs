using Cassandra;
using System.Diagnostics;
using System.Security.Authentication;
using CassandraMigrationProcessor.Infrastructure;
using CassandraMigrationProcessor.Models;
namespace CassandraMigrationProcessor.CassandraDriver;

/// <summary>
/// Creates Cassandra ISession instances for source (Cosmos DB)
/// and target (OSS Cassandra) clusters.
/// Delegates ARM credential discovery to ArmCredentialDiscovery.
/// </summary>
public static class CassandraClientFactory
{
    private const string ApplicationName = "CMT-" + AppVersion.Value;
    private const int ReadTimeoutMs = 120000;
    private const int ConnectTimeoutMs = 30000;
    private const int ReconnectBaseDelayMs = 2000;
    private const int ReconnectMaxDelayMs = 60000;

    // Schema-metadata refresh debounce. The driver coalesces server-pushed
    // schema-change events that arrive within a sliding window into a single
    // metadata refresh: each new event pushes the scheduled refresh out by
    // RefreshSchemaDelayIncrement, capped at MaxTotalRefreshSchemaDelay.
    // The migration's schema phase issues a DDL per table in a tight burst;
    // widening this window (driver defaults are 1000ms / 10000ms) collapses
    // that burst into ~one refresh per session instead of one-per-DDL, which
    // is what amplifies control-plane load against Cosmos DB's Cassandra API
    // metadata throttle. Sync stays ENABLED so token-map/topology awareness
    // (token-aware routing, node add/remove) is preserved for OSS Cassandra
    // and Cassandra MI targets.
    private const int SchemaRefreshDelayIncrementMs = 5000;
    private const int SchemaRefreshMaxTotalDelayMs = 60000;

    /// <summary>
    /// Create a session to a Cosmos DB Cassandra API account.
    /// Uses SSL on port 10350 with PlainTextAuthProvider.
    /// Retries on 429/OverloadedException with backoff.
    /// </summary>
    public static ISession CreateSourceSession(
        MigrationLog MigrationLog,
        string contactPoint,
        int port,
        string username,
        string password,
        int maxConnectionsPerHost = 0)
    {
        // Source always uses SSL (Cosmos DB requires it)
        var builder = CreateBaseBuilder(
            contactPoint, port, username, password,
            useSsl: true, maxConnectionsPerHost);

        // Single connect+register success path. The loop covers all
        // attempts; the `when (attempt < MaxRetries)` filter swallows
        // transient failures only on attempts 1..MaxRetries-1, so on
        // the final attempt any exception — transient or not —
        // propagates out unhandled, matching the original "Final
        // attempt — let exception propagate" semantics.
        const int MaxRetries = 5;
        for (int attempt = 1; attempt <= MaxRetries; attempt++)
        {
            try
            {
                return ConnectCluster(builder);
            }
            catch (Exception ex) when (
                ExceptionClassifier.IsTransient(ex)
                && attempt < MaxRetries)
            {
                int delayMs = ExceptionClassifier.GetRetryDelayMs(ex, attempt);
                MigrationLog.WriteLine(
                    $"Source connect retry " +
                    $"{attempt}: {ex.Message}",
                    LogType.Warning);
                Thread.Sleep(delayMs);
            }
        }

        // Unreachable: the loop either returns on success or rethrows
        // on the final attempt (the `when` filter is false when
        // attempt == MaxRetries).
        throw new UnreachableException();
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
            // If either attempt failed for AuthenticationException-class
            // reasons, surface that as the outer exception so
            // ExceptionClassifier.IsAuth (which only checks the outer
            // type) can flag it. The AggregateException wrapper below
            // otherwise masks auth as a generic connect failure.
            var authInner = UnwrapToAuthenticationException(sslException)
                            ?? UnwrapToAuthenticationException(ex);
            if (authInner != null)
            {
                var promoted = new Cassandra.AuthenticationException(
                    $"Authentication failed connecting to {contactPoint}:{port}. " +
                    $"SSL error: {sslException?.Message}. " +
                    $"Plain error: {ex.Message}",
                    authInner.Host);
                // Cassandra.AuthenticationException has no (string, Exception)
                // overload; stash original stacks on Data so logging
                // surfaces upstream TLS / socket detail.
                if (sslException != null)
                    promoted.Data["SslError"] = sslException.ToString();
                promoted.Data["PlainError"] = ex.ToString();
                throw promoted;
            }
            // Throw the SSL exception if both fail
            throw new AggregateException(
                $"Failed to connect to {contactPoint}:{port}. " +
                $"SSL error: {sslException?.Message}. " +
                $"Plain error: {ex.Message}",
                sslException ?? ex, ex);
        }
    }

    /// <summary>
    /// Walks an AggregateException / InnerException /
    /// NoHostAvailableException chain looking for the first
    /// <see cref="Cassandra.AuthenticationException"/>. The driver
    /// typically wraps bad creds in NoHostAvailableException whose
    /// per-host <see cref="NoHostAvailableException.Errors"/> dictionary
    /// holds the actual AuthenticationException.
    /// </summary>
    private static Cassandra.AuthenticationException? UnwrapToAuthenticationException(Exception? ex)
    {
        for (int depth = 0; ex != null && depth < 8; depth++)
        {
            if (ex is Cassandra.AuthenticationException auth)
                return auth;
            if (ex is NoHostAvailableException nhae)
            {
                if (nhae.Errors != null)
                {
                    return nhae.Errors.Values
                        .Select(UnwrapToAuthenticationException)
                        .FirstOrDefault(found => found != null);
                }
                return null;
            }
            if (ex is AggregateException agg)
            {
                return agg.InnerExceptions
                    .Select(UnwrapToAuthenticationException)
                    .FirstOrDefault(found => found != null);
            }
            ex = ex.InnerException;
        }
        return null;
    }

    private static Builder CreateBaseBuilder(
        string contactPoint, int port,
        string? username, string? password,
        bool useSsl, int maxConnectionsPerHost = 0)
    {
        var builder = Cluster.Builder()
            .AddContactPoint(contactPoint)
            .WithPort(port)
            .WithApplicationName(ApplicationName)
            .WithApplicationVersion(AppVersion.Value)
            // Reduce — but do NOT disable — the driver's automatic
            // schema/topology metadata synchronisation. By default the driver
            // re-reads schema (system_schema.*) and peers (system.peers/local)
            // on connect and refreshes again on every server-pushed
            // schema/topology event. The migration's schema phase issues a DDL
            // per target table in a tight burst, each event otherwise firing a
            // separate refresh on every open session — a control-plane read
            // storm that Cosmos DB's Cassandra API governs under a metadata
            // throttle (429 / Substatus 3200, "high rate of metadata
            // requests") that extra RUs do not relieve. Widening the refresh
            // debounce window coalesces that DDL-phase burst into ~one refresh
            // per session, cutting the amplification, while keeping metadata
            // sync ENABLED so token-map/topology awareness (token-aware
            // routing and node add/remove reactions) is retained for OSS
            // Cassandra and Cassandra MI targets. (Residual per-session
            // connect-time reads scale with session count and are addressed
            // separately by sharing sessions across the worker pool.)
            .WithMetadataSyncOptions(new MetadataSyncOptions()
                .SetRefreshSchemaDelayIncrement(SchemaRefreshDelayIncrementMs)
                .SetMaxTotalRefreshSchemaDelay(SchemaRefreshMaxTotalDelayMs))
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

        // Apply pooling only when the caller explicitly opted in via
        // maxConnectionsPerHost > 0. Otherwise leave the driver defaults
        // alone — we do not silently impose Cosmos DB recommendations or
        // any other tuning based on the endpoint.
        if (maxConnectionsPerHost > 0)
        {
            int localMax = maxConnectionsPerHost;
            int localCore = Math.Max(1, localMax / 2);
            int remoteMax = Math.Max(1, localMax / 2);
            int remoteCore = Math.Max(1, remoteMax / 2);
            builder = builder.WithPoolingOptions(new PoolingOptions()
                .SetMaxConnectionsPerHost(HostDistance.Local, localMax)
                .SetCoreConnectionsPerHost(HostDistance.Local, localCore)
                .SetMaxConnectionsPerHost(HostDistance.Remote, remoteMax)
                .SetCoreConnectionsPerHost(HostDistance.Remote, remoteCore));
        }

        return builder;
    }

    private static ISession ConnectCluster(Builder builder)
    {
        Cluster? cluster = null;
        try
        {
            cluster = builder.Build();
            // Sessions are keyspace-agnostic: every query uses
            // fully-qualified "keyspace"."table" identifiers.
            return cluster.Connect();
        }
        catch
        {
            cluster?.Dispose();
            throw;
        }
    }

    /// <summary>
    /// Per-side connection pool sizing. The per-side override
    /// (<see cref="Job.SourceMaxConnectionsPerHost"/> /
    /// <see cref="Job.TargetMaxConnectionsPerHost"/>) takes precedence
    /// when positive; otherwise the job-level fallback
    /// <see cref="Job.MaxConnectionsPerHost"/> applies to both sides.
    /// </summary>
    internal static int ResolveMaxConnectionsPerHost(int perSideOverride, int jobWideFallback)
        => perSideOverride > 0 ? perSideOverride : jobWideFallback;

    /// <summary>
    /// Async version — Create target session from a Job's properties.
    /// Prefer this over the sync overload to avoid blocking on ARM discovery.
    /// </summary>
    public static async Task<ISession> CreateTargetSessionAsync(
        MigrationLog MigrationLog, Job job)
    {
        if (job.IsSimulatedRun)
            return new NullSession();

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

                if (armResult.AuthMethod != "None"
                    && !string.IsNullOrEmpty(armResult.Password))
                {
                    username = armResult.Username ?? username;
                    password = armResult.Password;
                }
                else
                {
                    // ARM said "no auth" or returned no usable
                    // password — fall back to anonymous bind so the
                    // session attempt below isn't authenticated.
                    username = string.Empty;
                    password = string.Empty;
                }
            }
            catch (Exception ex)
            {
                MigrationLog?.WriteLine(
                    $"ARM target credential discovery failed: {ex.Message}",
                    LogType.Error);
                throw new InvalidOperationException(
                    "ARM target credential discovery failed.",
                    ex);
            }
        }

        return CreateTargetSession(job.TargetContactPoint,
            job.TargetPort,
            username,
            password,
            maxConnectionsPerHost: ResolveMaxConnectionsPerHost(job.TargetMaxConnectionsPerHost, job.MaxConnectionsPerHost));
    }
}
