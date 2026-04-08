using Cassandra;
using CassandraMigrationProcessor.Context;
using System;
using System.IdentityModel.Tokens.Jwt;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Security.Authentication;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;

namespace CassandraMigrationProcessor.Helpers.Cassandra
{
    /// <summary>
    /// Creates Cassandra ISession instances for source (Cosmos DB)
    /// and target (OSS Cassandra) clusters.
    /// Manages proactive AAD token refresh before expiry.
    /// </summary>
    public static class CassandraClientFactory
    {
        // Cache last-used connection parameters for token refresh
        private static string? _lastSourceContactPoint;
        private static int _lastSourcePort;
        private static string? _lastSourceUsername;
        private static string? _lastSourceKeyspace;

        // Proactive token refresh timer
        private static Timer? _tokenRefreshTimer;
        private static readonly object _refreshLock = new();
        private static ISession? _managedSourceSession;
        private static Log? _lastLog;
        private static DateTime _tokenExpiresAt = DateTime.MinValue;

        /// <summary>
        /// Generate a fresh AAD token for Cosmos DB Cassandra.
        /// Uses DefaultAzureCredential (Managed Identity in
        /// App Service, Azure CLI locally).
        /// </summary>
        public static string GetFreshAadToken()
        {
            try
            {
                var credential =
                    new Azure.Identity.DefaultAzureCredential();
                var tokenResult = credential.GetToken(
                    new Azure.Core.TokenRequestContext(
                        new[] { "https://cosmos.azure.com/.default" }));

                _tokenExpiresAt = tokenResult.ExpiresOn.UtcDateTime;

                return tokenResult.Token;
            }
            catch
            {
                throw;
            }
        }

        /// <summary>
        /// Returns the UTC time the current AAD token expires.
        /// </summary>
        public static DateTime TokenExpiresAtUtc => _tokenExpiresAt;

        /// <summary>
        /// Parse the "exp" claim from a JWT to determine when
        /// it expires. Returns DateTime.MaxValue if parsing fails.
        /// </summary>
        public static DateTime GetTokenExpiry(string token)
        {
            try
            {
                var handler = new JwtSecurityTokenHandler();
                if (handler.CanReadToken(token))
                {
                    var jwt = handler.ReadJwtToken(token);
                    return jwt.ValidTo;
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[WARN] GetTokenExpiry failed: {ex.Message}");
            }
            return DateTime.MaxValue;
        }

        /// <summary>
        /// Start the proactive token refresh timer. Schedules
        /// a refresh 5 minutes before the token expires.
        /// If the token can't be parsed, defaults to refreshing
        /// every 50 minutes (tokens typically live 60-75 min).
        /// </summary>
        public static void StartTokenRefreshTimer(
            string currentToken, Log log)
        {
            lock (_refreshLock)
            {
                _lastLog = log;
                StopTokenRefreshTimer();

                DateTime expiry = GetTokenExpiry(currentToken);
                if (expiry == DateTime.MaxValue)
                {
                    // Can't parse — refresh every 50 minutes
                    expiry = DateTime.UtcNow.AddMinutes(50);
                }

                _tokenExpiresAt = expiry;

                // Refresh 5 minutes before expiry, minimum 1 min
                TimeSpan delay = expiry - DateTime.UtcNow
                    - TimeSpan.FromMinutes(5);
                if (delay < TimeSpan.FromMinutes(1))
                    delay = TimeSpan.FromMinutes(1);

                _tokenRefreshTimer = new Timer(
                    TokenRefreshCallback, null,
                    delay, Timeout.InfiniteTimeSpan);
            }
        }

        /// <summary>
        /// Stop the proactive token refresh timer.
        /// </summary>
        public static void StopTokenRefreshTimer()
        {
            _tokenRefreshTimer?.Dispose();
            _tokenRefreshTimer = null;
        }

        private static void TokenRefreshCallback(object? state)
        {
            lock (_refreshLock)
            {
                try
                {
                    string freshToken = GetFreshAadToken();

                    // If we have a managed session, recreate it
                    if (_managedSourceSession != null
                        && !_managedSourceSession.IsDisposed
                        && _lastSourceContactPoint != null)
                    {
                        var oldSession = _managedSourceSession;
                        _managedSourceSession = CreateSourceSession(
                            _lastLog ?? new Log(),
                            _lastSourceContactPoint,
                            _lastSourcePort,
                            _lastSourceUsername ?? string.Empty,
                            freshToken,
                            _lastSourceKeyspace ?? string.Empty);
                        try { oldSession.Dispose(); }
                        catch (Exception ex)
                        {
                            Console.WriteLine($"[WARN] TokenRefresh old session dispose failed: {ex.Message}");
                        }
                    }

                    // Schedule next refresh
                    StartTokenRefreshTimer(freshToken,
                        _lastLog ?? new Log());
                }
                catch (Exception)
                {
                    // Retry in 2 minutes on failure
                    StopTokenRefreshTimer();
                    _tokenRefreshTimer = new Timer(
                        TokenRefreshCallback, null,
                        TimeSpan.FromMinutes(2),
                        Timeout.InfiniteTimeSpan);
                }
            }
        }

        /// <summary>
        /// Get the managed source session (for token refresh).
        /// Returns null if no managed session exists.
        /// </summary>
        public static ISession? ManagedSourceSession =>
            _managedSourceSession;

        /// <summary>
        /// Set the managed source session so the token refresh
        /// timer can reconnect it proactively.
        /// </summary>
        public static void SetManagedSourceSession(ISession session)
        {
            _managedSourceSession = session;
        }

        /// <summary>
        /// Detect if a password looks like an AAD/JWT token
        /// (very long base64-ish string).
        /// </summary>
        public static bool IsLikelyAadToken(string? password)
        {
            return password != null && password.Length > 200;
        }

        /// <summary>
        /// Reconnect the source session with a fresh AAD token.
        /// Returns a new ISession. The caller should dispose the
        /// old session. Also restarts the token refresh timer.
        /// </summary>
        public static ISession ReconnectSourceWithFreshToken(
            Log log)
        {
            string freshToken = GetFreshAadToken();

            // Restart refresh timer with new token
            StartTokenRefreshTimer(freshToken, log);

            var newSession = CreateSourceSession(
                log,
                _lastSourceContactPoint!,
                _lastSourcePort,
                _lastSourceUsername ?? string.Empty,
                freshToken,
                _lastSourceKeyspace ?? string.Empty);

            // Update managed session reference
            _managedSourceSession = newSession;

            return newSession;
        }

        /// <summary>
        /// Create a session to a Cosmos DB Cassandra API account.
        /// Uses SSL on port 10350 with PlainTextAuthProvider.
        /// Starts proactive token refresh if the password is a
        /// JWT/AAD token.
        /// Retries on 429/OverloadedException with backoff.
        /// </summary>
        public static ISession CreateSourceSession(
            Log log,
            string contactPoint,
            int port,
            string username,
            string password,
            string keyspace)
        {
            // Cache parameters for token refresh
            _lastSourceContactPoint = contactPoint;
            _lastSourcePort = port;
            _lastSourceUsername = username;
            _lastSourceKeyspace = keyspace;
            _lastLog = log;

            var sslOptions = new SSLOptions(
                SslProtocols.Tls12, true,
                (sender, certificate, chain, sslPolicyErrors) =>
                {
                    return true; // Azure MI/Cosmos certs may have chain+name issues
                });
            sslOptions.SetHostNameResolver(
                (ipAddress) => contactPoint);

            const int MaxRetries = 5;
            for (int attempt = 1; attempt <= MaxRetries; attempt++)
            {
                Cluster? cluster = null;
                try
                {
                    cluster = Cluster.Builder()
                        .AddContactPoint(contactPoint)
                        .WithPort(port)
                        .WithAuthProvider(
                            new PlainTextAuthProvider(
                                username, password))
                        .WithSSL(sslOptions)
                        .WithSocketOptions(new SocketOptions()
                            .SetReadTimeoutMillis(120000)
                            .SetConnectTimeoutMillis(30000))
                        .WithQueryOptions(new QueryOptions()
                            .SetConsistencyLevel(
                                ConsistencyLevel.LocalQuorum))
                        .WithReconnectionPolicy(
                            new ExponentialReconnectionPolicy(
                                2000, 60000))
                        .Build();

                    var session =
                        string.IsNullOrWhiteSpace(keyspace)
                            ? cluster.Connect()
                            : cluster.Connect(keyspace);

                    if (IsLikelyAadToken(password))
                    {
                        _managedSourceSession = session;
                        StartTokenRefreshTimer(password, log);
                    }

                    return session;
                }
                catch (Exception ex) when (
                    IsRetryableException(ex)
                    && attempt < MaxRetries)
                {
                    cluster?.Dispose();
                    int delayMs = GetRetryDelayMs(ex, attempt);
                    log.WriteLine(
                        $"Source connect retry " +
                        $"{attempt}: {ex.Message}",
                        LogType.Warning);
                    Thread.Sleep(delayMs);
                }
            }

            // Final attempt — let exception propagate
            var finalCluster = Cluster.Builder()
                .AddContactPoint(contactPoint)
                .WithPort(port)
                .WithAuthProvider(
                    new PlainTextAuthProvider(username, password))
                .WithSSL(sslOptions)
                .WithSocketOptions(new SocketOptions()
                    .SetReadTimeoutMillis(120000)
                    .SetConnectTimeoutMillis(30000))
                .WithQueryOptions(new QueryOptions()
                    .SetConsistencyLevel(
                        ConsistencyLevel.LocalQuorum))
                .WithReconnectionPolicy(
                    new ExponentialReconnectionPolicy(
                        2000, 60000))
                .Build();

            var finalSession =
                string.IsNullOrWhiteSpace(keyspace)
                    ? finalCluster.Connect()
                    : finalCluster.Connect(keyspace);

            if (IsLikelyAadToken(password))
            {
                _managedSourceSession = finalSession;
                StartTokenRefreshTimer(password, log);
            }

            return finalSession;
        }

        /// <summary>
        /// Determine if an exception is retryable (429, overload,
        /// transient connection errors).
        /// </summary>
        internal static bool IsRetryableException(Exception ex)
        {
            if (ex is global::Cassandra.OverloadedException)
                return true;

            var msg = ex.Message ?? string.Empty;
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
        public static ISession CreateTargetSession(
            Log log,
            string contactPoint,
            int port,
            string username,
            string password,
            string keyspace,
            bool useSsl = true,
            int maxConnectionsPerHost = 0)
        {
            // Try SSL first, then fall back to plain
            Exception? sslException = null;
            if (useSsl)
            {
                try
                {
                    var session = BuildAndConnect(
                        contactPoint, port, username, password,
                        keyspace, useSsl: true,
                        maxConnectionsPerHost);
                    return session;
                }
                catch (Exception ex)
                {
                    sslException = ex;
                }
            }

            try
            {
                var session = BuildAndConnect(
                    contactPoint, port, username, password,
                    keyspace, useSsl: false,
                    maxConnectionsPerHost);
                return session;
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

        private static ISession BuildAndConnect(
            string contactPoint, int port,
            string username, string password,
            string keyspace, bool useSsl,
            int maxConnectionsPerHost = 0)
        {
            int localMax = maxConnectionsPerHost > 0
                ? maxConnectionsPerHost : 8;
            int localCore = Math.Max(1, localMax / 2);
            int remoteMax = Math.Max(1, localMax / 2);
            int remoteCore = Math.Max(1, remoteMax / 2);

            var builder = Cluster.Builder()
                .AddContactPoint(contactPoint)
                .WithPort(port)
                .WithSocketOptions(new SocketOptions()
                    .SetReadTimeoutMillis(120000)
                    .SetConnectTimeoutMillis(30000))
                .WithPoolingOptions(new PoolingOptions()
                    .SetMaxConnectionsPerHost(
                        HostDistance.Local, localMax)
                    .SetCoreConnectionsPerHost(
                        HostDistance.Local, localCore)
                    .SetMaxConnectionsPerHost(
                        HostDistance.Remote, remoteMax)
                    .SetCoreConnectionsPerHost(
                        HostDistance.Remote, remoteCore))
                .WithQueryOptions(new QueryOptions()
                    .SetConsistencyLevel(
                        ConsistencyLevel.LocalQuorum))
                .WithReconnectionPolicy(
                    new ExponentialReconnectionPolicy(2000, 60000));

            if (!string.IsNullOrWhiteSpace(username)
                && password != null)
            {
                builder = builder.WithAuthProvider(
                    new PlainTextAuthProvider(username, password));
            }

            if (useSsl)
            {
                var sslOptions = new SSLOptions(
                    SslProtocols.Tls12, true,
                    (sender, certificate, chain, sslPolicyErrors) =>
                    {
                        return true; // Azure MI certs may have chain+name issues
                    });
                builder = builder.WithSSL(sslOptions);
            }

            Cluster? cluster = null;
            try
            {
                cluster = builder.Build();
                var session = string.IsNullOrWhiteSpace(keyspace)
                    ? cluster.Connect()
                    : cluster.Connect(keyspace);
                return session;
            }
            catch
            {
                cluster?.Dispose();
                throw;
            }
        }

        /// <summary>
        /// Create source session from a MigrationJob's properties.
        /// If SourceUseAad is true or password is missing (e.g.
        /// on resume after [JsonIgnore]), fetches a fresh AAD
        /// token automatically.
        /// </summary>
        public static ISession CreateSourceSession(
            Log log, MigrationJob job, string keyspace)
        {
            string password = job.SourcePassword ?? string.Empty;

            // If password is empty (resume) or AAD is enabled,
            // fetch a fresh token via managed identity
            if (string.IsNullOrEmpty(password) || job.SourceUseAad)
            {
                password = GetFreshAadToken();
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
                log,
                job.SourceContactPoint!,
                job.SourcePort,
                username,
                password,
                keyspace);
        }

        /// <summary>
        /// Create target session from a MigrationJob's properties.
        /// If target password is empty, tries ARM auto-discovery:
        /// 1. For MI clusters with authenticationMethod=None → no credentials needed
        /// 2. For Cosmos DB Cassandra accounts → fetch keys via listKeys ARM API
        /// Falls back to no-auth connection if ARM discovery fails.
        /// </summary>
        public static ISession CreateTargetSession(
            Log log, MigrationJob job, string keyspace)
        {
            string password = job.TargetPassword ?? string.Empty;
            string username = job.TargetUsername ?? string.Empty;

            // If password is empty, try ARM-based credential discovery
            if (string.IsNullOrEmpty(password))
            {
                try
                {
                    var armResult = DiscoverTargetCredentialsViaArm(
                        job.TargetContactPoint!,
                        job.TargetPort).GetAwaiter().GetResult();

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
                        // Auth required but password not available.
                        // Connect without credentials — MI may
                        // accept unauthenticated connections.
                        username = string.Empty;
                        password = string.Empty;
                    }
                }
                catch (Exception)
                {
                    // ARM discovery failed — continue with empty credentials
                }
            }

            return CreateTargetSession(
                log,
                job.TargetContactPoint!,
                job.TargetPort,
                username,
                password,
                keyspace,
                maxConnectionsPerHost: job.MaxConnectionsPerHost);
        }

        /// <summary>
        /// Result of ARM-based credential discovery.
        /// </summary>
        private class ArmCredentialResult
        {
            public string? AuthMethod { get; set; }
            public string? Username { get; set; }
            public string? Password { get; set; }
        }

        private static readonly HttpClient _armHttpClient = new();

        /// <summary>
        /// Discover target credentials via ARM control plane.
        /// Searches for Cassandra MI clusters and Cosmos DB accounts
        /// whose seed/contact points match the target IP/hostname.
        /// </summary>
        private static async Task<ArmCredentialResult> DiscoverTargetCredentialsViaArm(
            string targetContactPoint, int targetPort)
        {
            var credential = new Azure.Identity.DefaultAzureCredential();
            var armToken = (await credential.GetTokenAsync(
                new Azure.Core.TokenRequestContext(
                    new[] { "https://management.azure.com/.default" }))).Token;

            // Discover subscription ID from environment or well-known
            string? subscriptionId = Environment.GetEnvironmentVariable(
                "AZURE_SUBSCRIPTION_ID");

            if (string.IsNullOrEmpty(subscriptionId))
            {
                // Try to discover from the current resource's metadata
                subscriptionId = await GetSubscriptionIdFromImds();
            }

            if (string.IsNullOrEmpty(subscriptionId))
            {
                return new ArmCredentialResult();
            }

            // 1. Check MI clusters
            var miResult = await CheckMiClusters(
                armToken, subscriptionId, targetContactPoint);
            if (miResult != null) return miResult;

            // 2. Check Cosmos DB Cassandra accounts
            var cosmosResult = await CheckCosmosAccounts(
                armToken, subscriptionId, targetContactPoint,
                targetPort);
            if (cosmosResult != null) return cosmosResult;

            return new ArmCredentialResult();
        }

        /// <summary>
        /// Try to get subscription ID from Azure IMDS
        /// (works in App Service and VMs).
        /// </summary>
        private static async Task<string?> GetSubscriptionIdFromImds()
        {
            try
            {
                var req = new HttpRequestMessage(HttpMethod.Get,
                    "http://169.254.169.254/metadata/instance" +
                    "?api-version=2021-02-01");
                req.Headers.Add("Metadata", "true");
                var resp = await _armHttpClient.SendAsync(req);
                if (!resp.IsSuccessStatusCode) return null;
                var json = await resp.Content.ReadAsStringAsync();
                using var doc = JsonDocument.Parse(json);
                return doc.RootElement
                    .GetProperty("compute")
                    .GetProperty("subscriptionId")
                    .GetString();
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[WARN] GetSubscriptionIdFromImds failed: {ex.Message}");
                return null;
            }
        }

        /// <summary>
        /// Search MI clusters in the subscription for one whose
        /// seed nodes match the target contact point.
        /// </summary>
        private static async Task<ArmCredentialResult?> CheckMiClusters(
            string armToken, string subscriptionId,
            string targetContactPoint)
        {
            try
            {
                var url = $"https://management.azure.com/subscriptions/" +
                    $"{subscriptionId}/providers/Microsoft.DocumentDB/" +
                    $"cassandraClusters?api-version=2024-05-15";

                var req = new HttpRequestMessage(HttpMethod.Get, url);
                req.Headers.Authorization =
                    new AuthenticationHeaderValue("Bearer", armToken);
                var resp = await _armHttpClient.SendAsync(req);
                if (!resp.IsSuccessStatusCode) return null;

                var json = await resp.Content.ReadAsStringAsync();
                using var doc = JsonDocument.Parse(json);

                foreach (var cluster in doc.RootElement
                    .GetProperty("value").EnumerateArray())
                {
                    var props = cluster.GetProperty("properties");

                    // Check seed nodes
                    if (props.TryGetProperty("seedNodes", out var seeds))
                    {
                        foreach (var seed in seeds.EnumerateArray())
                        {
                            var ip = seed.TryGetProperty(
                                "ipAddress", out var ipProp)
                                ? ipProp.GetString() : null;
                            if (ip == targetContactPoint)
                            {
                                var authMethod = props
                                    .TryGetProperty(
                                        "authenticationMethod",
                                        out var authProp)
                                    ? authProp.GetString() : null;

                                return new ArmCredentialResult
                                {
                                    AuthMethod = authMethod ?? "Unknown"
                                };
                            }
                        }
                    }
                }
            }
            catch (Exception)
            {
                // ARM MI cluster search error — fall through
            }
            return null;
        }

        /// <summary>
        /// Search Cosmos DB accounts for a Cassandra API account
        /// matching the target contact point, and fetch its keys.
        /// </summary>
        private static async Task<ArmCredentialResult?> CheckCosmosAccounts(
            string armToken, string subscriptionId,
            string targetContactPoint, int targetPort)
        {
            // Only check Cosmos if port is 10350 (Cassandra API)
            if (targetPort != 10350) return null;

            try
            {
                // Derive account name from hostname
                // e.g. "myaccount.cassandra.cosmos.azure.com" → "myaccount"
                var hostParts = targetContactPoint.Split('.');
                if (hostParts.Length < 2) return null;
                var accountName = hostParts[0];

                // List Cosmos accounts to find matching one
                var listUrl = $"https://management.azure.com/subscriptions/" +
                    $"{subscriptionId}/providers/Microsoft.DocumentDB/" +
                    $"databaseAccounts?api-version=2024-05-15";

                var listReq = new HttpRequestMessage(HttpMethod.Get, listUrl);
                listReq.Headers.Authorization =
                    new AuthenticationHeaderValue("Bearer", armToken);
                var listResp = await _armHttpClient.SendAsync(listReq);
                if (!listResp.IsSuccessStatusCode) return null;

                var listJson = await listResp.Content.ReadAsStringAsync();
                using var listDoc = JsonDocument.Parse(listJson);

                foreach (var account in listDoc.RootElement
                    .GetProperty("value").EnumerateArray())
                {
                    var name = account.GetProperty("name").GetString();
                    if (!string.Equals(name, accountName,
                        StringComparison.OrdinalIgnoreCase))
                        continue;

                    // Found the account — fetch keys
                    var resourceId = account
                        .GetProperty("id").GetString();
                    var keysUrl = $"https://management.azure.com" +
                        $"{resourceId}/listKeys" +
                        $"?api-version=2024-05-15";
                    var keysReq = new HttpRequestMessage(
                        HttpMethod.Post, keysUrl);
                    keysReq.Headers.Authorization =
                        new AuthenticationHeaderValue("Bearer", armToken);
                    var keysResp = await _armHttpClient.SendAsync(keysReq);

                    if (keysResp.IsSuccessStatusCode)
                    {
                        var keysJson = await keysResp.Content
                            .ReadAsStringAsync();
                        using var keysDoc = JsonDocument.Parse(keysJson);
                        var primaryKey = keysDoc.RootElement
                            .TryGetProperty("primaryMasterKey",
                                out var keyProp)
                            ? keyProp.GetString() : null;

                        if (!string.IsNullOrEmpty(primaryKey))
                        {
                            return new ArmCredentialResult
                            {
                                AuthMethod = "Cassandra",
                                Username = accountName,
                                Password = primaryKey
                            };
                        }
                    }
                }
            }
            catch (Exception)
            {
                // ARM Cosmos account search error — fall through
            }
            return null;
        }
    }
}
