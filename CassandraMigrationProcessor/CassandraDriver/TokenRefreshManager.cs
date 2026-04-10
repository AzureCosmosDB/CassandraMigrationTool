using Cassandra;
using System;
using System.IdentityModel.Tokens.Jwt;
using System.Threading;
using CassandraMigrationProcessor.Infrastructure;
using CassandraMigrationProcessor.Models;

namespace CassandraMigrationProcessor.CassandraDriver
{
    /// <summary>
    /// Manages AAD token lifecycle and proactive refresh for
    /// Cosmos DB Cassandra API connections.
    /// </summary>
    public static class TokenRefreshManager
    {
        // Proactive token refresh timer
        private static Timer? _tokenRefreshTimer;
        private static readonly object _refreshLock = new();
        private static ISession? _managedSourceSession;
        private static MigrationLog? _lastLog;
        private static DateTime _tokenExpiresAt = DateTime.MinValue;

        // Cached source connection parameters for token refresh reconnection
        private static string? _lastSourceContactPoint;
        private static int _lastSourcePort;
        private static string? _lastSourceUsername;
        private static string? _lastSourceKeyspace;

        /// <summary>
        /// Cache source connection parameters so the token refresh
        /// timer can reconnect with a fresh token.
        /// </summary>
        internal static void CacheSourceConnectionParams(
            string contactPoint, int port, string username,
            string keyspace, MigrationLog log)
        {
            _lastSourceContactPoint = contactPoint;
            _lastSourcePort = port;
            _lastSourceUsername = username;
            _lastSourceKeyspace = keyspace;
            _lastLog = log;
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
            MigrationLog MigrationLog)
        {
            string freshToken = GetFreshAadToken();

            // Restart refresh timer with new token
            StartTokenRefreshTimer(freshToken, MigrationLog);

            var newSession = CassandraClientFactory.CreateSourceSession(
                MigrationLog,
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
        /// Generate a fresh AAD token for Cosmos DB Cassandra.
        /// Uses DefaultAzureCredential (Managed Identity in
        /// App Service, Azure CLI locally).
        /// </summary>
        public static string GetFreshAadToken()
        {
            var credential =
                new Azure.Identity.DefaultAzureCredential();
            var tokenResult = credential.GetToken(
                new Azure.Core.TokenRequestContext(
                    new[] { "https://cosmos.azure.com/.default" }));

            _tokenExpiresAt = tokenResult.ExpiresOn.UtcDateTime;

            return tokenResult.Token;
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
            string currentToken, MigrationLog MigrationLog)
        {
            lock (_refreshLock)
            {
                _lastLog = MigrationLog;
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
                        _managedSourceSession = CassandraClientFactory.CreateSourceSession(
                            _lastLog ?? new MigrationLog(),
                            _lastSourceContactPoint,
                            _lastSourcePort,
                            _lastSourceUsername ?? string.Empty,
                            freshToken,
                            _lastSourceKeyspace ?? string.Empty);
                        MigrationUtilities.SafeDispose(oldSession, "TokenRefresh old session");
                    }

                    // Schedule next refresh
                    StartTokenRefreshTimer(freshToken,
                        _lastLog ?? new MigrationLog());
                }
                catch (Exception ex)
                {
                    // Retry in 2 minutes on failure
                    Console.WriteLine($"[WARN] Token refresh failed: {ex.Message}");
                    _lastLog?.WriteLine($"Token refresh failed, retrying in 2 min: {ex.Message}", LogType.Warning);
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
    }
}
