using System.IdentityModel.Tokens.Jwt;
using CassandraMigrationProcessor.Infrastructure;
using CassandraMigrationProcessor.Models;

namespace CassandraMigrationProcessor.CassandraDriver;
/// <summary>
/// Manages AAD token lifecycle and proactive refresh for
/// Cosmos DB Cassandra API connections.
/// </summary>
public class TokenRefreshManager : IDisposable
{
    private Timer? _tokenRefreshTimer;
    private readonly object _refreshLock = new();
    private readonly Action<string> _refreshSession;
    private readonly MigrationLog _log;
    private bool _disposed;
    private DateTime _tokenExpiresAt = DateTime.MinValue;
    private int _consecutiveRefreshFailures;
    private const int MaxRefreshFailures = 6;

    public TokenRefreshManager(
        MigrationLog log,
        Action<string> refreshSession)
    {
        _log = log;
        _refreshSession = refreshSession
            ?? throw new ArgumentNullException(nameof(refreshSession));
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
    /// Acquire a fresh AAD token for Cosmos DB Cassandra
    /// without tracking expiry state. Use for one-shot
    /// sessions that do not need proactive refresh.
    /// </summary>
    public static string AcquireAadToken()
    {
        return AcquireTokenInternal().Token;
    }

    /// <summary>
    /// Generate a fresh AAD token for Cosmos DB Cassandra.
    /// Uses DefaultAzureCredential (Managed Identity in
    /// App Service, Azure CLI locally).
    /// </summary>
    public string GetFreshAadToken()
    {
        var tokenResult = AcquireTokenInternal();
        _tokenExpiresAt = tokenResult.ExpiresOn.UtcDateTime;
        return tokenResult.Token;
    }

    private static Azure.Core.AccessToken AcquireTokenInternal()
    {
        var credential = new Azure.Identity.DefaultAzureCredential();
        return credential.GetToken(
            new Azure.Core.TokenRequestContext(
                new[] { "https://cosmos.azure.com/.default" }));
    }

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
    public void StartTokenRefreshTimer(
        string currentToken)
    {
        lock (_refreshLock)
        {
            if (_disposed) return;
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
    public void StopTokenRefreshTimer()
    {
        _tokenRefreshTimer?.Dispose();
        _tokenRefreshTimer = null;
    }

    private void TokenRefreshCallback(object? state)
    {
        lock (_refreshLock)
        {
            if (_disposed) return;
            try
            {
                string freshToken = GetFreshAadToken();

                _refreshSession(freshToken);

                // Schedule next refresh
                _consecutiveRefreshFailures = 0;
                StartTokenRefreshTimer(freshToken);
            }
            catch (Exception ex)
            {
                _consecutiveRefreshFailures++;
                // Exponential backoff capped at 5 min:
                //   1: 30s  2: 1m  3: 2m  4: 4m  5+: 5m
                int seconds = Math.Min(300, 30 * (1 << Math.Min(_consecutiveRefreshFailures - 1, 4)));
                bool tokenAlreadyExpired = DateTime.UtcNow >= _tokenExpiresAt;
                LogType severity = (_consecutiveRefreshFailures >= MaxRefreshFailures || tokenAlreadyExpired)
                    ? LogType.Error
                    : LogType.Warning;
                string msg = $"Token refresh failed (attempt {_consecutiveRefreshFailures}, " +
                             $"retrying in {seconds}s, tokenExpiresAt={_tokenExpiresAt:O}): {ex.Message}";
                Console.WriteLine($"[{severity}] {msg}");
                _log?.WriteLine(msg, severity);
                StopTokenRefreshTimer();
                _tokenRefreshTimer = new Timer(
                    TokenRefreshCallback, null,
                    TimeSpan.FromSeconds(seconds),
                    Timeout.InfiniteTimeSpan);
            }
        }
    }

    public void Dispose()
    {
        lock (_refreshLock)
        {
            if (_disposed) return;
            _disposed = true;
            StopTokenRefreshTimer();
        }
    }
}
