using Cassandra;
using CassandraMigrationProcessor.Infrastructure;
using CassandraMigrationProcessor.Models;
using System.Collections.Concurrent;
using System.IdentityModel.Tokens.Jwt;

namespace CassandraMigrationProcessor.CassandraDriver;

internal sealed class SourceUdtRegistrationException : Exception
{
    public SourceUdtRegistrationException(string keyspace, Exception innerException)
        : base($"UDT mapping registration failed for source keyspace '{keyspace}'.", innerException)
    {
    }
}

/// <summary>
/// Owns the shared source-session lifecycle and session-scoped UDT mappings.
/// Rotated sessions remain available for a bounded grace period so in-flight
/// operations can complete.
/// </summary>
public sealed class SourceSessionWrapper : IDisposable
{
    private static readonly TimeSpan RetiredSessionDisposalDelay =
        TimeSpan.FromMinutes(10);
    private const int MaxRefreshFailures = 6;

    private readonly object _sync = new();
    private readonly object _refreshLock = new();
    private readonly MigrationLog _log;
    private readonly ICredentialSessionFactory _sessionFactory;
    private readonly HashSet<ISession> _retiredSessions =
        new(ReferenceEqualityComparer.Instance);
    private readonly ConcurrentDictionary<(ISession Session, string Keyspace), Lazy<Task>>
        _udtRegistrations = new();
    private ISession? _currentSession;
    private Timer? _tokenRefreshTimer;
    private DateTime _tokenExpiresAt = DateTime.MinValue;
    private int _consecutiveRefreshFailures;
    private bool _tokenRefreshEnabled;
    private bool _tokenRefreshDisposed;
    private bool _disposed;

    public SourceSessionWrapper(
        MigrationLog log,
        ICredentialSessionFactory sessionFactory)
    {
        _log = log ?? throw new ArgumentNullException(nameof(log));
        _sessionFactory = sessionFactory
            ?? throw new ArgumentNullException(nameof(sessionFactory));
    }

    public ISession GetSession()
    {
        return GetCurrentSession();
    }

    public async Task<ISession> GetTypedSessionAsync(string keyspace)
    {
        var session = GetCurrentSession();
        var key = (Session: session, Keyspace: keyspace);
        var registration = _udtRegistrations.GetOrAdd(
            key,
            key => new Lazy<Task>(
                () => RegisterUdtsAsync(key.Session, key.Keyspace),
                LazyThreadSafetyMode.ExecutionAndPublication));
        try
        {
            await registration.Value.ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            ((ICollection<KeyValuePair<(ISession Session, string Keyspace), Lazy<Task>>>)
                _udtRegistrations).Remove(new KeyValuePair<
                    (ISession Session, string Keyspace), Lazy<Task>>(
                    key, registration));
            throw new SourceUdtRegistrationException(keyspace, ex);
        }
        return session;
    }

    private ISession GetCurrentSession()
    {
        lock (_sync)
        {
            ObjectDisposedException.ThrowIf(_disposed, this);
            return _currentSession
                ?? throw new InvalidOperationException("The session provider has not been initialized.");
        }
    }

    private static async Task RegisterUdtsAsync(
        ISession session,
        string keyspace)
    {
        var allUdts = await SchemaManager.GetUserDefinedTypesAsync(
            session, keyspace);
        await DynamicUdtRegistrar.RegisterAsync(
            session, keyspace, allUdts);
    }

    public ISession Initialize(string credential)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(credential);

        var session = _sessionFactory.CreateSession(credential);
        try
        {
            lock (_sync)
            {
                ObjectDisposedException.ThrowIf(_disposed, this);
                if (_currentSession != null)
                    throw new InvalidOperationException("The session provider is already initialized.");
                _currentSession = session;
            }
        }
        catch
        {
            MigrationUtilities.SafeDisposeSession(
                session, "Unpublished initial session");
            throw;
        }

        if (IsLikelyAadToken(credential))
            StartTokenRefresh(credential);
        return session;
    }

    public void Refresh(string credential)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(credential);

        var session = _sessionFactory.CreateSession(credential);
        ISession? retiredSession;
        try
        {
            lock (_sync)
            {
                ObjectDisposedException.ThrowIf(_disposed, this);
                if (_currentSession == null)
                    throw new InvalidOperationException("The session provider has not been initialized.");

                retiredSession = _currentSession;
                _currentSession = session;
                _retiredSessions.Add(retiredSession);
            }
        }
        catch
        {
            MigrationUtilities.SafeDisposeSession(
                session, "Unpublished refreshed session");
            throw;
        }

        _ = DisposeRetiredSessionAfterDelayAsync(retiredSession);
    }

    private async Task DisposeRetiredSessionAfterDelayAsync(ISession session)
    {
        await Task.Delay(RetiredSessionDisposalDelay).ConfigureAwait(false);

        bool shouldDispose;
        lock (_sync)
        {
            shouldDispose = _retiredSessions.Remove(session);
        }

        if (shouldDispose)
        {
            RemoveUdtRegistrations(session);
            MigrationUtilities.SafeDisposeSession(
                session, "Deferred rotated session");
        }
    }

    private void RemoveUdtRegistrations(ISession session)
    {
        foreach (var key in _udtRegistrations.Keys)
        {
            if (ReferenceEquals(key.Session, session))
                _udtRegistrations.TryRemove(key, out _);
        }
    }

    private static bool IsLikelyAadToken(string? credential)
    {
        return credential != null && credential.Length > 200;
    }

    private static DateTime GetTokenExpiry(string token)
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
            Console.WriteLine(
                $"[Warning] Failed to read AAD token expiry: {ex.Message}");
        }

        return DateTime.MaxValue;
    }

    private void StartTokenRefresh(string currentToken)
    {
        lock (_refreshLock)
        {
            if (_tokenRefreshDisposed) return;
            _tokenRefreshEnabled = true;
            ScheduleTokenRefresh(currentToken);
        }
    }

    private void ScheduleTokenRefresh(string currentToken)
    {
        _tokenRefreshTimer?.Dispose();

        DateTime expiry = GetTokenExpiry(currentToken);
        if (expiry == DateTime.MaxValue)
            expiry = DateTime.UtcNow.AddMinutes(50);

        _tokenExpiresAt = expiry;

        TimeSpan delay = expiry - DateTime.UtcNow
            - TimeSpan.FromMinutes(5);
        if (delay < TimeSpan.FromMinutes(1))
            delay = TimeSpan.FromMinutes(1);

        _tokenRefreshTimer = new Timer(
            RefreshTokenCallback, null,
            delay, Timeout.InfiniteTimeSpan);
    }

    private void RefreshTokenCallback(object? state)
    {
        lock (_refreshLock)
        {
            if (_tokenRefreshDisposed || !_tokenRefreshEnabled) return;

            try
            {
                string freshToken = CassandraClientFactory.AcquireAadToken();
                Refresh(freshToken);

                _consecutiveRefreshFailures = 0;
                ScheduleTokenRefresh(freshToken);
            }
            catch (Exception ex)
            {
                _consecutiveRefreshFailures++;
                int seconds = Math.Min(
                    300,
                    30 * (1 << Math.Min(
                        _consecutiveRefreshFailures - 1, 4)));
                bool tokenAlreadyExpired =
                    DateTime.UtcNow >= _tokenExpiresAt;
                LogType severity =
                    _consecutiveRefreshFailures >= MaxRefreshFailures
                    || tokenAlreadyExpired
                        ? LogType.Error
                        : LogType.Warning;
                string message =
                    $"Token refresh failed (attempt {_consecutiveRefreshFailures}, " +
                    $"retrying in {seconds}s, tokenExpiresAt={_tokenExpiresAt:O}): " +
                    ex.Message;
                Console.WriteLine($"[{severity}] {message}");
                _log.WriteLine(message, severity);

                _tokenRefreshTimer?.Dispose();
                if (_tokenRefreshEnabled && !_tokenRefreshDisposed)
                {
                    _tokenRefreshTimer = new Timer(
                        RefreshTokenCallback, null,
                        TimeSpan.FromSeconds(seconds),
                        Timeout.InfiniteTimeSpan);
                }
            }
        }
    }

    public void StopTokenRefresh()
    {
        lock (_refreshLock)
        {
            _tokenRefreshEnabled = false;
            _tokenRefreshTimer?.Dispose();
            _tokenRefreshTimer = null;
        }
    }

    public void Dispose()
    {
        lock (_refreshLock)
        {
            if (_tokenRefreshDisposed) return;
            _tokenRefreshDisposed = true;
            _tokenRefreshEnabled = false;
            _tokenRefreshTimer?.Dispose();
            _tokenRefreshTimer = null;
        }

        List<ISession> sessionsToDispose;
        lock (_sync)
        {
            if (_disposed) return;
            _disposed = true;
            sessionsToDispose = _retiredSessions.ToList();
            _retiredSessions.Clear();
            if (_currentSession != null)
                sessionsToDispose.Add(_currentSession);
            _currentSession = null;
            _udtRegistrations.Clear();
        }

        foreach (var session in sessionsToDispose)
        {
            MigrationUtilities.SafeDisposeSession(
                session, "Source session wrapper");
        }
    }
}
