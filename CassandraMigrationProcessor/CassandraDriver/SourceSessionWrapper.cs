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
internal sealed class SourceSessionWrapper : IDisposable
{
    private static readonly TimeSpan RetiredSessionDisposalDelay =
        TimeSpan.FromMinutes(10);
    private const int MaxRefreshFailures = 6;

    private readonly MigrationLog _log;
    private readonly SourceSessionSettings _settings;
    private readonly SessionState _sessions = new();
    private readonly TokenRefreshState _tokenRefresh = new();

    public SourceSessionWrapper(
        MigrationLog log,
        Job job,
        int workerCount)
    {
        _log = log ?? throw new ArgumentNullException(nameof(log));
        var source = CassandraClientFactory.ResolveSourceSession(
            job, workerCount);
        _settings = source.Settings;
        try
        {
            _sessions.Current = CreateSession(source.Credential);
            if (source.Credential.Length > 200)
                StartTokenRefresh(source.Credential);
        }
        catch
        {
            Dispose();
            throw;
        }
    }

    public ISession GetSession()
    {
        lock (_sessions.Sync)
        {
            ObjectDisposedException.ThrowIf(_sessions.Disposed, this);
            return _sessions.Current
                ?? throw new InvalidOperationException("The session provider has not been initialized.");
        }
    }

    public async Task<ISession> GetTypedSessionAsync(string keyspace)
    {
        var session = GetSession();
        var key = (Session: session, Keyspace: keyspace);
        var registration = _sessions.UdtRegistrations.GetOrAdd(
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
                _sessions.UdtRegistrations).Remove(new KeyValuePair<
                    (ISession Session, string Keyspace), Lazy<Task>>(
                    key, registration));
            throw new SourceUdtRegistrationException(keyspace, ex);
        }
        return session;
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

    private void Refresh(string credential)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(credential);

        var session = CreateSession(credential);
        ISession? retiredSession;
        try
        {
            lock (_sessions.Sync)
            {
                ObjectDisposedException.ThrowIf(_sessions.Disposed, this);
                if (_sessions.Current == null)
                    throw new InvalidOperationException("The session provider has not been initialized.");

                retiredSession = _sessions.Current;
                _sessions.Current = session;
                _sessions.Retired.Add(retiredSession);
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

    private ISession CreateSession(string credential)
    {
        return CassandraClientFactory.CreateSourceSession(
            _log,
            _settings.ContactPoint,
            _settings.Port,
            _settings.Username,
            credential,
            _settings.MaxConnectionsPerHost);
    }

    private async Task DisposeRetiredSessionAfterDelayAsync(ISession session)
    {
        await Task.Delay(RetiredSessionDisposalDelay).ConfigureAwait(false);

        bool shouldDispose;
        lock (_sessions.Sync)
        {
            shouldDispose = _sessions.Retired.Remove(session);
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
        foreach (var key in _sessions.UdtRegistrations.Keys)
        {
            if (ReferenceEquals(key.Session, session))
                _sessions.UdtRegistrations.TryRemove(key, out _);
        }
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
        lock (_tokenRefresh.Sync)
        {
            if (_tokenRefresh.Disposed) return;
            ScheduleTokenRefresh(currentToken);
        }
    }

    private void ScheduleTokenRefresh(string currentToken)
    {
        StopTokenRefreshCore();

        DateTime expiry = GetTokenExpiry(currentToken);
        if (expiry == DateTime.MaxValue)
            expiry = DateTime.UtcNow.AddMinutes(50);

        _tokenRefresh.ExpiresAt = expiry;

        TimeSpan delay = expiry - DateTime.UtcNow
            - TimeSpan.FromMinutes(5);
        if (delay < TimeSpan.FromMinutes(1))
            delay = TimeSpan.FromMinutes(1);

        _tokenRefresh.Timer = new Timer(
            RefreshTokenCallback, null,
            delay, Timeout.InfiniteTimeSpan);
    }

    private void RefreshTokenCallback(object? state)
    {
        lock (_tokenRefresh.Sync)
        {
            if (_tokenRefresh.Disposed || _tokenRefresh.Timer == null) return;

            try
            {
                string freshToken = CassandraClientFactory.AcquireAadToken();
                Refresh(freshToken);

                _tokenRefresh.ConsecutiveFailures = 0;
                ScheduleTokenRefresh(freshToken);
            }
            catch (Exception ex)
            {
                _tokenRefresh.ConsecutiveFailures++;
                int seconds = Math.Min(
                    300,
                    30 * (1 << Math.Min(
                        _tokenRefresh.ConsecutiveFailures - 1, 4)));
                bool tokenAlreadyExpired =
                    DateTime.UtcNow >= _tokenRefresh.ExpiresAt;
                LogType severity =
                    _tokenRefresh.ConsecutiveFailures >= MaxRefreshFailures
                    || tokenAlreadyExpired
                        ? LogType.Error
                        : LogType.Warning;
                string message =
                    $"Token refresh failed (attempt {_tokenRefresh.ConsecutiveFailures}, " +
                    $"retrying in {seconds}s, tokenExpiresAt={_tokenRefresh.ExpiresAt:O}): " +
                    ex.Message;
                Console.WriteLine($"[{severity}] {message}");
                _log.WriteLine(message, severity);

                StopTokenRefreshCore();
                if (!_tokenRefresh.Disposed)
                {
                    _tokenRefresh.Timer = new Timer(
                        RefreshTokenCallback, null,
                        TimeSpan.FromSeconds(seconds),
                        Timeout.InfiniteTimeSpan);
                }
            }
        }
    }

    private void StopTokenRefreshCore()
    {
        _tokenRefresh.Timer?.Dispose();
        _tokenRefresh.Timer = null;
    }

    public void Dispose()
    {
        lock (_tokenRefresh.Sync)
        {
            if (!_tokenRefresh.Disposed)
            {
                _tokenRefresh.Disposed = true;
                StopTokenRefreshCore();
            }
        }

        List<ISession> sessionsToDispose;
        lock (_sessions.Sync)
        {
            if (_sessions.Disposed) return;
            _sessions.Disposed = true;
            sessionsToDispose = _sessions.Retired.ToList();
            _sessions.Retired.Clear();
            if (_sessions.Current != null)
                sessionsToDispose.Add(_sessions.Current);
            _sessions.Current = null;
            _sessions.UdtRegistrations.Clear();
        }

        foreach (var session in sessionsToDispose)
        {
            MigrationUtilities.SafeDisposeSession(
                session, "Source session wrapper");
        }
    }

    private sealed class SessionState
    {
        public object Sync { get; } = new();
        public HashSet<ISession> Retired { get; } =
            new(ReferenceEqualityComparer.Instance);
        public ConcurrentDictionary<
            (ISession Session, string Keyspace),
            Lazy<Task>> UdtRegistrations { get; } = new();
        public ISession? Current { get; set; }
        public bool Disposed { get; set; }
    }

    private sealed class TokenRefreshState
    {
        public object Sync { get; } = new();
        public Timer? Timer { get; set; }
        public DateTime ExpiresAt { get; set; } = DateTime.MinValue;
        public int ConsecutiveFailures { get; set; }
        public bool Disposed { get; set; }
    }
}
