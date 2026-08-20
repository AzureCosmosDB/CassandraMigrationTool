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

    private readonly object _sync = new();
    private readonly object _refreshLock = new();
    private readonly MigrationLog _log;
    private readonly SourceSessionSettings _settings;
    private readonly HashSet<ISession> _retiredSessions =
        new(ReferenceEqualityComparer.Instance);
    private readonly ConcurrentDictionary<(ISession Session, string Keyspace), Lazy<Task>>
        _udtRegistrations = new();
    private ISession _currentSession;
    private Timer? _tokenRefreshTimer;
    private DateTime _tokenExpiresAt = DateTime.MinValue;
    private int _consecutiveRefreshFailures;
    private int _disposed;

    public SourceSessionWrapper(
        MigrationLog log,
        Job job,
        int workerCount)
    {
        _log = log ?? throw new ArgumentNullException(nameof(log));
        _settings = CassandraClientFactory.ResolveSourceSessionSettings(
            job, workerCount);
        string credential = ResolveCredential(job);
        _currentSession = CreateSession(credential);
        if (job.SourceUseAad)
            ScheduleTokenRefresh(credential);
    }

    public ISession GetSession()
    {
        ObjectDisposedException.ThrowIf(
            Volatile.Read(ref _disposed) != 0,
            this);
        return Volatile.Read(ref _currentSession);
    }

    public async Task<ISession> GetTypedSessionAsync(string keyspace)
    {
        var session = GetSession();
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
            lock (_sync)
            {
                ObjectDisposedException.ThrowIf(
                    Volatile.Read(ref _disposed) != 0,
                    this);

                retiredSession = _currentSession;
                Volatile.Write(ref _currentSession, session);
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

    private static string ResolveCredential(Job job)
    {
        string credential = job.SourcePassword ?? string.Empty;
        if (string.IsNullOrEmpty(credential) || job.SourceUseAad)
        {
            credential = AcquireAadToken();
            // Do not write the bearer token back to SourcePassword. The
            // connection editor would otherwise expose it in the browser DOM.
            job.SourceUseAad = true;
        }

        return credential;
    }

    private static string AcquireAadToken()
    {
        var credential = new Azure.Identity.DefaultAzureCredential();
        return credential.GetToken(
            new Azure.Core.TokenRequestContext(
                new[] { "https://cosmos.azure.com/.default" }))
            .Token;
    }

    private void ScheduleTokenRefresh(string currentToken)
    {
        StopTokenRefreshCore();

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
            if (Volatile.Read(ref _disposed) != 0
                || _tokenRefreshTimer == null)
                return;

            try
            {
                string freshToken = AcquireAadToken();
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

                StopTokenRefreshCore();
                if (Volatile.Read(ref _disposed) == 0)
                {
                    _tokenRefreshTimer = new Timer(
                        RefreshTokenCallback, null,
                        TimeSpan.FromSeconds(seconds),
                        Timeout.InfiniteTimeSpan);
                }
            }
        }
    }

    private void StopTokenRefreshCore()
    {
        _tokenRefreshTimer?.Dispose();
        _tokenRefreshTimer = null;
    }

    public void Dispose()
    {
        List<ISession> sessionsToDispose;
        lock (_refreshLock)
        {
            if (Interlocked.Exchange(ref _disposed, 1) != 0)
                return;
            StopTokenRefreshCore();

            lock (_sync)
            {
                sessionsToDispose = _retiredSessions.ToList();
                _retiredSessions.Clear();
                sessionsToDispose.Add(_currentSession);
                _udtRegistrations.Clear();
            }
        }

        foreach (var session in sessionsToDispose)
        {
            MigrationUtilities.SafeDisposeSession(
                session, "Source session wrapper");
        }
    }
}
