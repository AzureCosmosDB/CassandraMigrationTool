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

    private readonly object _lifecycleLock = new();
    private readonly MigrationLog _log;
    private readonly Action<Exception> _reportFatalFailure;
    private readonly SourceSessionSettings _settings;
    private readonly HashSet<ISession> _retiredSessions =
        new(ReferenceEqualityComparer.Instance);
    private readonly ConcurrentDictionary<(ISession Session, string Keyspace), Lazy<Task>>
        _udtRegistrations = new();
    private ISession _currentSession;
    private Timer? _tokenRefreshTimer;
    private int _disposed;

    public SourceSessionWrapper(
        MigrationLog log,
        Job job,
        int workerCount,
        Action<Exception> reportFatalFailure)
    {
        _log = log ?? throw new ArgumentNullException(nameof(log));
        _reportFatalFailure = reportFatalFailure
            ?? throw new ArgumentNullException(nameof(reportFatalFailure));
        ArgumentNullException.ThrowIfNull(job);
        if (string.IsNullOrEmpty(job.SourceContactPoint))
            throw new ArgumentException(
                "Source contact point is required",
                nameof(job));

        string username = job.SourceUsername ?? string.Empty;
        if (string.IsNullOrWhiteSpace(username)
            && job.SourceUseAad)
        {
            username = job.SourceContactPoint.Split('.')[0];
        }

        int maxConnectionsPerHost =
            CassandraClientFactory.ResolveMaxConnectionsPerHost(
                job.SourceMaxConnectionsPerHost,
                job.MaxConnectionsPerHost);
        if (maxConnectionsPerHost == 0 && workerCount > 0)
        {
            maxConnectionsPerHost = Math.Clamp(
                (workerCount + 31) / 32,
                2,
                8);
        }

        _settings = new SourceSessionSettings(
            job.SourceContactPoint,
            job.SourcePort,
            username,
            maxConnectionsPerHost);
        string credential = ResolveCredential(job);
        _currentSession = CreateSession(credential);
        if (job.SourceUseAad)
            ScheduleTokenRefresh(GetTokenExpiry(credential));
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
        var session = CreateSession(credential);
        var retiredSession = _currentSession;
        Volatile.Write(ref _currentSession, session);
        _retiredSessions.Add(retiredSession);

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
        lock (_lifecycleLock)
        {
            shouldDispose = _retiredSessions.Remove(session);
        }

        if (shouldDispose)
        {
            RemoveUdtRegistrations(session);
            MigrationUtilities.SafeDisposeSession(
                session, "Deferred rotated session");
            _log.WriteLine(
                "Retired AAD source session disposed after the rotation grace period.",
                LogType.Info);
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
        if (string.IsNullOrWhiteSpace(token))
            throw new InvalidOperationException(
                "AAD token acquisition returned an empty token.");

        var handler = new JwtSecurityTokenHandler();
        if (!handler.CanReadToken(token))
            throw new InvalidOperationException(
                "AAD token acquisition returned a token that is not a readable JWT.");

        var expiry = handler.ReadJwtToken(token).ValidTo;
        if (expiry == DateTime.MinValue)
            throw new InvalidOperationException(
                "AAD token does not contain a valid expiration time.");

        return expiry;
    }

    private static string ResolveCredential(Job job)
    {
        if (job.SourceUseAad)
            return AcquireAadToken();

        return job.SourcePassword ?? string.Empty;
    }

    private static string AcquireAadToken()
    {
        var credential = new Azure.Identity.DefaultAzureCredential();
        return credential.GetToken(
            new Azure.Core.TokenRequestContext(
                new[] { "https://cosmos.azure.com/.default" }))
            .Token;
    }

    private void ScheduleTokenRefresh(DateTime expiry)
    {
        StopTokenRefreshCore();

        TimeSpan delay = expiry - DateTime.UtcNow
            - TimeSpan.FromMinutes(5);
        if (delay < TimeSpan.FromMinutes(1))
            delay = TimeSpan.FromMinutes(1);

        _tokenRefreshTimer = new Timer(
            RefreshTokenCallback, null,
            delay, Timeout.InfiniteTimeSpan);
        _log.WriteLine(
            $"AAD source token refresh scheduled for " +
            $"{DateTime.UtcNow.Add(delay):O}; token expires {expiry:O}.",
            LogType.Info);
    }

    private void RefreshTokenCallback(object? state)
    {
        Exception? fatalFailure = null;
        lock (_lifecycleLock)
        {
            if (Volatile.Read(ref _disposed) != 0
                || _tokenRefreshTimer == null)
                return;

            try
            {
                string freshToken = AcquireAadToken();
                DateTime expiry = GetTokenExpiry(freshToken);
                Refresh(freshToken);
                ScheduleTokenRefresh(expiry);
                _log.WriteLine(
                    "AAD source session refreshed successfully.",
                    LogType.Info);
            }
            catch (Exception ex)
            {
                string message =
                    $"AAD token refresh failed. Aborting migration job: {ex.Message}";
                _log.WriteLine(message, LogType.Error);
                StopTokenRefreshCore();
                fatalFailure = new InvalidOperationException(message, ex);
            }
        }

        if (fatalFailure != null)
            _reportFatalFailure(fatalFailure);
    }

    private void StopTokenRefreshCore()
    {
        _tokenRefreshTimer?.Dispose();
        _tokenRefreshTimer = null;
    }

    public void Dispose()
    {
        List<ISession> sessionsToDispose;
        lock (_lifecycleLock)
        {
            if (Interlocked.Exchange(ref _disposed, 1) != 0)
                return;
            StopTokenRefreshCore();

            sessionsToDispose = _retiredSessions.ToList();
            _retiredSessions.Clear();
            sessionsToDispose.Add(_currentSession);
            _udtRegistrations.Clear();
        }

        foreach (var session in sessionsToDispose)
        {
            MigrationUtilities.SafeDisposeSession(
                session, "Source session wrapper");
        }
    }

    private sealed record SourceSessionSettings(
        string ContactPoint,
        int Port,
        string Username,
        int MaxConnectionsPerHost);
}
