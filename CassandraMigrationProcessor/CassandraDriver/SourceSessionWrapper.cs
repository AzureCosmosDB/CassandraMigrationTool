using Cassandra;
using CassandraMigrationProcessor.Infrastructure;
using System.Collections.Concurrent;

namespace CassandraMigrationProcessor.CassandraDriver;

/// <summary>
/// Owns the shared source-session lifecycle and session-scoped UDT mappings.
/// Rotated sessions remain available for a bounded grace period so in-flight
/// operations can complete.
/// </summary>
public sealed class SourceSessionWrapper : IDisposable
{
    private static readonly TimeSpan RetiredSessionDisposalDelay =
        TimeSpan.FromMinutes(10);

    private readonly object _sync = new();
    private readonly ICredentialSessionFactory _sessionFactory;
    private readonly HashSet<ISession> _retiredSessions =
        new(ReferenceEqualityComparer.Instance);
    private readonly ConcurrentDictionary<(ISession Session, string Keyspace), Lazy<Task>>
        _udtRegistrations = new();
    private ISession? _currentSession;
    private bool _disposed;

    public SourceSessionWrapper(ICredentialSessionFactory sessionFactory)
    {
        _sessionFactory = sessionFactory
            ?? throw new ArgumentNullException(nameof(sessionFactory));
    }

    public ISession GetSession()
        => GetCurrentSession();

    public async Task<ISession> GetTypedSessionAsync(string keyspace)
    {
        var session = GetCurrentSession();
        await _udtRegistrations.GetOrAdd(
            (session, keyspace),
            key => new Lazy<Task>(
                () => RegisterUdtsAsync(key.Session, key.Keyspace),
                LazyThreadSafetyMode.ExecutionAndPublication))
            .Value
            .ConfigureAwait(false);
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
            return session;
        }
        catch
        {
            MigrationUtilities.SafeDisposeSession(
                session, "Unpublished initial session");
            throw;
        }
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
            MigrationUtilities.SafeDisposeSession(
                session, "Deferred rotated session");
        }
    }

    public void Dispose()
    {
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
        }

        foreach (var session in sessionsToDispose)
        {
            MigrationUtilities.SafeDisposeSession(
                session, "Source session wrapper");
        }
    }
}
