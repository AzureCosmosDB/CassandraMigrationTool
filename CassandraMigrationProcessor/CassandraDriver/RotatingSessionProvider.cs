using Cassandra;
using CassandraMigrationProcessor.Infrastructure;

namespace CassandraMigrationProcessor.CassandraDriver;

/// <summary>
/// Resolves the current shared session while retaining rotated sessions for a
/// bounded grace period so in-flight operations can complete.
/// </summary>
public interface ISessionProvider
{
    ISession GetSession();
}

public sealed class RotatingSessionProvider : ISessionProvider, IDisposable
{
    private static readonly TimeSpan RetiredSessionDisposalDelay =
        TimeSpan.FromMinutes(10);

    private readonly object _sync = new();
    private readonly Func<string, ISession> _sessionFactory;
    private readonly HashSet<ISession> _retiredSessions =
        new(ReferenceEqualityComparer.Instance);
    private ISession? _currentSession;
    private bool _disposed;

    public RotatingSessionProvider(Func<string, ISession> sessionFactory)
    {
        _sessionFactory = sessionFactory
            ?? throw new ArgumentNullException(nameof(sessionFactory));
    }

    public ISession GetSession()
    {
        lock (_sync)
        {
            ObjectDisposedException.ThrowIf(_disposed, this);
            return _currentSession
                ?? throw new InvalidOperationException("The session provider has not been initialized.");
        }
    }

    public void Initialize(ISession session)
    {
        ArgumentNullException.ThrowIfNull(session);

        lock (_sync)
        {
            ObjectDisposedException.ThrowIf(_disposed, this);
            if (_currentSession != null)
                throw new InvalidOperationException("The session provider is already initialized.");
            _currentSession = session;
        }
    }

    public void Refresh(string credential)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(credential);

        var session = _sessionFactory(credential);
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
                session, "Rotating session provider");
        }
    }
}
