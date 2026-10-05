using System.Collections.Immutable;
using System.Data;
using JasperFx.Core;
using JasperFx.Events.Daemon;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;

namespace Weasel.SqlServer;

/// <summary>
///     SQL Server implementation of <see cref="IAdvisoryLock" />. The contract was
///     originally a duplicate in <c>Weasel.Core.IAdvisoryLock</c> (byte-identical
///     to the upstream JasperFx.Events one); it was lifted into
///     <c>JasperFx.Events.Daemon</c> in jasperfx alpha.19 / PR #319 so the daemon
///     contracts have a single canonical home, and Weasel's duplicate was removed
///     in weasel#284. Existing consumers should update their <c>using</c>
///     statement from <c>Weasel.Core</c> to <c>JasperFx.Events.Daemon</c>.
/// </summary>
public class AdvisoryLock : IAdvisoryLock
{
    private const int ProbeTimeoutSeconds = 5;

    private readonly Func<SqlConnection> _source;
    private readonly ILogger _logger;
    private readonly string _databaseName;
    private readonly TimeSpan _monitoringInterval;

    // The monitor shares the connection, and SqlConnection does not support concurrent commands
    private readonly SemaphoreSlim _gate = new(1, 1);

    private volatile SqlConnection? _conn;
    private volatile ImmutableHashSet<int> _locks = ImmutableHashSet<int>.Empty;
    private PeriodicTimer? _monitorTimer;
    private Task? _monitor;
    private volatile bool _disposed;

    public AdvisoryLock(Func<SqlConnection> source, ILogger logger, string databaseName)
        : this(source, logger, databaseName, 5.Seconds())
    {
    }

    internal AdvisoryLock(Func<SqlConnection> source, ILogger logger, string databaseName, TimeSpan monitoringInterval)
    {
        _source = source;
        _logger = logger;
        _databaseName = databaseName;
        _monitoringInterval = monitoringInterval;
    }

    public bool HasLock(int lockId)
    {
        return _conn is { State: ConnectionState.Open } && _locks.Contains(lockId);
    }

    public async Task<bool> TryAttainLockAsync(int lockId, CancellationToken token)
    {
        // weasel#349: never start a new acquire once disposal has begun. During host shutdown opening _conn
        // races with disposal and can abort with a process-killing ObjectDisposedException — mirror the
        // Postgres guard for Polecat parity.
        if (_disposed) return false;

        try
        {
            await _gate.WaitAsync(token).ConfigureAwait(false);
            try
            {
                if (_disposed) return false;

                var conn = await connectAsync(token).ConfigureAwait(false);

                // Session locks are reentrant: taking it again would take a second release to free it
                if (_locks.Contains(lockId)) return true;

                try
                {
                    if (!await conn.TryGetGlobalLock(lockId.ToString(), cancellation: token).ConfigureAwait(false))
                    {
                        return false;
                    }
                }
                catch (Exception e)
                {
                    // A dead session, or a cancel that may have landed after the grant: ending the session leaves nothing in doubt
                    await endSessionAsync(lost: true, e).ConfigureAwait(false);
                    throw;
                }

                _locks = _locks.Add(lockId);
                startMonitoring();

                return true;
            }
            finally
            {
                _gate.Release();
            }
        }
        catch (ObjectDisposedException)
        {
            // The connection was disposed out from under an in-flight open/acquire during shutdown. Treat as
            // "lock not attained" rather than letting a process-aborting exception escape.
            return false;
        }
        catch (Exception e) when (_disposed && e is SqlException or InvalidOperationException)
        {
            // Same shutdown race, surfaced as a disposed-connection SqlException / InvalidOperationException.
            return false;
        }
    }

    /// <summary>
    ///     Who holds <paramref name="lockId" />, or null when nothing does.
    /// </summary>
    /// <remarks>
    ///     <para>
    ///         <see cref="HasLock" /> only answers "does this node", so without this no node — and no
    ///         monitoring tool — could learn which node owns a lock set it does not hold. That is the
    ///         diagnostic an operator needs when a projection agent stops with
    ///         <c>ProgressionProgressOutOfOrderException</c>, which means two processes believe they own
    ///         the same shard (weasel#650).
    ///     </para>
    ///     <para>
    ///         <c>sp_getapplock</c> takes the id as a string resource, and
    ///         <c>sys.dm_tran_locks.resource_description</c> reports it wrapped as
    ///         <c>0:[4242]:(1bb08fa7)</c> — the database principal, the resource name in brackets, and a
    ///         hash. Measured against the server. Matching the bracketed name rather than the whole
    ///         string keeps the principal and hash out of it, and the read is confined to the current
    ///         database because an application lock is per-database.
    ///     </para>
    ///     <para>
    ///         Read on its own connection rather than the one holding the locks, which may be busy. The
    ///         dynamic management views need <c>VIEW SERVER STATE</c>; without it this throws rather than
    ///         reporting the lock as unheld, since "nothing holds this" is precisely the reassuring
    ///         conclusion an operator chasing a double-runner must not be handed by accident.
    ///     </para>
    /// </remarks>
    public async Task<AdvisoryLockHolder?> FindHolderAsync(int lockId, CancellationToken token)
    {
        await using var conn = _source();
        await conn.OpenAsync(token).ConfigureAwait(false);

        await using var cmd = conn.CreateCommand();
        cmd.CommandText = """
                          SELECT TOP 1 l.request_session_id, s.program_name, s.login_time, c.client_net_address
                          FROM sys.dm_tran_locks l
                          LEFT JOIN sys.dm_exec_sessions s ON s.session_id = l.request_session_id
                          LEFT JOIN sys.dm_exec_connections c ON c.session_id = l.request_session_id
                          WHERE l.resource_type = 'APPLICATION'
                            AND l.resource_database_id = DB_ID()
                            AND l.request_status = 'GRANT'
                            AND CHARINDEX(':[' + @resource + ']:', l.resource_description) > 0
                          """;

        var resource = cmd.CreateParameter();
        resource.ParameterName = "resource";
        resource.Value = lockId.ToString();
        cmd.Parameters.Add(resource);

        await using var reader = await cmd.ExecuteReaderAsync(token).ConfigureAwait(false);
        if (!await reader.ReadAsync(token).ConfigureAwait(false))
        {
            return null;
        }

        return new AdvisoryLockHolder(lockId)
        {
            SessionId = await reader.IsDBNullAsync(0, token).ConfigureAwait(false)
                ? null
                : (await reader.GetFieldValueAsync<int>(0, token).ConfigureAwait(false)).ToString(),
            ApplicationName = await nullableStringAsync(reader, 1, token).ConfigureAwait(false),

            // The session's login time, which is the earliest the lock can have been taken: SQL Server
            // does not record when an application lock was acquired
            HeldSince = await reader.IsDBNullAsync(2, token).ConfigureAwait(false)
                ? null
                : new DateTimeOffset(await reader.GetFieldValueAsync<DateTime>(2, token).ConfigureAwait(false),
                    TimeSpan.Zero),
            ClientAddress = await nullableStringAsync(reader, 3, token).ConfigureAwait(false),

            // Definitive either way for this instance: the lock is exclusive, so if this instance holds
            // it the single holder is us, and if it does not, the holder is not us
            IsCurrentNode = HasLock(lockId)
        };
    }

    private static async Task<string?> nullableStringAsync(SqlDataReader reader, int ordinal, CancellationToken token)
    {
        return await reader.IsDBNullAsync(ordinal, token).ConfigureAwait(false)
            ? null
            : await reader.GetFieldValueAsync<string>(ordinal, token).ConfigureAwait(false);
    }

    public async Task ReleaseLockAsync(int lockId)
    {
        await _gate.WaitAsync().ConfigureAwait(false);
        try
        {
            if (!_locks.Contains(lockId)) return;

            if (_conn is not { State: ConnectionState.Open } conn)
            {
                await endSessionAsync(lost: true).ConfigureAwait(false);
                return;
            }

            var cancellation = new CancellationTokenSource();
            cancellation.CancelAfter(1.Seconds());

            try
            {
                await conn.ReleaseGlobalLock(lockId.ToString(), cancellation: cancellation.Token).ConfigureAwait(false);
            }
            catch (Exception e)
            {
                // Ending the session releases this lock whatever went wrong, and the others with it
                await endSessionAsync(lost: true, e).ConfigureAwait(false);
                return;
            }

            _locks = _locks.Remove(lockId);

            if (_locks.IsEmpty)
            {
                await endSessionAsync(lost: false).ConfigureAwait(false);
            }
        }
        finally
        {
            _gate.Release();
        }
    }

    public async ValueTask DisposeAsync()
    {
        // Set first, before disposing the connection, so any concurrent TryAttainLockAsync short-circuits (weasel#349).
        _disposed = true;

        Task? monitor;
        await _gate.WaitAsync().ConfigureAwait(false);
        try
        {
            monitor = _monitor;
            _monitorTimer?.Dispose();

            if (_conn is { State: ConnectionState.Open } conn)
            {
                foreach (var lockId in _locks)
                {
                    try
                    {
                        await conn.ReleaseGlobalLock(lockId.ToString(), CancellationToken.None).ConfigureAwait(false);
                    }
                    catch (Exception e)
                    {
                        // Closing the connection below ends the session, which releases it anyway
                        _logger.LogDebug(e, "Unable to release advisory lock {LockId} for database {Identifier}",
                            lockId, _databaseName);
                    }
                }
            }

            await endSessionAsync(lost: false).ConfigureAwait(false);
        }
        finally
        {
            _gate.Release();
        }

        if (monitor != null)
        {
            await monitor.ConfigureAwait(false);
        }
    }

    // Callers hold _gate
    private async Task<SqlConnection> connectAsync(CancellationToken token)
    {
        if (_conn is { State: ConnectionState.Open } open) return open;

        await endSessionAsync(lost: true).ConfigureAwait(false);

        var conn = _source();
        try
        {
            // Unpooled and never silently reconnected, so this connection and the session holding the locks end together
            conn.ConnectionString = new SqlConnectionStringBuilder(conn.ConnectionString)
            {
                Pooling = false,
                ConnectRetryCount = 0
            }.ConnectionString;

            await conn.OpenAsync(token).ConfigureAwait(false);
        }
        catch
        {
            await conn.DisposeAsync().ConfigureAwait(false);
            throw;
        }

        _conn = conn;
        return conn;
    }

    // Callers hold _gate. Every lock belongs to the connection's session, so SQL Server releases them all when it ends.
    private async Task endSessionAsync(bool lost, Exception? cause = null)
    {
        var conn = _conn;
        var held = _locks;
        _conn = null;
        _locks = ImmutableHashSet<int>.Empty;

        if (lost && !held.IsEmpty)
        {
            _logger.LogWarning(cause, "Lost advisory locks {LockIds} for database {Identifier}: the session holding them has ended",
                held, _databaseName);
        }

        if (conn == null) return;

        try
        {
            await conn.DisposeAsync().ConfigureAwait(false);
        }
        catch (Exception e)
        {
            _logger.LogDebug(e, "Error disposing the advisory lock connection for database {Identifier}", _databaseName);
        }
    }

    // Callers hold _gate
    private void startMonitoring()
    {
        if (_monitor != null || _disposed) return;

        _monitorTimer = new PeriodicTimer(_monitoringInterval);
        _monitor = monitorAsync(_monitorTimer);
    }

    // A node holding every lock sends nothing on its connection, so only a probe notices the session ending
    private async Task monitorAsync(PeriodicTimer timer)
    {
        while (await timer.WaitForNextTickAsync(CancellationToken.None).ConfigureAwait(false))
        {
            await _gate.WaitAsync(CancellationToken.None).ConfigureAwait(false);
            try
            {
                if (_conn is { } conn && !_locks.IsEmpty)
                {
                    await using var cmd = conn.CreateCommand();
                    cmd.CommandText = "SELECT 1";
                    cmd.CommandTimeout = ProbeTimeoutSeconds;
                    await cmd.ExecuteScalarAsync(CancellationToken.None).ConfigureAwait(false);
                }
            }
            catch (Exception e)
            {
                await endSessionAsync(lost: true, e).ConfigureAwait(false);
            }
            finally
            {
                _gate.Release();
            }
        }
    }
}
