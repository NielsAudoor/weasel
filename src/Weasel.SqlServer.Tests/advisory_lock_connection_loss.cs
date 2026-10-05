using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Shouldly;
using Xunit;

namespace Weasel.SqlServer.Tests;

public class advisory_lock_behavior
{
    private static CancellationToken ct => TestContext.Current.CancellationToken;

    [Fact]
    public async Task attaining_a_lock_it_already_holds_does_not_stack_the_hold()
    {
        await using var theLock = AdvisoryLockTesting.CreateLock(monitored: false);
        var lockId = AdvisoryLockTesting.NextLockId();

        (await theLock.TryAttainLockAsync(lockId, ct)).ShouldBeTrue();
        (await theLock.TryAttainLockAsync(lockId, ct)).ShouldBeTrue();

        await theLock.ReleaseLockAsync(lockId);

        theLock.HasLock(lockId).ShouldBeFalse();
        (await AdvisoryLockTesting.IsFreeAsync(lockId)).ShouldBeTrue();
    }

    [Fact]
    public async Task releasing_one_lock_keeps_the_others_held()
    {
        await using var theLock = AdvisoryLockTesting.CreateLock(monitored: false);
        var released = AdvisoryLockTesting.NextLockId();
        var kept = AdvisoryLockTesting.NextLockId();

        (await theLock.TryAttainLockAsync(released, ct)).ShouldBeTrue();
        (await theLock.TryAttainLockAsync(kept, ct)).ShouldBeTrue();

        await theLock.ReleaseLockAsync(released);

        theLock.HasLock(released).ShouldBeFalse();
        theLock.HasLock(kept).ShouldBeTrue();
        (await AdvisoryLockTesting.IsFreeAsync(released)).ShouldBeTrue();
        (await AdvisoryLockTesting.IsFreeAsync(kept)).ShouldBeFalse();
    }

    [Fact]
    public async Task releasing_a_lock_that_is_not_held_is_a_no_op()
    {
        await using var theLock = AdvisoryLockTesting.CreateLock(monitored: false);
        var held = AdvisoryLockTesting.NextLockId();

        (await theLock.TryAttainLockAsync(held, ct)).ShouldBeTrue();

        await theLock.ReleaseLockAsync(AdvisoryLockTesting.NextLockId());

        theLock.HasLock(held).ShouldBeTrue();
        (await AdvisoryLockTesting.IsFreeAsync(held)).ShouldBeFalse();
    }

    [Fact]
    public async Task disposal_releases_every_held_lock()
    {
        var theLock = AdvisoryLockTesting.CreateLock(monitored: true);
        var lockIds = new[] { AdvisoryLockTesting.NextLockId(), AdvisoryLockTesting.NextLockId(), AdvisoryLockTesting.NextLockId() };

        foreach (var lockId in lockIds)
        {
            (await theLock.TryAttainLockAsync(lockId, ct)).ShouldBeTrue();
        }

        await theLock.DisposeAsync();

        foreach (var lockId in lockIds)
        {
            theLock.HasLock(lockId).ShouldBeFalse();
            (await AdvisoryLockTesting.IsFreeAsync(lockId)).ShouldBeTrue();
        }
    }

    [Fact]
    public async Task monitoring_keeps_a_lock_whose_session_is_alive()
    {
        await using var theLock = AdvisoryLockTesting.CreateLock(monitored: true);
        var lockIds = new[] { AdvisoryLockTesting.NextLockId(), AdvisoryLockTesting.NextLockId() };

        foreach (var lockId in lockIds)
        {
            (await theLock.TryAttainLockAsync(lockId, ct)).ShouldBeTrue();
        }

        await Task.Delay(AdvisoryLockTesting.MonitoringInterval * 10, ct);

        foreach (var lockId in lockIds)
        {
            theLock.HasLock(lockId).ShouldBeTrue();
            (await AdvisoryLockTesting.IsFreeAsync(lockId)).ShouldBeFalse();
        }
    }

    [Fact]
    public async Task disposing_while_the_monitor_is_probing_still_releases_every_lock()
    {
        for (var i = 0; i < 25; i++)
        {
            var logger = new RecordingLogger();
            var theLock = new AdvisoryLock(() => new SqlConnection(ConnectionSource.ConnectionString), logger,
                "Testing", TimeSpan.FromMilliseconds(1));
            var lockIds = new[] { AdvisoryLockTesting.NextLockId(), AdvisoryLockTesting.NextLockId() };

            foreach (var lockId in lockIds)
            {
                (await theLock.TryAttainLockAsync(lockId, ct)).ShouldBeTrue();
            }

            await Task.Delay(i % 5, ct);
            await Should.NotThrowAsync(() => theLock.DisposeAsync().AsTask());

            foreach (var lockId in lockIds)
            {
                (await AdvisoryLockTesting.IsFreeAsync(lockId)).ShouldBeTrue();
            }

            logger.Entries.ShouldNotContain(x => x.Level >= LogLevel.Warning);
        }
    }
}

public class advisory_lock_connection_loss
{
    private static CancellationToken ct => TestContext.Current.CancellationToken;

    [Fact]
    public async Task monitoring_reports_every_lock_lost_with_the_connection()
    {
        await using var theLock = AdvisoryLockTesting.CreateLock(monitored: true);
        var lockIds = new[] { AdvisoryLockTesting.NextLockId(), AdvisoryLockTesting.NextLockId() };

        foreach (var lockId in lockIds)
        {
            (await theLock.TryAttainLockAsync(lockId, ct)).ShouldBeTrue();
            theLock.HasLock(lockId).ShouldBeTrue();
        }

        await AdvisoryLockTesting.KillSessionsHoldingAsync(lockIds);

        foreach (var lockId in lockIds)
        {
            await AdvisoryLockTesting.WaitForAsync(() => !theLock.HasLock(lockId),
                $"HasLock({lockId}) still reports a lock whose connection was killed");
        }
    }

    [Fact]
    public async Task the_public_constructor_notices_a_lost_session()
    {
        await using var theLock = new AdvisoryLock(() => new SqlConnection(ConnectionSource.ConnectionString),
            NullLogger.Instance, "Testing");
        var lockId = AdvisoryLockTesting.NextLockId();

        (await theLock.TryAttainLockAsync(lockId, ct)).ShouldBeTrue();

        await AdvisoryLockTesting.KillSessionsHoldingAsync(lockId);

        await AdvisoryLockTesting.WaitForAsync(() => !theLock.HasLock(lockId),
            "the default monitoring never reported the killed connection lost", TimeSpan.FromSeconds(20));
    }

    [Fact]
    public async Task reattains_a_lock_after_losing_its_connection()
    {
        await using var theLock = AdvisoryLockTesting.CreateLock(monitored: true);
        var lockId = AdvisoryLockTesting.NextLockId();

        (await theLock.TryAttainLockAsync(lockId, ct)).ShouldBeTrue();

        await AdvisoryLockTesting.KillSessionsHoldingAsync(lockId);
        await AdvisoryLockTesting.WaitForAsync(() => !theLock.HasLock(lockId),
            "the killed connection was never reported lost");

        (await theLock.TryAttainLockAsync(lockId, ct)).ShouldBeTrue();

        theLock.HasLock(lockId).ShouldBeTrue();
        (await AdvisoryLockTesting.IsFreeAsync(lockId)).ShouldBeFalse();

        await theLock.ReleaseLockAsync(lockId);
        (await AdvisoryLockTesting.IsFreeAsync(lockId)).ShouldBeTrue();
    }

    [Fact]
    public async Task another_node_can_take_a_lock_lost_with_its_connection()
    {
        await using var theLock = AdvisoryLockTesting.CreateLock(monitored: true);
        await using var otherNode = AdvisoryLockTesting.CreateLock(monitored: true);
        var lockId = AdvisoryLockTesting.NextLockId();

        (await theLock.TryAttainLockAsync(lockId, ct)).ShouldBeTrue();

        await AdvisoryLockTesting.KillSessionsHoldingAsync(lockId);
        await AdvisoryLockTesting.WaitForAsync(() => !theLock.HasLock(lockId),
            "the killed connection was never reported lost");

        (await otherNode.TryAttainLockAsync(lockId, ct)).ShouldBeTrue();
        (await theLock.TryAttainLockAsync(lockId, ct)).ShouldBeFalse();
        theLock.HasLock(lockId).ShouldBeFalse();
    }

    // The Polecat split brain: a node that kept polling for one lock reconnected, and went on reporting
    // the lock it had held on the dead session while another node held it.
    [Fact]
    public async Task a_lock_lost_with_its_connection_is_not_reported_after_reconnecting()
    {
        await using var theLock = AdvisoryLockTesting.CreateLock(monitored: false);
        await using var otherNode = AdvisoryLockTesting.CreateLock(monitored: false);
        var held = AdvisoryLockTesting.NextLockId();
        var polled = AdvisoryLockTesting.NextLockId();

        (await theLock.TryAttainLockAsync(held, ct)).ShouldBeTrue();

        await AdvisoryLockTesting.KillSessionsHoldingAsync(held);
        await AdvisoryLockTesting.AttainEventuallyAsync(theLock, polled);
        (await otherNode.TryAttainLockAsync(held, ct)).ShouldBeTrue();

        theLock.HasLock(held).ShouldBeFalse();
        theLock.HasLock(polled).ShouldBeTrue();
    }

    [Fact]
    public async Task a_cancelled_acquire_ends_the_session_so_nothing_is_left_in_doubt()
    {
        await using var theLock = AdvisoryLockTesting.CreateLock(monitored: false);
        await using var otherNode = AdvisoryLockTesting.CreateLock(monitored: false);
        var contested = AdvisoryLockTesting.NextLockId();
        var held = AdvisoryLockTesting.NextLockId();

        (await otherNode.TryAttainLockAsync(contested, ct)).ShouldBeTrue();
        (await theLock.TryAttainLockAsync(held, ct)).ShouldBeTrue();

        // sp_getapplock waits a full second for the contested lock, so this cancels it mid-flight
        using var cancellation = new CancellationTokenSource(TimeSpan.FromMilliseconds(200));
        await Should.ThrowAsync<Exception>(() => theLock.TryAttainLockAsync(contested, cancellation.Token));

        theLock.HasLock(contested).ShouldBeFalse();
        theLock.HasLock(held).ShouldBeFalse();
        await AdvisoryLockTesting.WaitForFreeAsync(held);

        (await theLock.TryAttainLockAsync(AdvisoryLockTesting.NextLockId(), ct)).ShouldBeTrue();
    }

    [Fact]
    public async Task disposal_ends_the_session_so_a_lock_it_never_tracked_is_freed_too()
    {
        var opened = new List<SqlConnection>();
        var theLock = AdvisoryLockTesting.CreateLock(monitored: false, opened: opened);
        var tracked = AdvisoryLockTesting.NextLockId();
        var untracked = AdvisoryLockTesting.NextLockId();

        (await theLock.TryAttainLockAsync(tracked, ct)).ShouldBeTrue();
        (await opened.Single().TryGetGlobalLock(untracked.ToString(), ct)).ShouldBeTrue();

        await theLock.DisposeAsync();

        (await AdvisoryLockTesting.IsFreeAsync(tracked)).ShouldBeTrue();
        await AdvisoryLockTesting.WaitForFreeAsync(untracked);
    }

    [Fact]
    public async Task releasing_a_lock_the_session_no_longer_holds_does_not_throw()
    {
        var opened = new List<SqlConnection>();
        await using var theLock = AdvisoryLockTesting.CreateLock(monitored: false, opened: opened);
        var lockId = AdvisoryLockTesting.NextLockId();

        (await theLock.TryAttainLockAsync(lockId, ct)).ShouldBeTrue();
        await opened.Single().ReleaseGlobalLock(lockId.ToString(), ct);

        await Should.NotThrowAsync(() => theLock.ReleaseLockAsync(lockId));

        theLock.HasLock(lockId).ShouldBeFalse();
    }

    [Fact]
    public async Task disposing_after_the_session_released_a_lock_still_releases_the_rest()
    {
        var logger = new RecordingLogger();
        var opened = new List<SqlConnection>();
        var theLock = AdvisoryLockTesting.CreateLock(monitored: false, logger, opened);
        var releasedBehindItsBack = AdvisoryLockTesting.NextLockId();
        var kept = AdvisoryLockTesting.NextLockId();

        (await theLock.TryAttainLockAsync(releasedBehindItsBack, ct)).ShouldBeTrue();
        (await theLock.TryAttainLockAsync(kept, ct)).ShouldBeTrue();
        await opened.Single().ReleaseGlobalLock(releasedBehindItsBack.ToString(), ct);

        await theLock.DisposeAsync();

        (await AdvisoryLockTesting.IsFreeAsync(kept)).ShouldBeTrue();
        logger.Entries.ShouldNotContain(x => x.Level >= LogLevel.Warning);
    }

    [Fact]
    public async Task releasing_a_lock_lost_with_its_connection_does_not_throw()
    {
        await using var theLock = AdvisoryLockTesting.CreateLock(monitored: false);
        var lockId = AdvisoryLockTesting.NextLockId();

        (await theLock.TryAttainLockAsync(lockId, ct)).ShouldBeTrue();
        await AdvisoryLockTesting.KillSessionsHoldingAsync(lockId);

        await Should.NotThrowAsync(() => theLock.ReleaseLockAsync(lockId));

        theLock.HasLock(lockId).ShouldBeFalse();
        (await AdvisoryLockTesting.IsFreeAsync(lockId)).ShouldBeTrue();
    }

    [Fact]
    public async Task disposing_after_losing_the_connection_logs_no_error()
    {
        var logger = new RecordingLogger();
        var theLock = AdvisoryLockTesting.CreateLock(monitored: false, logger);
        var lockId = AdvisoryLockTesting.NextLockId();

        (await theLock.TryAttainLockAsync(lockId, ct)).ShouldBeTrue();
        await AdvisoryLockTesting.KillSessionsHoldingAsync(lockId);

        await theLock.DisposeAsync();

        logger.Entries.ShouldNotContain(x => x.Level >= LogLevel.Error);
    }
}

internal static class AdvisoryLockTesting
{
    public static readonly TimeSpan MonitoringInterval = TimeSpan.FromMilliseconds(100);

    // Never handed a pooled session that still holds a lock, which would answer for that session
    private static readonly string NonPooled =
        new SqlConnectionStringBuilder(ConnectionSource.ConnectionString) { Pooling = false }.ConnectionString;

    private static int _lastLockId = 7_300_000;

    public static int NextLockId() => Interlocked.Increment(ref _lastLockId);

    public static AdvisoryLock CreateLock(bool monitored, ILogger? logger = null, List<SqlConnection>? opened = null)
    {
        return new AdvisoryLock(() =>
            {
                var conn = new SqlConnection(ConnectionSource.ConnectionString);
                opened?.Add(conn);
                return conn;
            },
            logger ?? NullLogger.Instance, "Testing", monitored ? MonitoringInterval : TimeSpan.FromHours(1));
    }

    public static async Task<bool> IsFreeAsync(int lockId)
    {
        await using var conn = new SqlConnection(NonPooled);
        await conn.OpenAsync();

        return await conn.TryGetGlobalLock(lockId.ToString());
    }

    // SQL Server releases a closed session's locks as it tears the session down, a moment after the client lets go
    public static Task WaitForFreeAsync(int lockId) =>
        WaitForAsync(() => IsFreeAsync(lockId).GetAwaiter().GetResult(), $"lock {lockId} was never released");

    public static async Task<short[]> SessionsHoldingAsync(params int[] lockIds)
    {
        await using var conn = new SqlConnection(NonPooled);
        await conn.OpenAsync();

        var sessions = new HashSet<short>();
        foreach (var lockId in lockIds)
        {
            await using var cmd = conn.CreateCommand();
            cmd.CommandText = """
                              SELECT request_session_id FROM sys.dm_tran_locks
                              WHERE resource_type = 'APPLICATION'
                                AND resource_database_id = DB_ID()
                                AND request_status = 'GRANT'
                                AND CHARINDEX(':[' + @resource + ']:', resource_description) > 0
                              """;
            cmd.With("resource", lockId.ToString());

            await using var reader = await cmd.ExecuteReaderAsync();
            while (await reader.ReadAsync())
            {
                sessions.Add((short)reader.GetInt32(0));
            }
        }

        return sessions.ToArray();
    }

    public static async Task KillSessionsHoldingAsync(params int[] lockIds)
    {
        var sessions = await SessionsHoldingAsync(lockIds);
        sessions.ShouldNotBeEmpty();

        await using var conn = new SqlConnection(NonPooled);
        await conn.OpenAsync();

        foreach (var session in sessions)
        {
            await using var cmd = conn.CreateCommand();
            cmd.CommandText = $"KILL {session}";
            await cmd.ExecuteNonQueryAsync();
        }

        await WaitForAsync(() => SessionsHoldingAsync(lockIds).GetAwaiter().GetResult().Length == 0,
            "the killed sessions still hold their locks");
    }

    // The first acquire after the kill is the one that discovers the dead connection, and throws doing so
    public static async Task AttainEventuallyAsync(AdvisoryLock theLock, int lockId)
    {
        for (var attempt = 0; attempt < 3; attempt++)
        {
            try
            {
                if (await theLock.TryAttainLockAsync(lockId, CancellationToken.None)) return;
            }
            catch (SqlException)
            {
            }
        }

        throw new Exception($"Lock {lockId} was never attained after the connection was killed");
    }

    public static async Task WaitForAsync(Func<bool> condition, string failureMessage, TimeSpan? timeout = null)
    {
        var deadline = DateTime.UtcNow.Add(timeout ?? TimeSpan.FromSeconds(10));
        while (!condition())
        {
            if (DateTime.UtcNow > deadline) throw new TimeoutException(failureMessage);
            await Task.Delay(50);
        }
    }
}

internal sealed class RecordingLogger : ILogger
{
    public List<(LogLevel Level, string Message, Exception? Exception)> Entries { get; } = new();

    public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

    public bool IsEnabled(LogLevel logLevel) => true;

    public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception,
        Func<TState, Exception?, string> formatter)
    {
        lock (Entries)
        {
            Entries.Add((logLevel, formatter(state, exception), exception));
        }
    }
}
