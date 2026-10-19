using System.Text.Json;
using AirFlash.Core;
using Xunit;
namespace AirFlash.Tests;
public class DeviceVolumeTests
{
    private static JsonElement Sample(int? volume, long sequence = 0, string status = "confirmed", bool available = true)
        => JsonSerializer.SerializeToElement(new { volume, sequence, status, available });
    private static JsonElement Members((string host, int? volume, string status)[] entries, long sequence = 0, string status = "confirmed", bool available = true)
        => JsonSerializer.SerializeToElement(new { volume = entries.Length > 0 ? entries[0].volume : null, sequence, status, available, members = entries.Select(e => new { host = e.host, volume = e.volume, status = e.status }).ToArray() });
    [Fact]
    public void InitialStateIsUnknownAndPhysicalChangesAreReadBack()
    {
        var state = new DeviceVolumeState();
        Assert.Null(state.Display); Assert.False(state.Available);
        state = state.Receive(Sample(28)); Assert.Equal(28, state.Display);
        state = state.Receive(Sample(33)); Assert.Equal(33, state.Display); Assert.Equal(33, state.LastNonZero);
    }
    [Fact]
    public void OldQueriesCannotOverwriteLatestDragOrConfirmIt()
    {
        var state = new DeviceVolumeState().Receive(Sample(28)).Begin(45, 1).Begin(60, 2);
        Assert.Equal(state, state.Receive(Sample(28)));
        Assert.Equal(state, state.Receive(Sample(45, 1)));
        state = state.Receive(Sample(28, 2, "pending")); Assert.Equal(60, state.Display); Assert.Equal(28, state.Actual);
        state = state.Receive(Sample(60, 2)); Assert.Null(state.Target); Assert.Equal(60, state.Display);
    }
    [Fact]
    public void TimeoutUsesActualReadAndLatePendingDoesNotResurrectTarget()
    {
        var state = new DeviceVolumeState().Receive(Sample(28)).Begin(60, 1).Receive(Sample(35, 1, "pending")).Timeout();
        Assert.Equal(35, state.Display); Assert.Equal("unconfirmed", state.Status);
        state = state.Receive(Sample(38, 1, "pending")); Assert.Null(state.Target); Assert.Equal(38, state.Display);
    }
    [Fact]
    public void MissingReadsDisableControlWithoutInventingDefaultVolume()
    {
        var state = new DeviceVolumeState().Receive(Sample(null, status: "unsynced", available: false));
        Assert.Null(state.Display); Assert.False(state.Available);
        state = state.Receive(Sample(28)).Begin(60, 1).Timeout();
        Assert.False(state.Available); Assert.Null(state.Target);
    }
    [Fact]
    public void MuteKeepsLastConfirmedNonzeroAndNeverInventsRestoreValue()
    {
        var state = new DeviceVolumeState().Receive(Sample(0)); Assert.Null(state.LastNonZero);
        state = state.Receive(Sample(28)).Begin(0, 1).Receive(Sample(0, 1));
        Assert.Equal(0, state.Display); Assert.Equal(28, state.LastNonZero);
    }
    [Fact]
    public void MembersTrackEachDeviceIndependently()
    {
        var pair = new (string, int?, string)[] { ("127.0.0.1", 28, "confirmed"), ("127.0.0.2", 25, "confirmed") };
        var state = new DeviceVolumeState().Receive(Members(pair));
        Assert.Equal(2, state.Members!.Count);
        Assert.Equal(28, state.Member("127.0.0.1")!.Display);
        Assert.Equal(25, state.Member("127.0.0.2")!.Display);
        Assert.True(state.Member("127.0.0.2")!.Available);
        // The receiver-wide card follows the session leader.
        Assert.Equal(28, state.Display);
        // A physical change on one device does not touch the other.
        state = state.Receive(Members([("127.0.0.1", 28, "confirmed"), ("127.0.0.2", 40, "confirmed")]));
        Assert.Equal(40, state.Member("127.0.0.2")!.Display);
        Assert.Equal(28, state.Member("127.0.0.1")!.Display);
        Assert.Equal(28, state.Display);
    }
    [Fact]
    public void TargetedEditWaitsForItsOwnDeviceOnly()
    {
        var state = new DeviceVolumeState().Receive(Members([("127.0.0.1", 28, "confirmed"), ("127.0.0.2", 25, "confirmed")]))
            .Begin(80, 1, "127.0.0.2");
        // The receiver-wide card keeps its own value and status.
        Assert.Null(state.Target);
        Assert.Equal(28, state.Display);
        Assert.Equal(80, state.Member("127.0.0.2")!.Target);
        Assert.Equal("pending", state.Member("127.0.0.2")!.Status);
        // The leader stays confirmed while the targeted device waits.
        state = state.Receive(Members([("127.0.0.1", 28, "confirmed"), ("127.0.0.2", 25, "pending")], sequence: 1));
        Assert.Equal(80, state.Member("127.0.0.2")!.Display);
        Assert.Null(state.Target);
        state = state.Receive(Members([("127.0.0.1", 28, "confirmed"), ("127.0.0.2", 80, "confirmed")], sequence: 1));
        Assert.Equal(80, state.Member("127.0.0.2")!.Display);
        Assert.Null(state.Member("127.0.0.2")!.Target);
        Assert.Equal("confirmed", state.Member("127.0.0.2")!.Status);
    }
    [Fact]
    public void TargetedTimeoutLeavesOtherDevicesAlone()
    {
        var state = new DeviceVolumeState().Receive(Members([("127.0.0.1", 28, "confirmed"), ("127.0.0.2", 25, "confirmed")]))
            .Begin(80, 1, "127.0.0.2").Timeout("127.0.0.2");
        Assert.Equal("unconfirmed", state.Member("127.0.0.2")!.Status);
        Assert.Null(state.Member("127.0.0.2")!.Target);
        Assert.Equal("confirmed", state.Member("127.0.0.1")!.Status);
        // The receiver-wide card is untouched by a targeted timeout.
        Assert.Equal("confirmed", state.Status);
        Assert.True(state.Available);
    }
    [Fact]
    public void LeaderTargetedEditDrivesTheReceiverWideCard()
    {
        var state = new DeviceVolumeState().Receive(Members([("127.0.0.1", 28, "confirmed"), ("127.0.0.2", 25, "confirmed")]))
            .Begin(80, 1, "127.0.0.1");
        Assert.Equal(80, state.Target);
        Assert.Equal("pending", state.Status);
        Assert.Equal(80, state.Member("127.0.0.1")!.Target);
        Assert.Null(state.Member("127.0.0.2")!.Target);
    }
    [Fact]
    public void NewerEventsRefreshTheAggregateAfterATargetedEdit()
    {
        var state = new DeviceVolumeState().Receive(Members([("127.0.0.1", 28, "confirmed"), ("127.0.0.2", 25, "confirmed")]))
            .Begin(80, 2, "127.0.0.2");
        // A periodic event carrying the latest sequence still updates the leader value.
        state = state.Receive(Members([("127.0.0.1", 33, "confirmed"), ("127.0.0.2", 25, "pending")], sequence: 2));
        Assert.Equal(33, state.Actual);
        Assert.Equal(33, state.Display);
        // An older event stays stale for both the card and its members.
        state = state.Receive(Members([("127.0.0.1", 20, "confirmed"), ("127.0.0.2", 20, "confirmed")], sequence: 1));
        Assert.Equal(33, state.Actual);
        Assert.Equal(33, state.Member("127.0.0.1")!.Actual);
    }
}
