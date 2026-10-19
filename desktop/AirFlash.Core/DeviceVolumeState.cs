using System.Text.Json;
namespace AirFlash.Core;

// Runtime receiver state. Never persisted or used as a PCM gain.

/// One physical device of a session, reported by the engine per peer.
public sealed record DeviceMemberVolume(string Host, int? Actual = null, int? Target = null, string Status = "unknown")
{
    public int? Display => Target ?? Actual;
    /// A device whose volume the engine has never read cannot be adjusted yet.
    public bool Available => Actual is not null;
}

public sealed record DeviceVolumeState(int? Actual = null, int? Target = null, long Sequence = 0,
    bool Available = false, string Status = "unknown", int? LastNonZero = null, bool ReadAfterWrite = false,
    IReadOnlyList<DeviceMemberVolume>? Members = null)
{
    public int? Display => Target ?? Actual;
    public DeviceMemberVolume? Member(string host) => Members?.FirstOrDefault(m => string.Equals(m.Host, host, StringComparison.Ordinal));
    /// The engine keeps the session leader first, matching the receiver's peer order.
    public string? LeaderHost => Members is { Count: > 0 } members ? members[0].Host : null;

    public DeviceVolumeState Begin(int value, long sequence, string? host = null)
    {
        var members = Members?.Select(m => host is null || m.Host == host
            ? m with { Target = value, Status = "pending" } : m).ToArray();
        // A targeted edit of the session leader also drives the receiver-wide card.
        var aggregate = host is null || host == LeaderHost;
        return this with {
            Target = aggregate ? value : Target, Sequence = sequence,
            Status = aggregate ? "pending" : Status,
            ReadAfterWrite = aggregate ? false : ReadAfterWrite, Members = members
        };
    }

    public DeviceVolumeState Receive(JsonElement item)
    {
        // Only events older than the last edit are stale; newer ones carry fresh member values.
        if (item.Integer("sequence") is { } sequence && sequence < Sequence) return this;
        var members = ParseMembers(item, Members);
        var next = this with { Members = members ?? Members };
        var number = item.Number("volume");
        int? actual = number is >= 0 and <= 100 && double.IsFinite(number.Value) && number == Math.Truncate(number.Value) ? (int)number.Value : null;
        var available = item.Boolean("available") == true && actual is not null;
        var status = item.Text("status");
        if (status is not ("pending" or "confirmed" or "unconfirmed" or "unsynced")) return next;
        // A late pending event cannot resurrect a target already timed out locally.
        if (Target is null && status == "pending") status = "unconfirmed";
        return next with {
            Actual = actual ?? Actual, Available = available, Status = status,
            Target = status == "pending" ? Target : null,
            LastNonZero = actual is > 0 ? actual : LastNonZero,
            ReadAfterWrite = ReadAfterWrite || actual is not null
        };
    }

    public DeviceVolumeState Timeout(string? host = null)
    {
        // Only devices still waiting on a write settle; confirmed ones keep their status.
        var members = Members?.Select(m => (host is null || m.Host == host) && m.Target is not null
            ? m with { Target = null, Status = "unconfirmed" } : m).ToArray();
        // A targeted edit of another device leaves the receiver-wide card untouched.
        var aggregate = host is null || host == LeaderHost;
        return this with {
            Target = aggregate ? null : Target,
            Available = aggregate ? Available && ReadAfterWrite : Available,
            Status = aggregate ? "unconfirmed" : Status,
            Members = members ?? Members
        };
    }

    private static IReadOnlyList<DeviceMemberVolume>? ParseMembers(JsonElement item, IReadOnlyList<DeviceMemberVolume>? previous)
    {
        if (!item.TryGetProperty("members", out var array) || array.ValueKind != JsonValueKind.Array) return null;
        var members = new List<DeviceMemberVolume>();
        foreach (var entry in array.EnumerateArray())
        {
            var host = entry.Text("host");
            if (host is null) continue;
            var number = entry.Number("volume");
            int? actual = number is >= 0 and <= 100 && double.IsFinite(number.Value) && number == Math.Truncate(number.Value) ? (int)number.Value : null;
            var status = entry.Text("status");
            if (status is not ("pending" or "confirmed" or "unconfirmed" or "unsynced")) status = "unknown";
            var target = previous?.FirstOrDefault(m => string.Equals(m.Host, host, StringComparison.Ordinal))?.Target;
            // A late pending event cannot resurrect a target already timed out locally.
            if (target is null && status == "pending") status = "unconfirmed";
            if (status != "pending") target = null;
            members.Add(new DeviceMemberVolume(host, actual, target, status));
        }
        return members.Count > 0 ? members : null;
    }
}
