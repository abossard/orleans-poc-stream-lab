using System.Collections;
using System.Reflection;
using Orleans.Providers.Streams.Common;
using Orleans.Runtime;
using Orleans.Streams;
using Poc;

namespace Live;

// Sequence numbers are strings: memory-stream tokens are tick-based and exceed JavaScript's safe integer range.
public sealed record Purged(string Seq, double AgeS);

public sealed record Consumer(string State, string? Cursor, string? CursorSeq, string? LastTokenSeq);

public sealed record StreamState(string Key, bool Registered, double IdleS, Consumer[] Consumers);

public sealed record CacheState(int Items, string? Oldest, string? Newest, Dictionary<string, Purged> LastPurgedToken);

public sealed record Snapshot(CacheState? Cache, StreamState[] Streams);

/// <summary>
/// Reads PersistentStreamPullingAgent and PooledQueueCache internals by reflection (field names are the same in 10.2.1 and 10.3.1).
/// The agent mutates them on its own scheduler, so a read can fail; the caller skips that tick.
/// </summary>
public static class Inspector
{
    private const BindingFlags Any = BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic;
    private static readonly Type ActivationDirectory = Type.GetType("Orleans.Runtime.ActivationDirectory, Orleans.Runtime", throwOnError: true)!;

    public static Snapshot? Read(IServiceProvider silo)
    {
        var agent = ((IEnumerable<KeyValuePair<GrainId, IGrainContext>>)silo.GetRequiredService(ActivationDirectory))
            .Select(kv => kv.Value)
            .FirstOrDefault(v => v.GetType().Name == "PersistentStreamPullingAgent");
        if (agent is null)
        {
            return null;
        }

        var now = DateTime.UtcNow;
        var cache = Unwrap(Get(agent, "queueCache"));
        var cacheState = cache is null
            ? null
            : new CacheState(
                cache.ItemCount,
                cache.Oldest?.SequenceNumber.ToString(),
                cache.Newest?.SequenceNumber.ToString(),
                ((Dictionary<StreamId, (DateTime TimeStamp, StreamSequenceToken Token)>)Get(cache, "lastPurgedToken")!).ToArray()
                .Where(kv => kv.Key.GetNamespace() == Names.ConsumerNamespace)
                .ToDictionary(kv => kv.Key.GetKeyAsString(), kv => new Purged(kv.Value.Token.SequenceNumber.ToString(), (now - kv.Value.TimeStamp).TotalSeconds)));

        var streams = ((IEnumerable)Get(agent, "pubSubCache")!).Cast<object>().ToArray()
            .Select(kv => (Id: (StreamId)Get(Get(kv, "Key"), "StreamId")!, Collection: Get(kv, "Value")!))
            .Where(s => s.Id.GetNamespace() == Names.ConsumerNamespace)
            .Select(s => new StreamState(
                s.Id.GetKeyAsString(),
                (bool)Get(s.Collection, "StreamRegistered")!,
                (now - (DateTime)Get(s.Collection, "lastActivityTime")!).TotalSeconds,
                ((IDictionary)Get(s.Collection, "queueData")!).Values.Cast<object>().Select(ReadConsumer).ToArray()))
            .ToArray();
        return new Snapshot(cacheState, streams);
    }

    private static Consumer ReadConsumer(object data)
    {
        // StreamConsumerData.Cursor is the adapter's IQueueCacheCursor; its "cursor" field is PooledQueueCache.Cursor.
        var cursor = Get(Get(data, "Cursor"), "cursor");
        var lastToken = Get(Get(data, "LastToken"), "Token") as StreamSequenceToken;
        return new Consumer(
            Get(data, "State")!.ToString()!,
            Get(cursor, "State")?.ToString(),
            (Get(cursor, "SequenceToken") as StreamSequenceToken)?.SequenceNumber.ToString(),
            lastToken?.SequenceNumber.ToString());
    }

    // Event Hub: EventHubAdapterReceiver.cache -> EventHubQueueCache.cache. Memory: MemoryPooledCache.cache.
    private static PooledQueueCache? Unwrap(object? o)
    {
        for (var i = 0; i < 3 && o is not null and not PooledQueueCache; i++)
        {
            o = Get(o, "cache");
        }

        return o as PooledQueueCache;
    }

    private static object? Get(object? o, string name) =>
        o is null ? null : o.GetType().GetField(name, Any)?.GetValue(o) ?? o.GetType().GetProperty(name, Any)?.GetValue(o);
}
