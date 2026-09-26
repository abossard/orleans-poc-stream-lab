using Microsoft.Extensions.DependencyInjection;
using Orleans.Configuration;
using Orleans.Streams;

namespace Poc;

public sealed record ScenarioResult(
    string Id,
    string Title,
    string Expected,
    string Observed,
    bool Pass,
    int OnErrorCount,
    string[] ErrorTypes,
    int[] Published,
    int[] Delivered,
    int[] Lost,
    int Activations,
    List<Entry> Timeline,
    Dictionary<string, double>? Settings = null,
    long? CacheMaxMessages = null,
    long? CacheMaxBytes = null);

/// <param name="Shape">A: S re-registered after StreamInactivityPeriod (handshake). E: S still registered (idle cursor). C: slow consumer.</param>
/// <param name="Miss">True when S's stale token is expected to be evicted and absent from the purge metadata when it is next used.</param>
public sealed record Variant(
    string Id,
    string Title,
    string Shape,
    TimeSpan DataMaxAgeInCache,
    TimeSpan MetadataMinTimeInCache,
    TimeSpan Quiet,
    bool KeepAlive,
    bool Miss);

public sealed class Scenarios(string transport, Version orleansVersion, string logDir)
{
    public static readonly Variant[] All =
    [
        new("A", "Grain kept alive by Ping, agent forgets S, S's token and its purge metadata are evicted (prod #4006 case)",
            "A", Timings.DataMaxAgeInCache, Timings.MetadataMinTimeInCache, Timings.QuietPeriod, KeepAlive: true, Miss: true),
        new("B", "Control: as A, but the grain is idle-collected (no Ping), so the new activation has no expectedToken",
            "A", Timings.DataMaxAgeInCache, Timings.MetadataMinTimeInCache, Timings.QuietPeriod, KeepAlive: false, Miss: true),
        new("C", "Genuine mid-stream miss: slow consumer while the partition moves on",
            "C", Timings.DataMaxAgeInCache, Timings.MetadataMinTimeInCache, TimeSpan.Zero, KeepAlive: true, Miss: true),
        new("D", "Control: as A, but the cache still remembers S's last purged token (MetadataMinTimeInCache at its 10 min default)",
            "A", Timings.DataMaxAgeInCache, StreamCacheEvictionOptions.DefaultMetadataMinTimeInCache, Timings.QuietPeriod, KeepAlive: true, Miss: false),
        new("E", "Warm stream: as A, but event 2 comes while the agent still has S registered (quiet longer than MetadataMinTimeInCache, shorter than StreamInactivityPeriod)",
            "E", Timings.DataMaxAgeInCache, Timings.MetadataMinTimeInCache, Timings.WarmQuietPeriod, KeepAlive: true, Miss: true),
        new("A-mid", "As A, DataMaxAgeInCache doubled but still shorter than the quiet period",
            "A", Timings.MidDataMaxAgeInCache, Timings.MetadataMinTimeInCache, Timings.QuietPeriod, KeepAlive: true, Miss: true),
        new("E-mid", "As E, DataMaxAgeInCache doubled; quiet still between DataMaxAgeInCache + MetadataMinTimeInCache and StreamInactivityPeriod",
            "E", Timings.MidDataMaxAgeInCache, Timings.MetadataMinTimeInCache, Timings.MidWarmQuietPeriod, KeepAlive: true, Miss: true),
        new("A-big", "As A, DataMaxAgeInCache longer than the quiet period",
            "A", Timings.BigDataMaxAgeInCache, Timings.MetadataMinTimeInCache, Timings.QuietPeriod, KeepAlive: true, Miss: false),
        new("E-big", "As E, DataMaxAgeInCache longer than the quiet period",
            "E", Timings.BigDataMaxAgeInCache, Timings.MetadataMinTimeInCache, Timings.WarmQuietPeriod, KeepAlive: true, Miss: false),
        new("A-meta", "As A, DataMaxAgeInCache unchanged, MetadataMinTimeInCache longer than the quiet period",
            "A", Timings.DataMaxAgeInCache, Timings.BigMetadataMinTimeInCache, Timings.QuietPeriod, KeepAlive: true, Miss: false),
        new("E-meta", "As E, DataMaxAgeInCache unchanged, MetadataMinTimeInCache longer than the quiet period",
            "E", Timings.DataMaxAgeInCache, Timings.BigMetadataMinTimeInCache, Timings.WarmQuietPeriod, KeepAlive: true, Miss: false),
        new("C-big", "As C, DataMaxAgeInCache longer than the consumer's lag",
            "C", Timings.BigDataMaxAgeInCache, Timings.MetadataMinTimeInCache, TimeSpan.Zero, KeepAlive: true, Miss: false),
    ];

    public static Variant Find(string id) =>
        All.FirstOrDefault(v => v.Id == id) ?? throw new ArgumentException($"Unknown scenario '{id}'");

    // Orleans 10.3.0 added the catch (QueueCacheMissException) when (cacheToken is not null) fallback.
    private bool HandshakeReportsMiss => orleansVersion < new Version(10, 3, 0);

    // 10.3.x wires the Event Hub cursor's Refresh to PooledQueueCache.Refresh; the memory cursor's Refresh is a no-op in both versions.
    private bool IdleCursorRefreshes => transport == "eventhub" && !HandshakeReportsMiss;

    public async Task<ScenarioResult> Run(string id, int portOffset)
    {
        var variant = Find(id);
        var timeline = new Timeline();
        var suffix = Guid.NewGuid().ToString("N")[..6];
        var serviceId = $"poc{orleansVersion.ToString().Replace(".", "")}{transport}{id.Replace("-", "")}{suffix}";
        await using var logFile = new StreamWriter(Path.Combine(logDir, $"{orleansVersion}-{transport}-{id}.log")) { AutoFlush = true };
        // PooledQueueCache only throws QueueCacheMissException once it has also forgotten the stream's last purged token.
        using var host = SiloHost.Build(transport, timeline, serviceId, portOffset, variant.DataMaxAgeInCache, variant.MetadataMinTimeInCache, logFile);
        await host.StartAsync();
        try
        {
            using var cache = new CacheStats();
            var client = host.Services.GetRequiredService<IClusterClient>();
            await WarmUp(client, timeline, suffix);
            timeline.Restart();
            cache.Reset();

            using var fillersCts = new CancellationTokenSource();
            var fillers = RunFillers(client.GetStreamProvider(Names.Provider), suffix, timeline, fillersCts.Token);
            var key = $"{id.ToLowerInvariant()}-{suffix}";
            Step(timeline, key, $"S = {Names.ConsumerNamespace}/{key}, DataMaxAgeInCache = {variant.DataMaxAgeInCache.TotalSeconds}s, MetadataMinTimeInCache = {variant.MetadataMinTimeInCache.TotalSeconds}s");
            var result = variant.Shape switch
            {
                "A" or "E" => await QuietStream(client, timeline, cache, key, variant),
                "C" => await SlowConsumer(client, timeline, key, variant),
                _ => throw new ArgumentException($"Unknown shape '{variant.Shape}'"),
            };
            await fillersCts.CancelAsync();
            await fillers;
            return result with
            {
                Timeline = timeline.Snapshot(),
                Settings = new()
                {
                    ["DataMaxAgeInCache"] = variant.DataMaxAgeInCache.TotalSeconds,
                    ["MetadataMinTimeInCache"] = variant.MetadataMinTimeInCache.TotalSeconds,
                    ["Quiet"] = variant.Quiet.TotalSeconds,
                },
                CacheMaxMessages = cache.MaxMessages,
                CacheMaxBytes = cache.MaxBytes,
            };
        }
        finally
        {
            await host.StopAsync();
        }
    }

    // The receiver may start reading slightly after "now" (Event Hub StartFromNow), so prove the pipeline is live first.
    private static async Task WarmUp(IClusterClient client, Timeline timeline, string suffix)
    {
        var key = $"warmup-{suffix}";
        var grain = client.GetGrain<IConsumerGrain>(key);
        var deadline = DateTime.UtcNow.AddSeconds(90);
        for (var version = 1; DateTime.UtcNow < deadline; version++)
        {
            await grain.Update(version);
            if (await WaitFor(() => timeline.Has(key, "OnNextAsync"), TimeSpan.FromSeconds(3)))
            {
                return;
            }
        }

        throw new TimeoutException("Warm-up event was never delivered; is the stream provider reading?");
    }

    private async Task<ScenarioResult> QuietStream(IClusterClient client, Timeline timeline, CacheStats cache, string key, Variant variant)
    {
        var (id, keepAlive, quiet) = (variant.Id, variant.KeepAlive, variant.Quiet);
        var grain = client.GetGrain<IConsumerGrain>(key);

        Step(timeline, key, "publish event 1 on S via grain.Update(1)");
        await grain.Update(1);
        await WaitFor(() => timeline.Has(key, "OnNextAsync", 1), TimeSpan.FromSeconds(30));

        using var pingCts = new CancellationTokenSource();
        var pinger = keepAlive ? RunPinger(grain, pingCts.Token) : Task.FromResult(0);
        Step(timeline, key, $"quiet period {quiet.TotalSeconds}s: no events on S, fillers keep the partition moving, Ping={(keepAlive ? $"every {Timings.PingInterval.TotalSeconds}s" : "off")}");
        await Task.Delay(quiet);

        Step(timeline, key, $"publish event 2 on S via grain.Update(2); cache holds {cache.Messages} messages");
        await grain.Update(2);
        await WaitFor(() => timeline.Has(key, "OnNextAsync", 2), TimeSpan.FromSeconds(30));
        await Task.Delay(TimeSpan.FromSeconds(3));

        await pingCts.CancelAsync();
        var pings = await pinger;
        if (keepAlive)
        {
            Step(timeline, key, $"pinged {pings} times");
        }

        var r = Evaluate(timeline, key, published: [1, 2]);
        var handshake = variant.Shape == "A" && keepAlive;
        var expectHandshakeMiss = handshake && variant.Miss && HandshakeReportsMiss;
        var expectWarmLoss = variant.Shape == "E" && variant.Miss && !IdleCursorRefreshes;
        var expectedActivations = keepAlive ? 1 : 2;
        var pass = r.OnErrorCount == (expectHandshakeMiss || expectWarmLoss ? 1 : 0)
                   && r.ErrorTypes.All(t => t == nameof(QueueCacheMissException))
                   && r.Lost.SequenceEqual(expectWarmLoss ? [2] : Array.Empty<int>())
                   && r.Activations == expectedActivations;
        var expected = variant switch
        {
            { KeepAlive: false } => "no OnErrorAsync (GetSequenceToken returns null), OnNextAsync(2); nothing lost; 2 activations",
            { Shape: "A" } when expectHandshakeMiss => "1 x OnErrorAsync(QueueCacheMissException) at the handshake, then OnNextAsync(2); nothing lost; 1 activation",
            { Shape: "A", Miss: true } => "no OnErrorAsync (miss caught, cursor at cacheToken), OnNextAsync(2); nothing lost; 1 activation",
            { Shape: "A" } when variant.DataMaxAgeInCache > quiet => "no OnErrorAsync (expectedToken still cached), OnNextAsync(2); nothing lost; 1 activation",
            { Shape: "A" } => "no OnErrorAsync (cache resumes at its oldest message), OnNextAsync(2); nothing lost; 1 activation",
            _ when expectWarmLoss => "1 x OnErrorAsync(QueueCacheMissException) from the idle cursor, event 2 skipped; 1 activation",
            { Miss: true } => "no OnErrorAsync (idle cursor refreshed to event 2), OnNextAsync(2); nothing lost; 1 activation",
            _ when variant.DataMaxAgeInCache > quiet => "no OnErrorAsync (idle cursor position still cached), OnNextAsync(2); nothing lost; 1 activation",
            _ => "no OnErrorAsync (cache resumes at its oldest message), OnNextAsync(2); nothing lost; 1 activation",
        };
        return r with { Id = id, Title = variant.Title, Expected = expected, Pass = pass };
    }

    private async Task<ScenarioResult> SlowConsumer(IClusterClient client, Timeline timeline, string key, Variant variant)
    {
        const int burst = 20;
        const int slowMs = 6000;
        var stream = client.GetStreamProvider(Names.Provider).GetStream<Payload>(StreamId.Create(Names.ConsumerNamespace, key));

        Step(timeline, key, $"publish events 1..{burst} on S; event 1 takes {slowMs / 1000}s to process");
        for (var v = 1; v <= burst; v++)
        {
            await stream.OnNextAsync(new Payload(v, v == 1 ? slowMs : 0));
        }

        await Task.Delay(TimeSpan.FromSeconds(10));
        Step(timeline, key, $"publish event {burst + 1} on S (after the slow period)");
        await stream.OnNextAsync(new Payload(burst + 1));
        await WaitFor(() => timeline.Has(key, "OnNextAsync", burst + 1), TimeSpan.FromSeconds(10));
        await Task.Delay(TimeSpan.FromSeconds(3));

        var r = Evaluate(timeline, key, published: [.. Enumerable.Range(1, burst + 1)]);
        var pass = variant.Miss
            ? r.ErrorTypes.Contains(nameof(QueueCacheMissException)) && r.Lost.Length > 0
            : r.OnErrorCount == 0 && r.Lost.Length == 0;
        return r with
        {
            Id = variant.Id,
            Title = variant.Title,
            Expected = variant.Miss
                ? "OnErrorAsync(QueueCacheMissException) on both versions and events skipped"
                : "no OnErrorAsync, all events delivered (lag shorter than DataMaxAgeInCache)",
            Pass = pass,
        };
    }

    private static ScenarioResult Evaluate(Timeline timeline, string key, int[] published)
    {
        var mine = timeline.Snapshot().Where(e => e.Grain == key).ToList();
        var errorTypes = mine.Where(e => e.Kind == "OnErrorAsync")
            .Select(e => e.Detail.Split(':')[0].Split('.')[^1])
            .ToArray();
        var delivered = mine.Where(e => e.Kind == "OnNextAsync").Select(e => e.Version!.Value).ToArray();
        var lost = published.Except(delivered).ToArray();
        var activations = mine.Count(e => e.Kind == "Activated");
        var observed = $"{errorTypes.Length} x OnErrorAsync"
                       + (errorTypes.Length > 0 ? $"({string.Join(", ", errorTypes)})" : "")
                       + $"; delivered [{string.Join(",", delivered)}]; lost [{string.Join(",", lost)}]; {activations} activation(s)";
        return new ScenarioResult("", "", "", observed, false, errorTypes.Length, errorTypes, published, delivered, lost, activations, []);
    }

    private static void Step(Timeline timeline, string key, string text) => timeline.Add("driver", key, "Step", text);

    private static async Task RunFillers(IStreamProvider streams, string suffix, Timeline timeline, CancellationToken ct)
    {
        var stream = streams.GetStream<Payload>(StreamId.Create(Names.FillerNamespace, suffix));
        var count = 0;
        timeline.Add("driver", "", "Fillers", $"start: 1 event every {Timings.FillerInterval.TotalMilliseconds} ms on {Names.FillerNamespace}/{suffix} (same partition, no consumer)");
        try
        {
            while (!ct.IsCancellationRequested)
            {
                await stream.OnNextAsync(new Payload(++count));
                await Task.Delay(Timings.FillerInterval, ct);
            }
        }
        catch (OperationCanceledException)
        {
        }

        timeline.Add("driver", "", "Fillers", $"stopped after {count} events");
    }

    private static async Task<int> RunPinger(IConsumerGrain grain, CancellationToken ct)
    {
        var count = 0;
        try
        {
            while (!ct.IsCancellationRequested)
            {
                await Task.Delay(Timings.PingInterval, ct);
                await grain.Ping();
                count++;
            }
        }
        catch (OperationCanceledException)
        {
        }

        return count;
    }

    private static async Task<bool> WaitFor(Func<bool> condition, TimeSpan timeout)
    {
        var deadline = DateTime.UtcNow + timeout;
        while (DateTime.UtcNow < deadline)
        {
            if (condition())
            {
                return true;
            }

            await Task.Delay(100);
        }

        return condition();
    }
}
