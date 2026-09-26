using System.Collections.Concurrent;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Text.RegularExpressions;
using System.Threading.Channels;
using Live;
using Orleans;
using Orleans.Configuration;
using Orleans.Streaming.Diagnostics;
using Poc;

var builder = WebApplication.CreateBuilder(args);
builder.Services.AddCors();
builder.Services.AddSingleton<Feed>();
builder.Services.AddSingleton<Lane>();
builder.Services.AddHostedService(sp => sp.GetRequiredService<Lane>());
var app = builder.Build();
// The page shows both Orleans versions, so it talks to both backends.
app.UseCors(p => p.AllowAnyOrigin().AllowAnyHeader().AllowAnyMethod());
app.UseDefaultFiles();
app.UseStaticFiles();

app.MapGet("/api/events", (Feed feed, CancellationToken ct) => TypedResults.ServerSentEvents(feed.Subscribe(ct)));
app.MapGet("/api/state", (Lane lane) => lane.State());
app.MapPost("/api/publish", async (Lane lane, PublishRequest r) =>
    await lane.Publish(r.Key) is { } version ? Results.Ok(new { r.Key, version }) : Results.Conflict("live silo not ready"));
app.MapPost("/api/scenarios/{id}/run", (Lane lane, string id) =>
    Scenarios.All.All(v => v.Id != id) ? Results.NotFound() : lane.TryRestart(id, null) ? Results.Accepted() : Results.Conflict("busy"));
app.MapPost("/api/reset", (Lane lane, string? transport) =>
    transport is not (null or "eventhub" or "memory") ? Results.BadRequest("transport: eventhub or memory")
    : lane.TryRestart(null, transport) ? Results.Accepted() : Results.Conflict("busy"));
app.Run();

public sealed record PublishRequest(string Key);

/// <param name="At">Unix time in ms when the backend saw it.</param>
public sealed record FeedEvent(long At, string Source, string Stream, string Kind, string Detail, int? Version);

/// <summary>Everything since the last reset or scenario start, replayed to each new SSE subscriber, then live.</summary>
public sealed class Feed
{
    private readonly Lock gate = new();
    private readonly List<FeedEvent> history = [];
    private readonly List<Channel<FeedEvent>> subscribers = [];

    public void Add(string source, string stream, string kind, string detail = "", int? version = null)
    {
        var e = new FeedEvent(DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(), source, stream, kind, detail, version);
        lock (gate)
        {
            history.Add(e);
            subscribers.ForEach(s => s.Writer.TryWrite(e));
        }
    }

    public void Clear()
    {
        lock (gate)
        {
            history.Clear();
        }

        Add("backend", "", "Reset");
    }

    public async IAsyncEnumerable<FeedEvent> Subscribe([EnumeratorCancellation] CancellationToken ct)
    {
        var channel = Channel.CreateUnbounded<FeedEvent>();
        lock (gate)
        {
            history.ForEach(e => channel.Writer.TryWrite(e));
            subscribers.Add(channel);
        }

        try
        {
            await foreach (var e in channel.Reader.ReadAllAsync(ct))
            {
                yield return e;
            }
        }
        finally
        {
            lock (gate)
            {
                subscribers.Remove(channel);
            }
        }
    }
}

/// <summary>
/// One Orleans silo at a time: the live silo (default timings, fillers, keep-alive for published keys)
/// or a scenario's own silo from <see cref="Scenarios.Run"/>. All of them write into one timeline, which feeds the SSE feed.
/// </summary>
public sealed class Lane : IHostedService
{
    public static readonly Version Orleans = Version.Parse(Regex.Match(
        typeof(StreamPullingAgentOptions).Assembly.GetCustomAttribute<AssemblyInformationalVersionAttribute>()!.InformationalVersion, @"^\d+(\.\d+)+").Value);

    private static readonly string LogDir = Directory.CreateDirectory(Path.Combine(Path.GetTempPath(), "poc-logs")).FullName;

    private readonly Feed feed;
    private readonly Timeline timeline = new();
    private readonly SemaphoreSlim gate = new(1, 1);
    private readonly ConcurrentDictionary<string, int> published = new();
    private string transport = "eventhub";
    private string status = "starting";
    private Variant settings = Scenarios.Find("A");
    private IHost? live;
    private CancellationTokenSource? liveLoops;
    private Task liveTasks = Task.CompletedTask;
    private IServiceProvider? silo;
    private Snapshot? snapshot;

    public Lane(Feed feed)
    {
        this.feed = feed;
        timeline.Added += e => feed.Add(e.Source, e.Grain != "" ? e.Grain : KeyIn(e.Detail), e.Kind, e.Detail, e.Version);
        StreamingEvents.AllEvents.Subscribe(new Observer<StreamingEvents.StreamingEvent>(OnStreamingEvent));
    }

    public Task StartAsync(CancellationToken cancellationToken)
    {
        // The Event Hubs emulator can take a while to accept connections after compose starts it.
        TryRestart(null, null, attempts: 30);
        _ = Inspect();
        return Task.CompletedTask;
    }

    public Task StopAsync(CancellationToken cancellationToken) => StopLive();

    public object State() => new
    {
        orleans = Orleans.ToString(),
        transport,
        status,
        settings = new
        {
            scenario = settings.Id,
            DataMaxAgeInCache = settings.DataMaxAgeInCache.TotalSeconds,
            MetadataMinTimeInCache = settings.MetadataMinTimeInCache.TotalSeconds,
            StreamInactivityPeriod = Timings.StreamInactivityPeriod.TotalSeconds,
        },
        scenarios = Scenarios.All.Select(v => new { v.Id, v.Title }),
        snapshot,
    };

    public async Task<int?> Publish(string key)
    {
        if (live is null || status != "live")
        {
            return null;
        }

        var v = published.AddOrUpdate(key, 1, (_, last) => last + 1);
        await live.Services.GetRequiredService<IClusterClient>().GetGrain<IConsumerGrain>(key).Update(v);
        return v;
    }

    /// <summary>Replaces the current silo: optionally runs a scenario on its own silo, then starts a fresh live silo.</summary>
    public bool TryRestart(string? scenario, string? newTransport, int attempts = 1)
    {
        if (!gate.Wait(0))
        {
            return false;
        }

        _ = Task.Run(async () =>
        {
            try
            {
                for (var attempt = 1; ; attempt++)
                {
                    try
                    {
                        await Restart(scenario, newTransport);
                        return;
                    }
                    catch (Exception ex) when (attempt < attempts)
                    {
                        feed.Add("backend", "", "Error", $"{ex.Message} (attempt {attempt} of {attempts})");
                        await Task.Delay(TimeSpan.FromSeconds(5));
                    }
                }
            }
            catch (Exception ex)
            {
                status = "error";
                feed.Add("backend", "", "Error", ex.Message);
            }
            finally
            {
                gate.Release();
            }
        });
        return true;
    }

    private async Task Restart(string? scenario, string? newTransport)
    {
        await StopLive();
        transport = newTransport ?? transport;
        published.Clear();
        feed.Clear();
        if (scenario is not null)
        {
            status = $"running {scenario}";
            settings = Scenarios.Find(scenario);
            var r = await new Scenarios(transport, Orleans, LogDir).Run(scenario, 1, timeline, sp => silo = sp);
            silo = null;
            feed.Add("driver", "", "Result", $"{(r.Pass ? "PASS" : "FAIL")}. Expected: {r.Expected}. Observed: {r.Observed}");
        }

        status = "starting live silo";
        settings = Scenarios.Find("A");
        var suffix = Guid.NewGuid().ToString("N")[..6];
        var host = SiloHost.Build(transport, timeline, $"live{suffix}", 0, settings.DataMaxAgeInCache, settings.MetadataMinTimeInCache, TextWriter.Null);
        try
        {
            await host.StartAsync();
            silo = host.Services;
            var client = host.Services.GetRequiredService<IClusterClient>();
            await Scenarios.WarmUp(client, timeline, suffix);
            liveLoops = new CancellationTokenSource();
            liveTasks = Task.WhenAll(
                    Scenarios.RunFillers(client.GetStreamProvider(Names.Provider), suffix, timeline, liveLoops.Token),
                    KeepAlive(client, liveLoops.Token))
                .ContinueWith(t =>
                {
                    if (t.IsFaulted)
                    {
                        feed.Add("backend", "", "Error", $"live silo fillers or keep-alive stopped: {t.Exception!.GetBaseException().Message}");
                    }
                });
            live = host;
            status = "live";
        }
        catch
        {
            silo = null;
            await host.StopAsync(CancellationToken.None).ContinueWith(_ => host.Dispose());
            throw;
        }
    }

    private async Task StopLive()
    {
        if (live is null)
        {
            return;
        }

        var host = live;
        (live, silo, status) = (null, null, "stopping live silo");
        await liveLoops!.CancelAsync();
        await liveTasks;
        await host.StopAsync();
        host.Dispose();
    }

    // A periodic read (Ping) keeps published grains active, so they keep their expectedToken.
    private async Task KeepAlive(IClusterClient client, CancellationToken ct)
    {
        try
        {
            while (true)
            {
                await Task.Delay(Timings.PingInterval, ct);
                await Task.WhenAll(published.Keys.Select(k => client.GetGrain<IConsumerGrain>(k).Ping()));
            }
        }
        catch (OperationCanceledException)
        {
        }
    }

    private async Task Inspect()
    {
        Snapshot? previous = null;
        IServiceProvider? previousSilo = null;
        while (true)
        {
            await Task.Delay(250);
            var current = silo;
            if (current != previousSilo)
            {
                (previous, previousSilo, snapshot) = (null, current, null);
            }

            try
            {
                snapshot = current is null ? null : Inspector.Read(current);
            }
            catch
            {
                continue;
            }

            if (previous is not null && snapshot is not null)
            {
                Diff(previous, snapshot);
            }

            previous = snapshot ?? previous;
        }
    }

    private void Diff(Snapshot before, Snapshot after)
    {
        var (purgedBefore, purgedAfter) = (before.Cache.LastPurgedToken, after.Cache.LastPurgedToken);
        foreach (var (key, p) in purgedAfter.Where(kv => !purgedBefore.TryGetValue(kv.Key, out var old) || old.Seq != kv.Value.Seq))
        {
            feed.Add("cache", key, "Purged", $"PooledQueueCache purged token {p.Seq}: lastPurgedToken = {p.Seq}");
        }

        foreach (var key in purgedBefore.Keys.Except(purgedAfter.Keys))
        {
            feed.Add("cache", key, "PurgeMetadataExpired", $"lastPurgedToken removed after MetadataMinTimeInCache ({settings.MetadataMinTimeInCache.TotalSeconds}s)");
        }

        var cursorsBefore = before.Streams.ToDictionary(s => s.Key, s => s.Consumers.FirstOrDefault());
        foreach (var s in after.Streams)
        {
            var c = s.Consumers.FirstOrDefault();
            if (c is not null && (!cursorsBefore.TryGetValue(s.Key, out var old) || old is null || old.Cursor != c.Cursor || old.CursorSeq != c.CursorSeq))
            {
                feed.Add("agent", s.Key, "Cursor", $"cache cursor {c.Cursor} at {c.CursorSeq}");
            }
        }
    }

    private void OnStreamingEvent(StreamingEvents.StreamingEvent e)
    {
        var (id, kind, detail) = e switch
        {
            StreamingEvents.SubscriptionAdded a => (a.StreamId, "SubscriptionAdded", "RegisterStream: the pulling agent registered S and added its implicit subscriber"),
            StreamingEvents.SubscriptionAttached a => (a.StreamId, "SubscriptionAttached", "handshake done, cache cursor set"),
            StreamingEvents.StreamInactive i => (i.StreamId, "StreamInactive", $"CleanupPubSubCache removed S after StreamInactivityPeriod ({i.InactivityPeriod.TotalSeconds}s)"),
            _ => (default, "", ""),
        };
        if (kind != "" && id.GetNamespace() == Names.ConsumerNamespace)
        {
            feed.Add("agent", id.GetKeyAsString(), kind, detail);
        }
    }

    private static string KeyIn(string text) => Regex.Match(text, Names.ConsumerNamespace + @"/([\w-]+)").Groups[1].Value;

    private sealed class Observer<T>(Action<T> next) : IObserver<T>
    {
        public void OnNext(T value) => next(value);

        public void OnError(Exception error) { }

        public void OnCompleted() { }
    }
}
