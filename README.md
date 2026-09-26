# Orleans #4006 PoC: stale `StreamSequenceToken` after a stream goes quiet

Minimal Orleans app that reproduces the `QueueCacheMissException` errors behind [Azure/ahm-planning#4006](https://github.com/Azure/ahm-planning/issues/4006) and shows how Orleans 10.3.1 changes them. The same code runs against Orleans **10.2.1** and **10.3.1**, on **memory streams** and on the **Azure Event Hubs emulator**.

Terms: a `StreamSequenceToken` is a stream position. For Event Hub it is an `EventHubSequenceToken` (offset, sequence number, event index), and sequence numbers count events per **partition**. After a delivery the subscription handle keeps the token as `expectedToken` (a `StreamHandshakeToken`) and returns it from `GetSequenceToken()` during a handshake. `cacheToken` is the token of the event that made the pulling agent register the stream again. S is the consumer grain's own stream.

## What it proves

- **Handshake path (the #4006 noise), scenario A.** The consumer grain stays active, the pulling agent forgets S, S's last token is evicted, then a new event arrives. On 10.2.1 the grain gets `OnErrorAsync(QueueCacheMissException)` and then `OnNextAsync(event 2)`. On 10.3.1 it only gets `OnNextAsync(event 2)`. Nothing is lost on either version.
- **Keep-alive matters, scenario B.** If the grain is idle-collected, the new activation returns `null` from `GetSequenceToken()` and no version reports an error.
- **Purge metadata matters, scenario D.** `PooledQueueCache` remembers the last purged token per stream for `MetadataMinTimeInCache` (default 10 min). While it does, a stale token resumes at the oldest cached message without throwing. Prod's 30 min `StreamInactivityPeriod` is longer than that, so the metadata was always gone by the time the handshake ran.
- **Idle-cursor path (`RunConsumerCursor`), scenario E. This is a real loss on 10.2.1.** S is still registered, but it was quiet for longer than `MetadataMinTimeInCache` and shorter than `StreamInactivityPeriod`. Its idle cursor still points at an evicted position. On 10.2.1 the next event throws `QueueCacheMissException` in `RunConsumerCursor`. The cursor then jumps to the newest message, so **event 2 is skipped**. On 10.3.1 with Event Hub, `EventHubAdapterReceiver.Cursor.Refresh` calls `PooledQueueCache.Refresh`, which moves the idle cursor to the new event: no error and no loss. Memory streams still lose the event on 10.3.1 because their cursor `Refresh` does nothing.
- **Genuine mid-stream miss, scenario C.** A slow consumer falls behind the cache. Both versions call `OnErrorAsync(QueueCacheMissException)` and skip the evicted events.

## How to run

Requires the .NET 10 SDK and Docker (only for the Event Hub transport).

```bash
open -a Docker                          # if the daemon is not running
./run-all.sh                            # 5 scenarios x {10.2.1, 10.3.1} x {memory, eventhub}, about 12 min
TRANSPORTS=memory ./run-all.sh          # no Docker needed
VERSIONS=10.2.1 TRANSPORTS=eventhub SCENARIOS=A,E ./run-all.sh
docker compose down                     # stop the emulator and Azurite
```

Single run by hand:

```bash
dotnet build src/Poc -c Release -p:OrleansVersion=10.2.1
dotnet artifacts/10.2.1/bin/Release/net10.0/Poc.dll --transport eventhub --scenarios A,E --out results
```

Output: `results/<version>-<transport>.json` and `.md` (full callback timeline per scenario), `results/logs/<version>-<transport>-<scenario>.log` (silo log, pulling agent at Debug), `results/summary.md`. Each run prints the loaded `Orleans.Streaming` version and path, so you can check which build was used.

## Setup

| Piece | PoC | Prod equivalent |
|---|---|---|
| Consumer | `ConsumerGrain`: `[ImplicitStreamSubscription]`, `IStreamSubscriptionObserver`, `OnSubscribed` calls `handleFactory.Create<T>().ResumeAsync(this)`, `OnErrorAsync` only records | `EntityConfigGrain.cs` L31, L63-78 |
| Producer | `grain.Update(v)` publishes on the grain's own stream | `EntityConfigGrain.HandleEntityUpdate` L150 |
| Keep-alive | driver calls `grain.Ping()` every 3 s | discovery `GetConfig()` every 5 min |
| Other traffic on the partition | 1 filler event every 500 ms on `filler/<id>` (no consumer), 1 partition | other entities hashed to the same partition |
| Stream provider | `AddMemoryStreams` or `AddEventHubStreams` (emulator + Azure Table checkpointer on Azurite), `StreamPubSubType.ImplicitOnly` | `Silo/Program.cs` L190-245 |
| `DataMinTimeInCache` / `DataMaxAgeInCache` | 1 s / 3 s | 10 s / 30 s |
| `MetadataMinTimeInCache` | 5 s (scenario D: 10 min default) | 10 min default |
| `StreamInactivityPeriod` (cleanup every 1/10) | 20 s | 30 min default |
| `CollectionAge` / `CollectionQuantum` | 10 s / 2 s | 15 min / 1 min defaults |
| Quiet period before event 2 | A, B, D: 26 s (longer than `StreamInactivityPeriod` + cleanup). E: 12 s (longer than eviction + metadata, shorter than `StreamInactivityPeriod`) | 30+ min (A), 10-30 min (E) |

A silo-side `IIncomingGrainCallFilter` (`HandshakeProbe`) records what `GetSequenceToken()` returns during the handshake, and pulling-agent log lines about S (`Got back 1 subscribers for stream ...`) go into the timeline as evidence of (re)registration.

## Results

All 20 runs matched the expectations coded in `src/Poc/Scenarios.cs` (full table with expectations: [results/summary.md](results/summary.md)).

| Scenario | Transport | 10.2.1 OnErrorAsync | 10.2.1 delivered | 10.2.1 lost | 10.3.1 OnErrorAsync | 10.3.1 delivered | 10.3.1 lost |
|---|---|---|---|---:|---|---|---:|
| A handshake (prod case) | eventhub | 1 x QueueCacheMissException | 2/2 | 0 | 0 | 2/2 | 0 |
| A handshake (prod case) | memory | 1 x QueueCacheMissException | 2/2 | 0 | 0 | 2/2 | 0 |
| B no keep-alive | eventhub | 0 | 2/2 | 0 | 0 | 2/2 | 0 |
| B no keep-alive | memory | 0 | 2/2 | 0 | 0 | 2/2 | 0 |
| C slow consumer | eventhub | 2 x QueueCacheMissException | 1/21 | 20 | 1 x QueueCacheMissException | 2/21 | 19 |
| C slow consumer | memory | 2 x QueueCacheMissException | 1/21 | 20 | 2 x QueueCacheMissException | 1/21 | 20 |
| D purge metadata kept | eventhub | 0 | 2/2 | 0 | 0 | 2/2 | 0 |
| D purge metadata kept | memory | 0 | 2/2 | 0 | 0 | 2/2 | 0 |
| E idle cursor (`RunConsumerCursor`) | eventhub | 1 x QueueCacheMissException | 1/2 | 1 | 0 | 2/2 | 0 |
| E idle cursor (`RunConsumerCursor`) | memory | 1 x QueueCacheMissException | 1/2 | 1 | 1 x QueueCacheMissException | 1/2 | 1 |

"OnErrorAsync" is the count on S's grain, with exception type. "Delivered" and "Lost" are the published versions seen or missing in `OnNextAsync`.

## Key log excerpts

From `results/10.2.1-eventhub.md` and `results/10.3.1-eventhub.md` (t in seconds from scenario start; event 1 was delivered at t = 0.03).

A on 10.2.1: re-registration, stale `expectedToken`, error, then delivery.

```text
26.157 agent         Got back 1 subscribers for stream poc/entity-update/a-a0660f.
26.160 agent->grain  GetSequenceToken returned DeliveryToken(EventHubSequenceToken(EventHubOffset: 31984, SequenceNumber: 187, EventIndex: 0))
26.165 grain         OnErrorAsync Orleans.Streams.QueueCacheMissException: Item not found in cache.  Requested: EventHubSequenceToken(EventHubOffset: 31984, SequenceNumber: 187, EventIndex: 0), Low: EventHubSequenceToken(EventHubOffset: , SequenceNumber: 232, EventIndex: 0), High: EventHubSequenceToken(EventHubOffset: , SequenceNumber: 238, EventIndex: 0)
26.166 grain         OnNextAsync 2 token=EventHubSequenceToken(EventHubOffset: 40968, SequenceNumber: 238, EventIndex: 0)
```

A on 10.3.1: same handshake, no error.

```text
26.143 agent         Got back 1 subscribers for stream poc/entity-update/a-5c73d8.
26.146 agent->grain  GetSequenceToken returned DeliveryToken(EventHubSequenceToken(EventHubOffset: 92112, SequenceNumber: 527, EventIndex: 0))
26.148 grain         OnNextAsync 2 token=EventHubSequenceToken(EventHubOffset: 101096, SequenceNumber: 578, EventIndex: 0)
```

E on 10.2.1: no re-registration and no `GetSequenceToken`. `Requested` is event 1's own position (SequenceNumber 436) with an empty offset. Event 2 never reaches `OnNextAsync`.

```text
 0.039 grain         OnNextAsync 1 token=EventHubSequenceToken(EventHubOffset: 76072, SequenceNumber: 436, EventIndex: 0)
12.132 grain         Published 2
12.154 grain         OnErrorAsync Orleans.Streams.QueueCacheMissException: Item not found in cache.  Requested: EventHubSequenceToken(EventHubOffset: , SequenceNumber: 436, EventIndex: 0), Low: EventHubSequenceToken(EventHubOffset: , SequenceNumber: 454, EventIndex: 0), High: EventHubSequenceToken(EventHubOffset: , SequenceNumber: 460, EventIndex: 0)
```

E on 10.3.1: the idle cursor is refreshed to event 2.

```text
 0.024 grain         OnNextAsync 1 token=EventHubSequenceToken(EventHubOffset: 133040, SequenceNumber: 758, EventIndex: 0)
12.132 grain         Published 2
12.154 grain         OnNextAsync 2 token=EventHubSequenceToken(EventHubOffset: 137272, SequenceNumber: 782, EventIndex: 0)
```

## Hypothesis check

| Step | Verdict | Evidence |
|---|---|---|
| 1. After a delivery the handle keeps `expectedToken` (rewindable stream) | Confirmed | A: `GetSequenceToken` returns `DeliveryToken(<event 1 token>)` 26 s later. [StreamSubscriptionHandleImpl.cs v10.3.1 L191](https://github.com/dotnet/orleans/blob/v10.3.1/src/Orleans.Streaming/Internal/StreamSubscriptionHandleImpl.cs#L191), [L92-95](https://github.com/dotnet/orleans/blob/v10.3.1/src/Orleans.Streaming/Internal/StreamSubscriptionHandleImpl.cs#L92-L95) |
| 2. The grain stays active, so `expectedToken` survives | Confirmed | A: 1 activation, pinged. B: without Ping the grain is collected (`Deactivated ActivationIdle`), the new activation returns `null` and no error is reported on any version |
| 3a. The agent forgets S after `StreamInactivityPeriod` | Confirmed | A: `Got back 1 subscribers for stream .../a-...` logged again at event 2. [v10.2.1 L491](https://github.com/dotnet/orleans/blob/v10.2.1/src/Orleans.Streaming/PersistentStreams/PersistentStreamPullingAgent.cs#L491), [L575-585](https://github.com/dotnet/orleans/blob/v10.2.1/src/Orleans.Streaming/PersistentStreams/PersistentStreamPullingAgent.cs#L575-L585); [v10.3.1 L529](https://github.com/dotnet/orleans/blob/v10.3.1/src/Orleans.Streaming/PersistentStreams/PersistentStreamPullingAgent.cs#L529), [L620-645](https://github.com/dotnet/orleans/blob/v10.3.1/src/Orleans.Streaming/PersistentStreams/PersistentStreamPullingAgent.cs#L620-L645) |
| 3b. S's token is evicted relative to the newest event on the partition | Confirmed | `Requested` is older than `Low` in every miss. [ChronologicalEvictionStrategy.cs v10.2.1 L91-98](https://github.com/dotnet/orleans/blob/v10.2.1/src/Orleans.Streaming/Common/PooledCache/ChronologicalEvictionStrategy.cs#L91-L98), [TimePurgePredicate.cs v10.3.1 L32-36](https://github.com/dotnet/orleans/blob/v10.3.1/src/Orleans.Streaming/Common/PooledCache/TimePurgePredicate.cs#L32-L36) |
| 3c. (not in the original hypothesis) S's purge metadata must be gone too | Added | D: with `MetadataMinTimeInCache` = 10 min, 10.2.1 does not throw. `SetCursor` resumes at the oldest message when the requested token is not older than S's last purged token: [PooledQueueCache.cs v10.2.1 L198-220](https://github.com/dotnet/orleans/blob/v10.2.1/src/Orleans.Streaming/Common/PooledCache/PooledQueueCache.cs#L198-L220), [v10.3.1 L233-254](https://github.com/dotnet/orleans/blob/v10.3.1/src/Orleans.Streaming/Common/PooledCache/PooledQueueCache.cs#L233-L254). Metadata expiry: [v10.2.1 L140-166](https://github.com/dotnet/orleans/blob/v10.2.1/src/Orleans.Streaming/Common/PooledCache/PooledQueueCache.cs#L140-L166), [v10.3.1 L174-200](https://github.com/dotnet/orleans/blob/v10.3.1/src/Orleans.Streaming/Common/PooledCache/PooledQueueCache.cs#L174-L200). Default `MetadataMinTimeInCache` = 2 x 5 min: [RecoverableStreamOptions.cs L36-41](https://github.com/dotnet/orleans/blob/v10.3.1/src/Orleans.Streaming/Common/RecoverableStreamOptions.cs#L36-L41) |
| 4a. Re-registration handshake: `GetCacheCursor(expectedToken)` throws | Confirmed | A, both versions reach it. [v10.2.1 L642](https://github.com/dotnet/orleans/blob/v10.2.1/src/Orleans.Streaming/PersistentStreams/PersistentStreamPullingAgent.cs#L642) `RegisterStream`, [L345](https://github.com/dotnet/orleans/blob/v10.2.1/src/Orleans.Streaming/PersistentStreams/PersistentStreamPullingAgent.cs#L345); [v10.3.1 L701](https://github.com/dotnet/orleans/blob/v10.3.1/src/Orleans.Streaming/PersistentStreams/PersistentStreamPullingAgent.cs#L701), [L365](https://github.com/dotnet/orleans/blob/v10.3.1/src/Orleans.Streaming/PersistentStreams/PersistentStreamPullingAgent.cs#L365) |
| 4b. 10.2.1: `OnErrorAsync`, then resume at `cacheToken` | Confirmed | A 10.2.1, both transports. [v10.2.1 L363](https://github.com/dotnet/orleans/blob/v10.2.1/src/Orleans.Streaming/PersistentStreams/PersistentStreamPullingAgent.cs#L363) `ErrorProtocol`, [L368-379](https://github.com/dotnet/orleans/blob/v10.2.1/src/Orleans.Streaming/PersistentStreams/PersistentStreamPullingAgent.cs#L368-L379) fallback |
| 4c. 10.3.1: silent resume at `cacheToken` | Confirmed | A 10.3.1, both transports. [v10.3.1 L367-372](https://github.com/dotnet/orleans/blob/v10.3.1/src/Orleans.Streaming/PersistentStreams/PersistentStreamPullingAgent.cs#L367-L372) |

### Contradiction: "no loss" does not hold for every 10.2.1 miss

Scenario E is a second code path that throws the same exception and **does lose the event** on 10.2.1:

1. After delivering an event, the consumer's cursor goes idle at the newest partition position it scanned ([PooledQueueCache.cs v10.2.1 L310-311](https://github.com/dotnet/orleans/blob/v10.2.1/src/Orleans.Streaming/Common/PooledCache/PooledQueueCache.cs#L310-L311), [v10.3.1 L344-345](https://github.com/dotnet/orleans/blob/v10.3.1/src/Orleans.Streaming/Common/PooledCache/PooledQueueCache.cs#L344-L345)). The stream stays registered because it is quiet for less than `StreamInactivityPeriod`.
2. When the next event arrives, `StartInactiveCursors` calls `consumerData.Cursor?.Refresh(startToken)` ([v10.2.1 L755](https://github.com/dotnet/orleans/blob/v10.2.1/src/Orleans.Streaming/PersistentStreams/PersistentStreamPullingAgent.cs#L755), [v10.3.1 L813](https://github.com/dotnet/orleans/blob/v10.3.1/src/Orleans.Streaming/PersistentStreams/PersistentStreamPullingAgent.cs#L813)).
   - 10.2.1 Event Hub: `EventHubAdapterReceiver.Cursor.Refresh` is empty ([EventHubAdapterReceiver.cs v10.2.1 L399-401](https://github.com/dotnet/orleans/blob/v10.2.1/src/Azure/Orleans.Streaming.EventHubs/Providers/Streams/EventHub/EventHubAdapterReceiver.cs#L399-L401)).
   - 10.3.1 Event Hub: it calls `cache.Refresh` ([EventHubAdapterReceiver.cs v10.3.1 L443-446](https://github.com/dotnet/orleans/blob/v10.3.1/src/Azure/Orleans.Streaming.EventHubs/Providers/Streams/EventHub/EventHubAdapterReceiver.cs#L443-L446), [EventHubQueueCache.cs v10.3.1 L141-144](https://github.com/dotnet/orleans/blob/v10.3.1/src/Azure/Orleans.Streaming.EventHubs/Providers/Streams/EventHub/EventHubQueueCache.cs#L141-L144)). That lands in the new `PooledQueueCache.Refresh`, which moves an idle cursor to the new event ([PooledQueueCache.cs v10.3.1 L123-155](https://github.com/dotnet/orleans/blob/v10.3.1/src/Orleans.Streaming/Common/PooledCache/PooledQueueCache.cs#L123-L155), not present in 10.2.1).
   - Memory streams: `MemoryPooledCache.Cursor.Refresh` is empty in both versions ([v10.2.1 L133-135](https://github.com/dotnet/orleans/blob/v10.2.1/src/Orleans.Streaming/MemoryStreams/MemoryPooledCache.cs#L133-L135), [v10.3.1 L135-137](https://github.com/dotnet/orleans/blob/v10.3.1/src/Orleans.Streaming/MemoryStreams/MemoryPooledCache.cs#L135-L137)).
3. Without the refresh, `RunConsumerCursor` calls `MoveNext`, and `TryGetNextMessage` re-seeks the idle cursor at its evicted position ([PooledQueueCache.cs v10.2.1 L282-289](https://github.com/dotnet/orleans/blob/v10.2.1/src/Orleans.Streaming/Common/PooledCache/PooledQueueCache.cs#L282-L289), [v10.3.1 L316-323](https://github.com/dotnet/orleans/blob/v10.3.1/src/Orleans.Streaming/Common/PooledCache/PooledQueueCache.cs#L316-L323)). The purge metadata has expired, so `SetCursor` throws ([v10.2.1 L216](https://github.com/dotnet/orleans/blob/v10.2.1/src/Orleans.Streaming/Common/PooledCache/PooledQueueCache.cs#L216), [v10.3.1 L250](https://github.com/dotnet/orleans/blob/v10.3.1/src/Orleans.Streaming/Common/PooledCache/PooledQueueCache.cs#L250)).
4. `RunConsumerCursor` catches it and recreates the cursor with `GetCacheCursor(streamId, null)`, which is the newest message, idle ([v10.2.1 L799-804](https://github.com/dotnet/orleans/blob/v10.2.1/src/Orleans.Streaming/PersistentStreams/PersistentStreamPullingAgent.cs#L799-L804), [v10.3.1 L862-867](https://github.com/dotnet/orleans/blob/v10.3.1/src/Orleans.Streaming/PersistentStreams/PersistentStreamPullingAgent.cs#L862-L867)). It then calls `ErrorProtocol(..., isDeliveryError: true, ...)`, which delivers `OnErrorAsync` ([v10.2.1 L867](https://github.com/dotnet/orleans/blob/v10.2.1/src/Orleans.Streaming/PersistentStreams/PersistentStreamPullingAgent.cs#L867), [v10.3.1 L953](https://github.com/dotnet/orleans/blob/v10.3.1/src/Orleans.Streaming/PersistentStreams/PersistentStreamPullingAgent.cs#L953)). The new event sits at or before that position, so it is never delivered.

The grain is not asked for its token on this path, so keep-alive does not matter. Prod window with the defaults: the next event on the entity arrives roughly 10.5 to 30 min after the previous one. The edges are fuzzy because metadata expiry is checked every 2 min (`MetadataMinTimeInCache / 5`) and inactivity cleanup runs every 3 min.

How to tell the two paths apart in logs: the handshake path's `Requested` token is the delivered batch token, which carries an `EventHubOffset`. The idle-cursor path's `Requested` token comes from the cache and has an empty offset (`EventHubOffset: ,`), see `EventHubDataAdapter.cs` L53 (cache token, `""` offset) vs L79 (batch token, real offset), same lines in [v10.2.1](https://github.com/dotnet/orleans/blob/v10.2.1/src/Azure/Orleans.Streaming.EventHubs/Providers/Streams/EventHub/EventHubDataAdapter.cs#L53-L79) and [v10.3.1](https://github.com/dotnet/orleans/blob/v10.3.1/src/Azure/Orleans.Streaming.EventHubs/Providers/Streams/EventHub/EventHubDataAdapter.cs#L53-L79). In `../data/dgrep` there are 65 unique messages with an offset and 1 without. The one without is entity `4371d781-...` at 2026-09-07T00:31:53.83Z (`Requested: SequenceNumber 2126, Low 2128, High 2129`). 2126 is where its 00:11:49 handshake had resumed, 20 min earlier. The parent analysis found no processing log after it, but that query only reached `Searching`.

### Other observations

- Scenario C: events 2..20 are evicted while event 1 is still being processed (6 s), so all versions report the miss and skip them. The miss moves the cursor to the newest position, and that position is evicted too before event 21 arrives 4 s later. On memory (both versions) and 10.2.1 Event Hub, event 21 then causes a second miss and is skipped, which is path E again. On 10.3.1 Event Hub the refresh moves the cursor to event 21, so it is delivered.
- The emulator reproduces the prod error text, for example `Requested: EventHubSequenceToken(EventHubOffset: 31984, SequenceNumber: 187, ...), Low: EventHubSequenceToken(EventHubOffset: , SequenceNumber: 232, ...)` (scenario A, 10.2.1).
- The Event Hubs emulator image runs natively on arm64 (no `platform` override needed).

## Files

| Path | What |
|---|---|
| `src/Poc/ConsumerGrain.cs` | consumer grain (mirrors `EntityConfigGrain`), `HandshakeProbe` call filter |
| `src/Poc/Scenarios.cs` | scenarios A-E, expectations, evaluation |
| `src/Poc/SiloHost.cs` | silo config per transport, scaled timings |
| `src/Poc/Timeline.cs`, `TimelineLoggerProvider.cs` | callback log the driver asserts on, silo log capture |
| `src/Poc/Report.cs`, `Program.cs` | JSON/markdown output, CLI |
| `Directory.Build.props`, `Directory.Packages.props` | `-p:OrleansVersion=...` switch, per-version output under `artifacts/<version>/` |
| `docker-compose.yml`, `emulator/Config.json` | Event Hubs emulator (1 partition, consumer group `orleans`) + Azurite |
| `run-all.sh` | full matrix and `results/summary.md` |
| `results/` | output of the last `./run-all.sh` |
