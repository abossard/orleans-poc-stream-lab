const gh = (v: string, path: string, lines = '') => `https://github.com/dotnet/orleans/blob/v${v}/src/${path}${lines && '#' + lines}`
const agent = (v: string, lines: string) => gh(v, 'Orleans.Streaming/PersistentStreams/PersistentStreamPullingAgent.cs', lines)
const cache = (v: string, lines: string) => gh(v, 'Orleans.Streaming/Common/PooledCache/PooledQueueCache.cs', lines)
const receiver = (v: string, lines: string) =>
  gh(v, 'Azure/Orleans.Streaming.EventHubs/Providers/Streams/EventHub/EventHubAdapterReceiver.cs', lines)

export type Term = { term: string; meaning: string; seeIt: string; links: [string, string][] }

export const TERMS: Term[] = [
  {
    term: 'PersistentStreamPullingAgent',
    meaning: 'Reads one queue (here the only Event Hub partition), puts new events into the queue cache and delivers them to the grains.',
    seeIt: 'top row of each stream (R, I, ▾)',
    links: [['10.2.1', agent('10.2.1', '')], ['10.3.1', agent('10.3.1', '')]],
  },
  {
    term: 'queue cache, PooledQueueCache',
    meaning: 'Keeps the newest events of one queue in memory, all streams mixed.',
    seeIt: 'the bar under each version',
    links: [['10.2.1', cache('10.2.1', '')], ['10.3.1', cache('10.3.1', '')]],
  },
  {
    term: 'StreamSequenceToken',
    meaning: 'The position of an event in the queue. The number counts every event on the partition, from all streams.',
    seeIt: 'seq numbers on the bar and in the log',
    links: [['EventHubDataAdapter L53-79', gh('10.3.1', 'Azure/Orleans.Streaming.EventHubs/Providers/Streams/EventHub/EventHubDataAdapter.cs', 'L53-L79')]],
  },
  {
    term: 'expectedToken (StreamHandshakeToken)',
    meaning: 'The grain keeps the token of the last event it got and returns it from GetSequenceToken().',
    seeIt: '"expectedToken" badge and marker, while the grain is active',
    links: [
      ['L191', gh('10.3.1', 'Orleans.Streaming/Internal/StreamSubscriptionHandleImpl.cs', 'L191')],
      ['L92-95', gh('10.3.1', 'Orleans.Streaming/Internal/StreamSubscriptionHandleImpl.cs', 'L92-L95')],
    ],
  },
  {
    term: 'handshake (DoHandshakeWithConsumer)',
    meaning: 'When the agent registers a stream, it asks the grain for its expectedToken and opens a cursor there (GetCacheCursor).',
    seeIt: '◆ GetSequenceToken',
    links: [['10.2.1 L320-334', agent('10.2.1', 'L320-L334')], ['GetCacheCursor L345', agent('10.2.1', 'L345')]],
  },
  {
    term: 'cacheToken',
    meaning: 'The token of the event that made the agent register the stream. 10.3.1 starts there when the cache no longer has expectedToken.',
    seeIt: 'Run A on 10.3.1: ● without ▲',
    links: [['10.2.1 AddSubscriber_Impl', agent('10.2.1', 'L282-L308')], ['10.3.1 catch', agent('10.3.1', 'L362-L373')]],
  },
  {
    term: 'StreamInactivityPeriod',
    meaning: 'The agent forgets a stream after this long without events (CleanupPubSubCache). Here 20 s, default 30 min.',
    seeIt: 'I marker, "registered" badge',
    links: [
      ['CleanupPubSubCache', agent('10.2.1', 'L575-L585')],
      ['default', gh('10.3.1', 'Orleans.Streaming/PersistentStreams/Options/PersistentStreamProviderOptions.cs', 'L131')],
    ],
  },
  {
    term: 'DataMinTimeInCache, DataMaxAgeInCache',
    meaning: 'The cache drops an event once it has spent DataMinTimeInCache in the cache and is DataMaxAgeInCache older than the newest event. Here 1 s and 3 s, default 5 min and 30 min.',
    seeIt: 'P marker, hatched part of the bar',
    links: [['TimePurgePredicate', gh('10.3.1', 'Orleans.Streaming/Common/PooledCache/TimePurgePredicate.cs', 'L32-L36')]],
  },
  {
    term: 'lastPurgedToken, MetadataMinTimeInCache',
    meaning:
      "When the cache drops a stream's event, it remembers the token as lastPurgedToken for MetadataMinTimeInCache. While it has it, an old token proves the stream lost nothing, and reading starts at the oldest cached event. Here 5 s, default 10 min.",
    seeIt: 'M marker, "lastPurgedToken" badge',
    links: [
      ['SetCursor', cache('10.2.1', 'L198-L220')],
      ['metadata expiry', cache('10.2.1', 'L140-L166')],
      ['default', gh('10.2.1', 'Orleans.Streaming/Common/RecoverableStreamOptions.cs', 'L36-L41')],
    ],
  },
  {
    term: 'QueueCacheMissException',
    meaning: 'SetCursor throws it when a token is older than the oldest cached event and the cache no longer has lastPurgedToken.',
    seeIt: '▲ OnErrorAsync, Requested < Low in the log',
    links: [['10.2.1 L216', cache('10.2.1', 'L216')], ['10.3.1 L250', cache('10.3.1', 'L250')]],
  },
  {
    term: 'ErrorProtocol, OnErrorAsync',
    meaning: "On 10.2.1 the handshake passes the miss to the grain's OnErrorAsync, then starts at cacheToken.",
    seeIt: 'Run A on 10.2.1: ▲ then ●',
    links: [['ErrorProtocol', agent('10.2.1', 'L354-L365')], ['cursor at cacheToken', agent('10.2.1', 'L368-L379')]],
  },
  {
    term: 'idle cursor, Cursor.Refresh, RunConsumerCursor',
    meaning:
      "After delivering, a stream's cursor waits at its last position. On 10.2.1 Event Hub, Refresh does nothing, so RunConsumerCursor misses, jumps to the newest event and skips the new one. On 10.3.1, Refresh moves the cursor to the new event.",
    seeIt: 'cursor badge "(purged)", ✕ in Run E on 10.2.1',
    links: [
      ['StartInactiveCursors', agent('10.2.1', 'L743-L760')],
      ['10.2.1 Refresh', receiver('10.2.1', 'L399-L401')],
      ['10.3.1 Refresh', receiver('10.3.1', 'L443-L446')],
      ['PooledQueueCache.Refresh', cache('10.3.1', 'L123-L155')],
      ['miss handling', agent('10.2.1', 'L799-L804')],
    ],
  },
  {
    term: 'activation collection (CollectionAge)',
    meaning: 'Orleans removes a grain that got no calls for CollectionAge. The new activation has no expectedToken, so the handshake cannot miss (Run B). Here 10 s, default 15 min.',
    seeIt: 'D marker, "grain not active" badge',
    links: [['CollectionAge', gh('10.3.1', 'Orleans.Runtime/Configuration/Options/GrainCollectionOptions.cs', 'L24')]],
  },
]
