const gh = (v: string, path: string, lines = '') => `https://github.com/dotnet/orleans/blob/v${v}/src/${path}${lines && '#' + lines}`
const agent = (v: string, lines: string) => gh(v, 'Orleans.Streaming/PersistentStreams/PersistentStreamPullingAgent.cs', lines)
const cache = (v: string, lines: string) => gh(v, 'Orleans.Streaming/Common/PooledCache/PooledQueueCache.cs', lines)
const receiver = (v: string, lines: string) =>
  gh(v, 'Azure/Orleans.Streaming.EventHubs/Providers/Streams/EventHub/EventHubAdapterReceiver.cs', lines)

export type Term = { term: string; meaning: string; seeIt: string; links: [string, string][] }

// Source lines as cited in ../../why-it-stopped.md.
export const TERMS: Term[] = [
  {
    term: 'PersistentStreamPullingAgent',
    meaning: 'Reads one queue (here the single Event Hub partition), fills the queue cache and delivers events to consumers.',
    seeIt: 'top row of each swimlane (R, I, cursor)',
    links: [['10.2.1', agent('10.2.1', '')], ['10.3.1', agent('10.3.1', '')]],
  },
  {
    term: 'queue cache, PooledQueueCache',
    meaning: 'In-memory cache of the newest events of one queue, from oldest to newest sequence number.',
    seeIt: 'queue cache strip',
    links: [['10.2.1', cache('10.2.1', '')], ['10.3.1', cache('10.3.1', '')]],
  },
  {
    term: 'StreamSequenceToken',
    meaning: 'Position of an event in the queue. Sequence numbers count every event on the partition, not per stream.',
    seeIt: 'seq numbers on the strip and in the log',
    links: [['EventHubDataAdapter L53-79', gh('10.3.1', 'Azure/Orleans.Streaming.EventHubs/Providers/Streams/EventHub/EventHubDataAdapter.cs', 'L53-L79')]],
  },
  {
    term: 'expectedToken (StreamHandshakeToken)',
    meaning: "The grain's subscription handle keeps the last delivered token and returns it from GetSequenceToken().",
    seeIt: '"expectedToken" badge and marker (while the activation is alive)',
    links: [
      ['L191', gh('10.3.1', 'Orleans.Streaming/Internal/StreamSubscriptionHandleImpl.cs', 'L191')],
      ['L92-95', gh('10.3.1', 'Orleans.Streaming/Internal/StreamSubscriptionHandleImpl.cs', 'L92-L95')],
    ],
  },
  {
    term: 'handshake (DoHandshakeWithConsumer)',
    meaning: 'When the agent registers a stream it asks the consumer for its expectedToken and calls GetCacheCursor(expectedToken).',
    seeIt: '◆ GetSequenceToken',
    links: [['10.2.1 L320-334', agent('10.2.1', 'L320-L334')], ['GetCacheCursor L345', agent('10.2.1', 'L345')]],
  },
  {
    term: 'cacheToken',
    meaning: 'Token of the event that made the agent register the stream. 10.3.1 resumes there when expectedToken was purged.',
    seeIt: 'A on 10.3.1: OnNextAsync without ▲',
    links: [['10.2.1 AddSubscriber_Impl', agent('10.2.1', 'L282-L308')], ['10.3.1 catch', agent('10.3.1', 'L362-L373')]],
  },
  {
    term: 'StreamInactivityPeriod',
    meaning: 'CleanupPubSubCache drops a stream after this long without events. PoC 20 s, default 30 min.',
    seeIt: 'I marker, "registered" badge',
    links: [
      ['CleanupPubSubCache', agent('10.2.1', 'L575-L585')],
      ['default', gh('10.3.1', 'Orleans.Streaming/PersistentStreams/Options/PersistentStreamProviderOptions.cs', 'L131')],
    ],
  },
  {
    term: 'DataMinTimeInCache, DataMaxAgeInCache',
    meaning: 'Eviction: an event is purged once it is older than DataMaxAgeInCache relative to the newest event. PoC 1 s / 3 s, prod 10 s / 30 s.',
    seeIt: 'P marker, hatched part of the strip',
    links: [['TimePurgePredicate', gh('10.3.1', 'Orleans.Streaming/Common/PooledCache/TimePurgePredicate.cs', 'L32-L36')]],
  },
  {
    term: 'lastPurgedToken, MetadataMinTimeInCache',
    meaning:
      "After purging a stream's event the cache keeps lastPurgedToken[stream] for MetadataMinTimeInCache. While it exists a stale token resumes at the oldest event instead of throwing. PoC 5 s, default 10 min.",
    seeIt: 'M marker, "lastPurgedToken" badge',
    links: [
      ['SetCursor', cache('10.2.1', 'L198-L220')],
      ['metadata expiry', cache('10.2.1', 'L140-L166')],
      ['default', gh('10.2.1', 'Orleans.Streaming/Common/RecoverableStreamOptions.cs', 'L36-L41')],
    ],
  },
  {
    term: 'QueueCacheMissException',
    meaning: 'SetCursor throws it when the token is older than the oldest cached event and lastPurgedToken is gone.',
    seeIt: '▲ OnErrorAsync, Requested < Low in the log',
    links: [['10.2.1 L216', cache('10.2.1', 'L216')], ['10.3.1 L250', cache('10.3.1', 'L250')]],
  },
  {
    term: 'ErrorProtocol, OnErrorAsync',
    meaning: 'On 10.2.1 the handshake forwards the miss to the consumer, then sets the cursor at cacheToken.',
    seeIt: 'A on 10.2.1: ▲ then ●',
    links: [['ErrorProtocol', agent('10.2.1', 'L354-L365')], ['cursor at cacheToken', agent('10.2.1', 'L368-L379')]],
  },
  {
    term: 'idle cursor, Cursor.Refresh, RunConsumerCursor',
    meaning:
      "A registered stream's cursor idles at its last position. 10.2.1 Event Hub Refresh does nothing, so RunConsumerCursor misses, resets the cursor to the newest event and the new event is lost. 10.3.1 Refresh moves the idle cursor to it.",
    seeIt: 'cursor badge "(purged)", ✕ lost in E on 10.2.1',
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
    meaning: 'Idle activations are collected. A new activation has no expectedToken, so the handshake cannot miss (scenario B). PoC 10 s, default 15 min.',
    seeIt: 'D marker, "no activation" badge',
    links: [['CollectionAge', gh('10.3.1', 'Orleans.Runtime/Configuration/Options/GrainCollectionOptions.cs', 'L24')]],
  },
]
