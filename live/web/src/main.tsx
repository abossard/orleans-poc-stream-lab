import { useEffect, useState } from 'react'
import { createRoot } from 'react-dom/client'
import { TERMS } from './terms'

type FeedEvent = { at: number; source: string; stream: string; kind: string; detail: string; version: number | null }
type Consumer = { cursor: string | null; cursorSeq: string | null }
type StreamState = { key: string; idleS: number; consumers: Consumer[] }
type Cache = { items: number; oldest: string | null; newest: string | null; lastPurgedToken: Record<string, { seq: string; ageS: number }> }
type State = {
  orleans: string
  transport: string
  status: string
  settings: { scenario: string; dataMaxAgeInCache: number; metadataMinTimeInCache: number; streamInactivityPeriod: number }
  scenarios: { id: string; title: string }[]
  snapshot: { cache: Cache | null; streams: StreamState[] } | null
}

// One backend per Orleans version, see live/compose.yml.
const LANES = [8102, 8103].map(p => `${location.protocol}//${location.hostname}:${p}`)
const LOST_AFTER_MS = 8000
const WINDOW_MS = 180_000
const MAIN_SCENARIOS = ['A', 'B', 'C', 'D', 'E']

// Rows: 0 pulling agent, 1 grain callbacks, 2 events on S, 3 queue cache.
const MARKERS: Record<string, { glyph: string; row: number; cls: string; label: string }> = {
  SubscriptionAdded: { glyph: 'R', row: 0, cls: 'agent', label: 'agent registers the stream' },
  GetSequenceToken: { glyph: '◆', row: 0, cls: 'hs', label: 'handshake: agent asks the grain for its token' },
  StreamInactive: { glyph: 'I', row: 0, cls: 'agent', label: 'agent forgets the quiet stream' },
  Cursor: { glyph: '▾', row: 0, cls: 'cursor', label: 'cursor moves' },
  OnErrorAsync: { glyph: '▲', row: 1, cls: 'err', label: 'grain gets QueueCacheMissException' },
  Deactivated: { glyph: 'D', row: 1, cls: 'dim', label: 'Orleans removes the idle grain' },
  Published: { glyph: '○', row: 2, cls: 'pub', label: 'published' },
  OnNextAsync: { glyph: '●', row: 2, cls: 'ok', label: 'delivered (OnNextAsync)' },
  Purged: { glyph: 'P', row: 3, cls: 'cache', label: "cache drops the stream's event" },
  PurgeMetadataExpired: { glyph: 'M', row: 3, cls: 'meta', label: 'cache forgets lastPurgedToken' },
}

const post = (url: string, body?: object) =>
  fetch(url, { method: 'POST', headers: { 'content-type': 'application/json' }, body: body && JSON.stringify(body) })
const seqs = (text: string) => [...text.matchAll(/(?:SequenceNumber: |SeqNum=)(\d+)/g)].map(m => m[1])
// Memory-stream tokens are tick-based, so show only their last digits.
const short = (seq: string) => (seq.length > 9 ? '…' + seq.slice(-4) : seq)
const pretty = (detail: string) =>
  detail
    .replace(/Orleans\.Streams\.QueueCacheMissException: Item not found in cache\.\s+/, 'QueueCacheMissException ')
    .replace(/EventHubSequenceToken\(EventHubOffset: (\d*), SequenceNumber: (\d+), EventIndex: \d+\)/g, (_, o, s) => `seq ${s} ${o ? `(offset ${o})` : '(empty offset)'}`)
    .replace(/\[EventSequenceToken: SeqNum=(\d+), EventIndex=\d+\]/g, (_, s) => `seq ${short(s)}`)
    .replace(/\d{12,}/g, short)
const lastOf = (events: FeedEvent[], ...kinds: string[]) => events.findLast(e => kinds.includes(e.kind))
const activationAlive = (events: FeedEvent[]) => lastOf(events, 'Activated', 'Deactivated')?.kind === 'Activated'
const expectedToken = (events: FeedEvent[]) => (activationAlive(events) ? seqs(lastOf(events, 'OnNextAsync')?.detail ?? '')[0] : undefined)

function useLane(url: string) {
  const [events, setEvents] = useState<FeedEvent[]>([])
  const [state, setState] = useState<State>()
  useEffect(() => {
    const source = new EventSource(`${url}/api/events`)
    source.onopen = () => setEvents([]) // the backend replays its history on every (re)connect
    source.onmessage = m => {
      const e: FeedEvent = JSON.parse(m.data)
      setEvents(prev => (e.kind === 'Reset' ? [] : [...prev, e]))
    }
    return () => source.close()
  }, [url])
  useEffect(() => {
    const poll = () => fetch(`${url}/api/state`).then(r => r.json()).then(setState, () => setState(undefined))
    poll()
    const id = setInterval(poll, 500)
    return () => clearInterval(id)
  }, [url])
  return { events, state }
}

function App() {
  const [scenarios, setScenarios] = useState<State['scenarios']>([])
  const [key, setKey] = useState('s1')
  const [transport, setTransport] = useState('eventhub')
  const all = (path: string, body?: object) => LANES.forEach(url => post(url + path, body))
  useEffect(() => {
    fetch(`${LANES[0]}/api/state`).then(r => r.json()).then((s: State) => (setScenarios(s.scenarios), setTransport(s.transport)), () => {})
  }, [])
  return (
    <>
      <header className="top">
        <h1>Orleans persistent streams: 10.2.1 and 10.3.1 side by side</h1>
        <div className="scenarios">
          {scenarios.filter(s => MAIN_SCENARIOS.includes(s.id)).map(s => (
            <div key={s.id}>
              <button onClick={() => all(`/api/scenarios/${s.id}/run`)}>Run {s.id}</button> {s.title}
            </div>
          ))}
        </div>
        <div className="row">
          Cache-size variants:
          {scenarios.filter(s => !MAIN_SCENARIOS.includes(s.id)).map(s => (
            <button key={s.id} title={s.title} onClick={() => all(`/api/scenarios/${s.id}/run`)}>{s.id}</button>
          ))}
          <form onSubmit={e => (e.preventDefault(), all('/api/publish', { key }))}>
            stream key <input value={key} onChange={e => setKey(e.target.value)} size={8} /> <button>Publish on both versions</button>
          </form>
          <select value={transport} onChange={e => setTransport(e.target.value)}>
            <option>eventhub</option>
            <option>memory</option>
          </select>
          <button onClick={() => all(`/api/reset?transport=${transport}`)}>Reset</button>
        </div>
        <p className="muted">
          Each scenario runs on Orleans 10.2.1 (left) and 10.3.1 (right) at the same time. When it ends, you can publish by hand: publish a key,
          then publish it again after 12 s (idle-cursor path) or after 26 s (handshake path).
        </p>
        <p className="legend">
          {Object.values(MARKERS).map(m => (
            <span key={m.label}>
              <b className={m.cls}>{m.glyph}</b> {m.label}
            </span>
          ))}
          <span>
            <b className="err">✕</b> lost: not delivered after {LOST_AFTER_MS / 1000} s
          </span>
        </p>
      </header>
      <main>
        <div className="lanes">
          {LANES.map(url => (
            <Lane key={url} url={url} />
          ))}
        </div>
        <aside>
          <h2>Terms</h2>
          {TERMS.map(t => (
            <div key={t.term} className="term">
              <b>{t.term}</b> {t.meaning} <i>See: {t.seeIt}.</i>{' '}
              {t.links.map(([label, href]) => (
                <a key={href} href={href} target="_blank" rel="noreferrer">
                  {label}
                </a>
              ))}
            </div>
          ))}
        </aside>
      </main>
    </>
  )
}

function Lane({ url }: { url: string }) {
  const { events, state } = useLane(url)
  const now = Date.now()
  const start = Math.max(events[0]?.at ?? now, now - WINDOW_MS)
  const t0 = (events.find(e => e.kind === 'Step') ?? events[0])?.at ?? now
  const span = Math.max(40_000, now - start)
  const x = (at: number) => ((at - start) / span) * 100
  // Warm-up streams (Scenarios.WarmUp) only prove the receiver reads; they stay in the log.
  const keys = [...new Set(events.map(e => e.stream).filter(k => k && !k.startsWith('warmup')))]
  const result = lastOf(events, 'Result', 'Error')
  const ticks = []
  for (let t = Math.ceil((start - t0) / 10_000) * 10; t0 + t * 1000 <= start + span; t += 10) ticks.push(t)
  return (
    <section className="lane">
      <h2>
        Orleans {state?.orleans ?? '?'} <small>{state ? `${state.transport} · ${state.status}` : `offline (${url})`}</small>
      </h2>
      {state && (
        <p className="muted">
          scenario {state.settings.scenario} settings: DataMaxAgeInCache {state.settings.dataMaxAgeInCache} s, MetadataMinTimeInCache{' '}
          {state.settings.metadataMinTimeInCache} s, StreamInactivityPeriod {state.settings.streamInactivityPeriod} s
        </p>
      )}
      {result && <p className={`result ${result.detail.startsWith('PASS') ? 'pass' : 'fail'}`}>{result.detail}</p>}
      <CacheStrip state={state} events={events} />
      <div className="axis">
        {ticks.map(t => (
          <span key={t} style={{ left: `${x(t0 + t * 1000)}%` }}>{t} s</span>
        ))}
      </div>
      {keys.map(k => (
        <StreamRow key={k} streamKey={k} events={events.filter(e => e.stream === k)} state={state} x={x} now={now} />
      ))}
      <ol className="log" reversed>
        {events
          .slice(-80)
          .reverse()
          .map((e, i) => (
            <li key={i} className={MARKERS[e.kind]?.cls}>
              <code>{((e.at - t0) / 1000).toFixed(2).padStart(6)}</code> {e.source} <b>{e.stream}</b> {e.kind} {e.version ?? ''} {pretty(e.detail)}
            </li>
          ))}
      </ol>
    </section>
  )
}

function CacheStrip({ state, events }: { state?: State; events: FeedEvent[] }) {
  const cache = state?.snapshot?.cache
  if (!cache?.oldest || !cache.newest) {
    return <div className="strip muted">queue cache: empty</div>
  }
  const [oldest, newest] = [BigInt(cache.oldest), BigInt(cache.newest)]
  const consumerStreams = state!.snapshot!.streams.filter(s => !s.key.startsWith('warmup'))
  const marks: { seq: bigint; text: string; cls: string }[] = []
  for (const s of consumerStreams) {
    const c = s.consumers[0]
    if (c?.cursorSeq) marks.push({ seq: BigInt(c.cursorSeq), text: `▾ cursor ${s.key} (${c.cursor})`, cls: 'cursor' })
  }
  for (const [k, p] of Object.entries(cache.lastPurgedToken).filter(([k]) => !k.startsWith('warmup'))) {
    marks.push({ seq: BigInt(p.seq), text: `M lastPurgedToken ${k} (${p.ageS.toFixed(1)} s old)`, cls: 'meta' })
  }
  for (const k of new Set(events.map(e => e.stream).filter(k => k && !k.startsWith('warmup')))) {
    const seq = expectedToken(events.filter(e => e.stream === k))
    if (seq) marks.push({ seq: BigInt(seq), text: `● expectedToken ${k} (grain)`, cls: 'ok' })
  }
  const lo = marks.reduce((m, { seq }) => (seq < m ? seq : m), oldest)
  const pos = (seq: bigint) => (Number(seq - lo) / Math.max(1, Number(newest - lo))) * 100
  return (
    <div className="strip">
      <div className="bar">
        <div className="purged" style={{ width: `${pos(oldest)}%` }} />
        <div className="cached" style={{ left: `${pos(oldest)}%` }} />
      </div>
      <div className="ends">
        <span>{lo < oldest ? `purged ${short(String(lo))}` : ''}</span>
        <span>
          queue cache: {cache.items} messages, oldest {short(cache.oldest)}, newest {short(cache.newest)}
        </span>
      </div>
      {marks.map(m => (
        <div key={m.text} className={`mark ${m.cls} ${m.seq < oldest ? 'stale' : ''}`}>
          <span style={{ left: `${pos(m.seq)}%`, transform: pos(m.seq) > 50 ? 'translateX(-100%)' : undefined }}>
            {m.text} seq {short(String(m.seq))}
            {m.seq < oldest ? ' (purged)' : ''}
          </span>
        </div>
      ))}
    </div>
  )
}

function StreamRow({ streamKey, events, state, x, now }: { streamKey: string; events: FeedEvent[]; state?: State; x: (at: number) => number; now: number }) {
  const stream = state?.snapshot?.streams.find(s => s.key === streamKey)
  const consumer = stream?.consumers[0]
  const oldest = state?.snapshot?.cache?.oldest
  const purged = state?.snapshot?.cache?.lastPurgedToken[streamKey]
  const delivered = new Set(events.filter(e => e.kind === 'OnNextAsync').map(e => e.version))
  const lost = (e: FeedEvent) => e.kind === 'Published' && !delivered.has(e.version) && now - e.at > LOST_AFTER_MS
  const cursorStale = !!consumer?.cursorSeq && !!oldest && BigInt(consumer.cursorSeq) < BigInt(oldest)
  const expected = expectedToken(events)
  const registered = stream
    ? `registered, quiet ${stream.idleS.toFixed(0)} s of ${state!.settings.streamInactivityPeriod} s`
    : lastOf(events, 'SubscriptionAdded', 'StreamInactive')?.kind === 'StreamInactive'
      ? 'forgotten by the agent'
      : 'not registered'
  const metadata = purged
    ? `lastPurgedToken ${short(purged.seq)}`
    : lastOf(events, 'Purged', 'PurgeMetadataExpired')?.kind === 'PurgeMetadataExpired'
      ? 'lastPurgedToken expired'
      : ''
  return (
    <div className="stream">
      <div className="badges">
        <b>consumer/{streamKey}</b>
        <span className={stream ? 'on' : ''}>{registered}</span>
        {consumer && (
          <span className={cursorStale ? 'bad' : 'on'}>
            cursor {consumer.cursor} at {short(consumer.cursorSeq ?? '?')}
            {cursorStale ? ' (purged)' : ''}
          </span>
        )}
        {metadata && <span className={purged ? 'on' : 'bad'}>{metadata}</span>}
        <span className={activationAlive(events) ? 'on' : ''}>
          {activationAlive(events) ? `grain active${expected ? `, expectedToken ${short(expected)}` : ''}` : 'grain not active'}
        </span>
      </div>
      <div className="track">
        {events.map((e, i) => {
          const m = MARKERS[e.kind]
          return (
            m && (
              <span key={i} className={`m ${lost(e) ? 'err' : m.cls}`} style={{ left: `${x(e.at)}%`, top: `${m.row * 14}px` }} title={`${e.kind} ${e.version ?? ''} ${pretty(e.detail)}`}>
                {lost(e) ? '✕' : m.glyph}
                {m.row === 2 && e.version}
              </span>
            )
          )
        })}
      </div>
    </div>
  )
}

createRoot(document.getElementById('root')!).render(<App />)
