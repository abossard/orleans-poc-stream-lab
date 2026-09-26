import { useEffect, useState } from 'react'
import { createRoot } from 'react-dom/client'
import { TERMS } from './terms'

type FeedEvent = { at: number; source: string; stream: string; kind: string; detail: string; version: number | null }
type Consumer = { state: string; cursor: string | null; cursorSeq: string | null; lastTokenSeq: string | null }
type StreamState = { key: string; registered: boolean; idleS: number; consumers: Consumer[] }
type Cache = { items: number; oldest: string | null; newest: string | null; lastPurgedToken: Record<string, { seq: string; ageS: number }> }
type State = {
  orleans: string
  transport: string
  status: string
  settings: { scenario: string; dataMaxAgeInCache: number; metadataMinTimeInCache: number; streamInactivityPeriod: number }
  scenarios: { id: string; title: string }[]
  snapshot: { cache: Cache | null; streams: StreamState[] } | null
}

// One backend per Orleans version (see live/compose.yml). Override with ?lanes=http://host:port,http://host:port
const LANES =
  new URLSearchParams(location.search).get('lanes')?.split(',') ?? [8021, 8031].map(p => `${location.protocol}//${location.hostname}:${p}`)
const LOST_AFTER_MS = 8000
const WINDOW_MS = 180_000
const MAIN_SCENARIOS = ['A', 'B', 'C', 'D', 'E']

// Row 0: pulling agent, row 1: grain, row 2: queue cache.
const MARKERS: Record<string, { glyph: string; row: number; cls: string; label: string }> = {
  SubscriptionAdded: { glyph: 'R', row: 0, cls: 'agent', label: 'stream registered (RegisterStream)' },
  GetSequenceToken: { glyph: '◆', row: 0, cls: 'hs', label: 'handshake: GetSequenceToken' },
  StreamInactive: { glyph: 'I', row: 0, cls: 'agent', label: 'removed after StreamInactivityPeriod' },
  Cursor: { glyph: '▾', row: 0, cls: 'cursor', label: 'cache cursor moved' },
  Published: { glyph: '○', row: 1, cls: 'pub', label: 'published' },
  OnNextAsync: { glyph: '●', row: 1, cls: 'ok', label: 'OnNextAsync (delivered)' },
  OnErrorAsync: { glyph: '▲', row: 1, cls: 'err', label: 'OnErrorAsync(QueueCacheMissException)' },
  Deactivated: { glyph: 'D', row: 1, cls: 'dim', label: 'activation collected' },
  Purged: { glyph: 'P', row: 2, cls: 'cache', label: "stream's token purged, lastPurgedToken set" },
  PurgeMetadataExpired: { glyph: 'M', row: 2, cls: 'meta', label: 'lastPurgedToken expired' },
}

const post = (url: string, body?: object) =>
  fetch(url, { method: 'POST', headers: { 'content-type': 'application/json' }, body: body && JSON.stringify(body) })
const seqs = (text: string) => [...text.matchAll(/(?:SequenceNumber: |SeqNum=)(\d+)/g)].map(m => m[1])
// Memory-stream tokens are tick-based, so show only their last digits.
const short = (seq: string) => (seq.length > 9 ? '…' + seq.slice(-4) : seq)
const pretty = (detail: string) =>
  detail
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
        <h1>Orleans persistent streams, live</h1>
        <div className="scenarios">
          {scenarios.filter(s => MAIN_SCENARIOS.includes(s.id)).map(s => (
            <div key={s.id}>
              <button onClick={() => all(`/api/scenarios/${s.id}/run`)}>Run {s.id}</button> {s.title}
            </div>
          ))}
        </div>
        <div className="row">
          Variants:
          {scenarios.filter(s => !MAIN_SCENARIOS.includes(s.id)).map(s => (
            <button key={s.id} title={s.title} onClick={() => all(`/api/scenarios/${s.id}/run`)}>{s.id}</button>
          ))}
          <form onSubmit={e => (e.preventDefault(), all('/api/publish', { key }))}>
            stream key <input value={key} onChange={e => setKey(e.target.value)} size={8} /> <button>Publish on both</button>
          </form>
          <select value={transport} onChange={e => setTransport(e.target.value)}>
            <option>eventhub</option>
            <option>memory</option>
          </select>
          <button onClick={() => all(`/api/reset?transport=${transport}`)}>Reset</button>
        </div>
        <p className="muted">
          Every scenario runs on its own silo in both lanes at once, then a fresh live silo starts. By hand: publish a key, wait 12 s and publish
          again for the idle-cursor path (E), or wait 26 s for the handshake path (A).
        </p>
        <p className="legend">
          {Object.values(MARKERS).map(m => (
            <span key={m.label}>
              <b className={m.cls}>{m.glyph}</b> {m.label}
            </span>
          ))}
          <span>
            <b className="err">✕</b> lost (published, no OnNextAsync after {LOST_AFTER_MS / 1000} s)
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
  const keys = [...new Set(events.map(e => e.stream).filter(Boolean))].sort((a, b) => Number(a.startsWith('warmup')) - Number(b.startsWith('warmup')))
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
  const consumerStreams = (state!.snapshot!.streams ?? []).filter(s => !s.key.startsWith('warmup'))
  const marks: { seq: bigint; text: string; cls: string }[] = []
  for (const s of consumerStreams) {
    const c = s.consumers[0]
    if (c?.cursorSeq) marks.push({ seq: BigInt(c.cursorSeq), text: `▾ cache cursor ${s.key} (${c.cursor})`, cls: 'cursor' })
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
          <span style={{ left: `${pos(m.seq)}%` }}>
            {m.text} seq {short(String(m.seq))}
            {m.seq < oldest ? ' older than oldest: purged' : ''}
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
    ? `registered, idle ${stream.idleS.toFixed(0)} s of ${state!.settings.streamInactivityPeriod} s`
    : lastOf(events, 'SubscriptionAdded', 'StreamInactive')?.kind === 'StreamInactive'
      ? 'removed as inactive'
      : 'not registered'
  const metadata = purged
    ? `lastPurgedToken ${short(purged.seq)}`
    : lastOf(events, 'Purged', 'PurgeMetadataExpired')?.kind === 'PurgeMetadataExpired'
      ? 'lastPurgedToken expired'
      : ''
  return (
    <div className={`stream ${streamKey.startsWith('warmup') ? 'warmup' : ''}`}>
      <div className="badges">
        <b>entity-update/{streamKey}</b>
        <span className={stream ? 'on' : ''}>{registered}</span>
        {consumer && (
          <span className={cursorStale ? 'bad' : 'on'}>
            cache cursor {consumer.cursor} at {short(consumer.cursorSeq ?? '?')}
            {cursorStale ? ', purged' : ''}
          </span>
        )}
        {metadata && <span className={purged ? 'on' : 'bad'}>{metadata}</span>}
        <span className={activationAlive(events) ? 'on' : ''}>
          {activationAlive(events) ? `activation alive${expected ? `, expectedToken ${short(expected)}` : ''}` : 'no activation'}
        </span>
      </div>
      <div className="track">
        {events.map((e, i) => {
          const m = MARKERS[e.kind]
          return (
            m && (
              <span key={i} className={`m ${lost(e) ? 'err' : m.cls}`} style={{ left: `${x(e.at)}%`, top: `${m.row * 14}px` }} title={`${e.kind} ${e.version ?? ''} ${pretty(e.detail)}`}>
                {lost(e) ? '✕' : m.glyph}
                {m.row === 1 && e.version}
              </span>
            )
          )
        })}
      </div>
    </div>
  )
}

createRoot(document.getElementById('root')!).render(<App />)
