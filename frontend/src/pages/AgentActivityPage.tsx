import axios from 'axios';
import { useCallback, useEffect, useRef, useState } from 'react';

const api = axios.create({ baseURL: import.meta.env.VITE_API_BASE_URL || '/api' });
api.interceptors.request.use((config) => {
  const auth = JSON.parse(localStorage.getItem('auth') || '{}');
  const token = auth?.state?.token;
  if (token) config.headers.Authorization = `Bearer ${token}`;
  return config;
});

// ── Types ────────────────────────────────────────────────────────────────────

interface AgentKpi {
  contacted: number;
  traders: number;
  depositors: number;
  netDeposit: number;
  volume: number;
  callsMade: number;
  talkTimeSecs: number;
  target: number | null;
  callbacksSet: number;
  runRate: number | null;
  contactRate: number | null;
  avgCallSecs: number;
}

interface AgentRow {
  id: number;
  name: string;
  initials: string;
  team: string;
  status: 'on_call' | 'available' | 'offline';
  lastSeen: string | null;
  shiftElapsed: string | null;
  kpi: AgentKpi;
  tasks: Record<string, number>;
  portfolioClients: number;
}

// ── Helpers ──────────────────────────────────────────────────────────────────

function fmtMoney(v: number): string {
  return '$' + Math.round(v).toLocaleString('en-US');
}

function fmtNum(v: number): string {
  return Math.round(v).toLocaleString('en-US');
}

function fmtDuration(secs: number): string {
  const h = Math.floor(secs / 3600);
  const m = Math.floor((secs % 3600) / 60);
  if (h > 0) return `${h}h ${String(m).padStart(2, '0')}m`;
  return `${m}m`;
}

function fmtAvgCall(secs: number): string {
  const m = Math.floor(secs / 60);
  const s = secs % 60;
  return `${m}:${String(s).padStart(2, '0')}`;
}

function runRateColor(rr: number | null): string {
  if (rr === null) return '#94a3b8';
  if (rr >= 80) return '#22c55e';
  if (rr >= 55) return '#f59e0b';
  return '#ef4444';
}

function contactRateColor(cr: number | null): string {
  if (cr === null) return '#94a3b8';
  if (cr >= 72) return '#22c55e';
  if (cr >= 58) return '#f59e0b';
  return '#ef4444';
}

// ── Sub-components ───────────────────────────────────────────────────────────

function StatusBadge({ status }: { status: AgentRow['status'] }) {
  const cfg = {
    on_call:   { bg: '#f0fdf4', color: '#16a34a', border: '#bbf7d0', dot: '#16a34a', label: 'On Call',   blink: true },
    available: { bg: '#eff6ff', color: '#2563eb', border: '#bfdbfe', dot: '#2563eb', label: 'Available', blink: false },
    offline:   { bg: '#f8fafc', color: '#94a3b8', border: '#e2e8f0', dot: '#cbd5e1', label: 'Offline',   blink: false },
  }[status];

  return (
    <span style={{
      display: 'inline-flex', alignItems: 'center', gap: 5,
      padding: '2px 8px', borderRadius: 4,
      background: cfg.bg, color: cfg.color,
      border: `1px solid ${cfg.border}`,
      fontSize: 10, fontWeight: 700,
    }}>
      <span style={{
        width: 6, height: 6, borderRadius: '50%',
        background: cfg.dot,
        animation: cfg.blink ? 'pulse 1.5s ease-in-out infinite' : 'none',
      }} />
      {cfg.label}
    </span>
  );
}

function ExpandedRow({ agent }: { agent: AgentRow }) {
  const k = agent.kpi;
  const taskEntries = Object.entries(agent.tasks);
  const maxTask = Math.max(...taskEntries.map(([, c]) => c), 1);

  return (
    <div style={{ padding: '16px 20px', background: '#f8fafc', borderTop: '1px solid #f1f5f9' }}>
      <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr 1fr 1fr', gap: 12 }}>

        {/* Panel 1 – Daily Performance */}
        <div style={{ background: '#fff', border: '1px solid #e2e8f0', borderRadius: 8, padding: '14px 16px' }}>
          <div style={{ fontSize: 10, fontWeight: 700, color: '#94a3b8', textTransform: 'uppercase', letterSpacing: '0.05em', marginBottom: 10 }}>
            Daily Performance
          </div>
          <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 8 }}>
            {[
              { label: 'Contacted',   value: fmtNum(k.contacted) },
              { label: 'Calls Made',  value: fmtNum(k.callsMade) },
              { label: 'Traders',     value: fmtNum(k.traders) },
              { label: 'Depositors',  value: fmtNum(k.depositors) },
              { label: 'Net Deposit', value: fmtMoney(k.netDeposit) },
              { label: 'Volume',      value: fmtMoney(k.volume) },
              { label: 'Talk Time',   value: fmtDuration(k.talkTimeSecs) },
              { label: 'Avg Call',    value: fmtAvgCall(k.avgCallSecs) },
              { label: 'Callbacks',   value: k.callbacksSet },
              { label: 'Target',      value: k.target !== null ? `$${k.target.toLocaleString()}` : '—' },
              { label: 'Contact%',    value: k.contactRate !== null ? `${k.contactRate.toFixed(0)}%` : '—' },
              { label: 'Run Rate',    value: k.runRate !== null ? `${k.runRate.toFixed(0)}%` : '—' },
            ].map(({ label, value }) => (
              <div key={label} style={{ padding: '6px 8px', background: '#f8fafc', borderRadius: 6 }}>
                <div style={{ fontSize: 9, color: '#94a3b8', textTransform: 'uppercase', letterSpacing: '0.04em' }}>{label}</div>
                <div style={{ fontSize: 13, fontWeight: 600, color: '#0f172a', fontFamily: 'monospace', marginTop: 2 }}>
                  {value}
                </div>
              </div>
            ))}
          </div>
        </div>

        {/* Panel 2 – Tasks by Type */}
        <div style={{ background: '#fff', border: '1px solid #e2e8f0', borderRadius: 8, padding: '14px 16px' }}>
          <div style={{ fontSize: 10, fontWeight: 700, color: '#94a3b8', textTransform: 'uppercase', letterSpacing: '0.05em', marginBottom: 10 }}>
            Tasks by Type
          </div>
          {taskEntries.length === 0 ? (
            <div style={{ fontSize: 11, color: '#94a3b8', paddingTop: 8 }}>No pending tasks</div>
          ) : taskEntries.map(([type, count]) => (
            <div key={type} style={{ marginBottom: 8 }}>
              <div style={{ display: 'flex', justifyContent: 'space-between', fontSize: 10, color: '#64748b', marginBottom: 3 }}>
                <span>{type}</span>
                <span style={{ fontWeight: 700, color: '#0d9488' }}>{count}</span>
              </div>
              <div style={{ height: 5, background: '#f1f5f9', borderRadius: 3 }}>
                <div style={{
                  width: `${(count / maxTask) * 100}%`,
                  height: '100%', background: '#0d9488', borderRadius: 3,
                }} />
              </div>
            </div>
          ))}
        </div>

        {/* Panel 3 – Efficiency KPIs (CLAUD-162) */}
        <div style={{ background: '#fff', border: '1px solid #e2e8f0', borderRadius: 8, padding: '14px 16px' }}>
          <div style={{ fontSize: 10, fontWeight: 700, color: '#94a3b8', textTransform: 'uppercase', letterSpacing: '0.05em', marginBottom: 10 }}>
            Efficiency
          </div>
          {(() => {
            const depositConv = k.contacted > 0 ? Math.round((k.depositors / k.contacted) * 100) : 0;
            const traderConv = k.contacted > 0 ? Math.round((k.traders / k.contacted) * 100) : 0;
            const rrVal = k.runRate ?? 0;
            const crVal = k.contactRate ?? 0;
            const bars = [
              { label: 'Run Rate', pct: rrVal, color: runRateColor(rrVal), sub: `${k.contacted} of ${k.target ?? '?'} target` },
              { label: 'Contact Rate', pct: crVal, color: contactRateColor(crVal), sub: `${k.contacted} contacts / ${k.callsMade} calls` },
              { label: 'Deposit Conv.', pct: depositConv, color: '#22c55e', sub: `${k.depositors} depositors` },
              { label: 'Trader Conv.', pct: traderConv, color: '#2563eb', sub: `${k.traders} traders` },
            ];
            return bars.map(({ label, pct, color, sub }) => (
              <div key={label} style={{ marginBottom: 12 }}>
                <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: 4 }}>
                  <span style={{ fontSize: 10.5, color: '#475569', fontWeight: 500 }}>{label}</span>
                  <span style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: 11, fontWeight: 700, color }}>{pct}%</span>
                </div>
                <div style={{ height: 5, background: '#f1f5f9', borderRadius: 3, marginBottom: 3 }}>
                  <div style={{ width: `${Math.min(pct, 100)}%`, height: '100%', borderRadius: 3, background: color }} />
                </div>
                <div style={{ fontSize: 9, color: '#94a3b8' }}>{sub}</div>
              </div>
            ));
          })()}
        </div>

        {/* Panel 4 – Status card */}
        <div style={{ background: '#fff', border: '1px solid #e2e8f0', borderRadius: 8, padding: '14px 16px' }}>
          <div style={{ fontSize: 10, fontWeight: 700, color: '#94a3b8', textTransform: 'uppercase', letterSpacing: '0.05em', marginBottom: 10 }}>
            Shift Status
          </div>
          <div style={{ display: 'flex', alignItems: 'center', gap: 10, marginBottom: 12 }}>
            <div style={{
              width: 36, height: 36, borderRadius: '50%',
              background: 'linear-gradient(135deg, #0d9488, #14b8a6)',
              display: 'flex', alignItems: 'center', justifyContent: 'center',
              color: '#fff', fontSize: 12, fontWeight: 700,
            }}>
              {agent.initials}
            </div>
            <div>
              <div style={{ fontSize: 12, fontWeight: 600, color: '#0f172a' }}>{agent.name}</div>
              <div style={{ fontSize: 10, color: '#64748b' }}>{agent.team || '—'}</div>
            </div>
          </div>
          <StatusBadge status={agent.status} />
          {agent.lastSeen && (
            <div style={{ fontSize: 10, color: '#94a3b8', marginTop: 8 }}>
              Last seen: {agent.lastSeen}
            </div>
          )}
          {agent.shiftElapsed && (
            <div style={{ fontSize: 10, color: '#64748b', marginTop: 4 }}>
              Talk time today: {agent.shiftElapsed}
            </div>
          )}
          <div style={{ marginTop: 12, padding: '6px 8px', background: '#f8fafc', borderRadius: 6 }}>
            <div style={{ fontSize: 9, color: '#94a3b8', textTransform: 'uppercase' }}>Callbacks Set</div>
            <div style={{ fontSize: 14, fontWeight: 700, color: '#0d9488', fontFamily: 'monospace' }}>{k.callbacksSet}</div>
          </div>
        </div>
      </div>
    </div>
  );
}

// ── Main Component ────────────────────────────────────────────────────────────

type SortKey = 'name' | 'runRate' | 'netDeposit' | 'contacted' | 'callsMade';

export function AgentActivityPage() {
  const [agents, setAgents] = useState<AgentRow[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [expandedId, setExpandedId] = useState<number | null>(null);
  const [teamFilter, setTeamFilter] = useState<string>('All');
  const [sortKey, setSortKey] = useState<SortKey>('runRate');
  const pollRef = useRef<ReturnType<typeof setInterval> | null>(null);

  const load = useCallback(async () => {
    try {
      const res = await api.get('/agent-activity');
      setAgents(res.data);
      setError(null);
    } catch {
      setError('Failed to load agent activity.');
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    load();
    pollRef.current = setInterval(load, 60_000);
    return () => {
      if (pollRef.current) clearInterval(pollRef.current);
    };
  }, [load]);

  // Derive team list
  const teams = ['All', ...Array.from(new Set(agents.map(a => a.team).filter(Boolean)))];

  // CLAUD-160: only show agents who are online OR have any activity today
  const hasActivity = (a: AgentRow) => {
    const totalTasks = Object.values(a.tasks).reduce((s, c) => s + c, 0);
    return a.status !== 'offline' || a.kpi.callsMade > 0 || a.kpi.contacted > 0 || a.kpi.netDeposit > 0 || totalTasks > 0;
  };

  // Filter + sort
  const visible = agents
    .filter(hasActivity)
    .filter(a => teamFilter === 'All' || a.team === teamFilter)
    .sort((a, b) => {
      // Offline always last
      const offA = a.status === 'offline' ? 1 : 0;
      const offB = b.status === 'offline' ? 1 : 0;
      if (offA !== offB) return offA - offB;

      switch (sortKey) {
        case 'runRate':      return (b.kpi.runRate ?? -1) - (a.kpi.runRate ?? -1);
        case 'netDeposit':   return b.kpi.netDeposit - a.kpi.netDeposit;
        case 'contacted':    return b.kpi.contacted - a.kpi.contacted;
        case 'callsMade':    return b.kpi.callsMade - a.kpi.callsMade;
        default:             return a.name.localeCompare(b.name);
      }
    });

  // Totals row (online agents only)
  const online = visible.filter(a => a.status !== 'offline');
  const totals = online.reduce(
    (acc, a) => ({
      contacted:        acc.contacted        + a.kpi.contacted,
      traders:          acc.traders          + a.kpi.traders,
      depositors:       acc.depositors       + a.kpi.depositors,
      netDeposit:       acc.netDeposit       + a.kpi.netDeposit,
      volume:           acc.volume           + a.kpi.volume,
      callsMade:        acc.callsMade        + a.kpi.callsMade,
      talkTimeSecs:     acc.talkTimeSecs     + a.kpi.talkTimeSecs,
      tasks:            acc.tasks            + Object.values(a.tasks).reduce((s, c) => s + c, 0),
      portfolioClients: acc.portfolioClients + a.portfolioClients,
    }),
    { contacted: 0, traders: 0, depositors: 0, netDeposit: 0, volume: 0, callsMade: 0, talkTimeSecs: 0, tasks: 0, portfolioClients: 0 }
  );
  const avgRunRate   = online.filter(a => a.kpi.runRate !== null).reduce((s, a) => s + (a.kpi.runRate ?? 0), 0) / (online.filter(a => a.kpi.runRate !== null).length || 1);
  const avgContact   = online.filter(a => a.kpi.contactRate !== null).reduce((s, a) => s + (a.kpi.contactRate ?? 0), 0) / (online.filter(a => a.kpi.contactRate !== null).length || 1);

  // Column header click to sort
  function SortTh({ col, label }: { col: SortKey; label: string }) {
    const active = sortKey === col;
    return (
      <th
        onClick={() => setSortKey(col)}
        style={{
          padding: '7px 12px', fontSize: 9.5, fontWeight: 700,
          textTransform: 'uppercase', color: active ? '#0d9488' : '#94a3b8',
          letterSpacing: '0.05em', cursor: 'pointer', whiteSpace: 'nowrap',
          userSelect: 'none', textAlign: col === 'name' ? 'left' : 'center',
        }}
      >
        {label}{active ? ' ↓' : ''}
      </th>
    );
  }

  const TH_STYLE: React.CSSProperties = {
    padding: '7px 12px', fontSize: 9.5, fontWeight: 700,
    textTransform: 'uppercase', color: '#94a3b8', letterSpacing: '0.05em',
    whiteSpace: 'nowrap', textAlign: 'center',
  };

  const TD: React.CSSProperties = {
    padding: '9px 12px', fontSize: 11, borderBottom: '1px solid #f1f5f9',
    whiteSpace: 'nowrap', textAlign: 'center',
  };

  if (loading) {
    return (
      <div style={{ padding: 32, color: '#94a3b8', fontSize: 13 }}>Loading agent activity…</div>
    );
  }

  return (
    <div style={{ background: '#f1f5f9', minHeight: '100vh', padding: '20px 24px' }}>
      <style>{`
        @keyframes pulse {
          0%, 100% { opacity: 1; }
          50% { opacity: 0.35; }
        }
      `}</style>

      {/* Header */}
      <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: 16 }}>
        <div>
          <h1 style={{ margin: 0, fontSize: 20, fontWeight: 700, color: '#0f172a' }}>Agent Activity</h1>
          <div style={{ fontSize: 11, color: '#64748b', marginTop: 2 }}>
            {visible.length} agent{visible.length !== 1 ? 's' : ''} active · refreshes every 60s
          </div>
        </div>

        {/* Team filter chips */}
        <div style={{ display: 'flex', gap: 6 }}>
          {teams.map(t => (
            <button
              key={t}
              onClick={() => setTeamFilter(t)}
              style={{
                padding: '5px 14px', borderRadius: 20, fontSize: 11, fontWeight: 600,
                border: '1px solid',
                borderColor: teamFilter === t ? '#0d9488' : '#e2e8f0',
                background:  teamFilter === t ? '#0d9488' : '#fff',
                color:       teamFilter === t ? '#fff'    : '#64748b',
                cursor: 'pointer',
              }}
            >
              {t}
            </button>
          ))}
        </div>
      </div>

      {error && (
        <div style={{ background: '#fef2f2', border: '1px solid #fecaca', borderRadius: 8, padding: '10px 16px', color: '#dc2626', fontSize: 12, marginBottom: 16 }}>
          {error}
        </div>
      )}

      {/* Table */}
      <div style={{ background: '#fff', borderRadius: 10, border: '1px solid #e2e8f0', overflow: 'hidden' }}>
        <table style={{ width: '100%', borderCollapse: 'collapse' }}>
          <thead style={{ background: '#f8fafc', borderBottom: '1px solid #f1f5f9' }}>
            <tr>
              <SortTh col="name"       label="Agent" />
              <th style={TH_STYLE}>Status</th>
              <th style={TH_STYLE}>Shift</th>
              <SortTh col="contacted"  label="Contacted" />
              <th style={TH_STYLE}>Traders</th>
              <th style={TH_STYLE}>Depos.</th>
              <SortTh col="netDeposit" label="Net Dep" />
              <th style={TH_STYLE}>Volume</th>
              <SortTh col="callsMade"  label="Calls" />
              <th style={TH_STYLE}>Contact%</th>
              <SortTh col="runRate"    label="Run Rate" />
              <th style={TH_STYLE}>Tasks</th>
              <th style={TH_STYLE}>Portfolio</th>
            </tr>
          </thead>
          <tbody>
            {visible.length === 0 && (
              <tr>
                <td colSpan={13} style={{ ...TD, textAlign: 'center', color: '#94a3b8', padding: '40px 0' }}>
                  No agents found
                </td>
              </tr>
            )}

            {/* Totals row — top (CLAUD-160) */}
            {visible.length > 0 && (
              <tr style={{ background: '#f0fdfa', borderBottom: '2px solid #0d9488' }}>
                <td style={{ ...TD, textAlign: 'left', fontWeight: 700, color: '#0d9488' }}>
                  <div style={{ display: 'flex', alignItems: 'center', gap: 6 }}>
                    <span style={{ fontSize: 14 }}>Σ</span> Totals ({online.length} online)
                  </div>
                </td>
                <td style={TD} />
                <td style={{ ...TD, fontFamily: 'monospace', fontWeight: 700, color: '#0d9488' }}>
                  {fmtDuration(totals.talkTimeSecs)}
                </td>
                <td style={{ ...TD, fontFamily: 'monospace', fontWeight: 700, color: '#0d9488' }}>
                  {fmtNum(totals.contacted)}
                </td>
                <td style={{ ...TD, fontFamily: 'monospace', fontWeight: 700, color: '#0d9488' }}>
                  {fmtNum(totals.traders)}
                </td>
                <td style={{ ...TD, fontFamily: 'monospace', fontWeight: 700, color: '#0d9488' }}>
                  {fmtNum(totals.depositors)}
                </td>
                <td style={{ ...TD, fontFamily: 'monospace', fontWeight: 700, color: '#0d9488' }}>
                  {fmtMoney(totals.netDeposit)}
                </td>
                <td style={{ ...TD, fontFamily: 'monospace', fontWeight: 700, color: '#0d9488' }}>
                  {fmtMoney(totals.volume)}
                </td>
                <td style={{ ...TD, fontFamily: 'monospace', fontWeight: 700, color: '#0d9488' }}>
                  {fmtNum(totals.callsMade)}
                </td>
                <td style={{ ...TD, fontFamily: 'monospace', fontWeight: 700, color: contactRateColor(avgContact) }}>
                  {online.length > 0 ? `${avgContact.toFixed(0)}%` : '—'}
                </td>
                <td style={TD}>
                  {online.length > 0 ? (
                    <div style={{ display: 'flex', flexDirection: 'column', alignItems: 'center' }}>
                      <div style={{ display: 'flex', alignItems: 'center', gap: 4, marginBottom: 3 }}>
                        <span style={{ fontFamily: 'monospace', fontSize: 11, fontWeight: 700, color: runRateColor(avgRunRate) }}>
                          {avgRunRate.toFixed(0)}%
                        </span>
                      </div>
                      <div style={{ width: 36, height: 4, background: '#ccfbf1', borderRadius: 2 }}>
                        <div style={{
                          width: `${Math.min(100, avgRunRate)}%`,
                          height: '100%', borderRadius: 2,
                          background: runRateColor(avgRunRate),
                        }} />
                      </div>
                    </div>
                  ) : <span style={{ color: '#94a3b8' }}>—</span>}
                </td>
                <td style={{ ...TD, fontFamily: 'monospace', fontWeight: 700, color: '#0d9488' }}>
                  {totals.tasks > 0 ? fmtNum(totals.tasks) : '—'}
                </td>
                <td style={{ ...TD, fontFamily: 'monospace', fontWeight: 700, color: '#0d9488' }}>
                  {totals.portfolioClients > 0 ? fmtNum(totals.portfolioClients) : '—'}
                </td>
              </tr>
            )}

            {visible.map(agent => {
              const k = agent.kpi;
              const offline = agent.status === 'offline';
              const expanded = expandedId === agent.id;
              const taskTotal = Object.values(agent.tasks).reduce((s, c) => s + c, 0);

              return [
                <tr
                  key={agent.id}
                  onClick={() => setExpandedId(expanded ? null : agent.id)}
                  style={{
                    opacity: offline ? 0.45 : 1,
                    cursor: 'pointer',
                    background: expanded ? '#f0fdfa' : 'transparent',
                    transition: 'background 0.15s',
                  }}
                  onMouseEnter={e => { if (!expanded) (e.currentTarget as HTMLElement).style.background = '#f8fafc'; }}
                  onMouseLeave={e => { if (!expanded) (e.currentTarget as HTMLElement).style.background = 'transparent'; }}
                >
                  {/* Agent */}
                  <td style={{ ...TD, textAlign: 'left' }}>
                    <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
                      <div style={{
                        width: 28, height: 28, borderRadius: '50%',
                        background: 'linear-gradient(135deg, #0d9488, #14b8a6)',
                        display: 'flex', alignItems: 'center', justifyContent: 'center',
                        color: '#fff', fontSize: 10, fontWeight: 700, flexShrink: 0,
                      }}>
                        {agent.initials}
                      </div>
                      <div>
                        <div style={{ fontWeight: 600, color: '#0f172a', fontSize: 12 }}>{agent.name}</div>
                        {agent.team && <div style={{ fontSize: 10, color: '#94a3b8' }}>{agent.team}</div>}
                      </div>
                    </div>
                  </td>

                  {/* Status */}
                  <td style={TD}><StatusBadge status={agent.status} /></td>

                  {/* Shift */}
                  <td style={{ ...TD, fontFamily: 'monospace', color: '#64748b' }}>
                    {agent.shiftElapsed ?? '—'}
                  </td>

                  {/* Contacted */}
                  <td style={{ ...TD, fontFamily: 'monospace', fontWeight: 600, color: '#0f172a' }}>
                    {fmtNum(k.contacted)}
                  </td>

                  {/* Traders */}
                  <td style={{ ...TD, fontFamily: 'monospace', color: '#64748b' }}>{fmtNum(k.traders)}</td>

                  {/* Depositors */}
                  <td style={{ ...TD, fontFamily: 'monospace', color: '#64748b' }}>{fmtNum(k.depositors)}</td>

                  {/* Net Deposit */}
                  <td style={{ ...TD, fontFamily: 'monospace', fontWeight: 600, color: k.netDeposit >= 0 ? '#16a34a' : '#dc2626' }}>
                    {fmtMoney(k.netDeposit)}
                  </td>

                  {/* Volume */}
                  <td style={{ ...TD, fontFamily: 'monospace', color: '#64748b' }}>{fmtMoney(k.volume)}</td>

                  {/* Calls */}
                  <td style={{ ...TD, fontFamily: 'monospace', color: '#64748b' }}>{fmtNum(k.callsMade)}</td>

                  {/* Contact% */}
                  <td style={{ ...TD, fontFamily: 'monospace', fontWeight: 600, color: contactRateColor(k.contactRate) }}>
                    {k.contactRate !== null ? `${k.contactRate.toFixed(0)}%` : '—'}
                  </td>

                  {/* Run Rate */}
                  <td style={TD}>
                    {k.runRate !== null ? (
                      <div style={{ display: 'flex', flexDirection: 'column', alignItems: 'center' }}>
                        <div style={{ display: 'flex', alignItems: 'center', gap: 4, marginBottom: 3 }}>
                          <span style={{ fontFamily: 'monospace', fontSize: 11, fontWeight: 600, color: runRateColor(k.runRate) }}>
                            {k.runRate.toFixed(0)}%
                          </span>
                        </div>
                        <div style={{ width: 36, height: 4, background: '#f1f5f9', borderRadius: 2 }}>
                          <div style={{
                            width: `${Math.min(100, k.runRate)}%`,
                            height: '100%', borderRadius: 2,
                            background: runRateColor(k.runRate),
                          }} />
                        </div>
                      </div>
                    ) : <span style={{ color: '#94a3b8' }}>—</span>}
                  </td>

                  {/* Tasks */}
                  <td style={{ ...TD, fontFamily: 'monospace', color: taskTotal > 0 ? '#0d9488' : '#94a3b8', fontWeight: taskTotal > 0 ? 600 : 400 }}>
                    {taskTotal > 0 ? fmtNum(taskTotal) : '—'}
                  </td>

                  {/* Portfolio */}
                  <td style={{ ...TD, fontFamily: 'monospace', color: agent.portfolioClients > 0 ? '#0f172a' : '#94a3b8' }}>
                    {agent.portfolioClients > 0 ? fmtNum(agent.portfolioClients) : '—'}
                  </td>
                </tr>,

                // Expanded detail row
                expanded && (
                  <tr key={`${agent.id}-expanded`}>
                    <td colSpan={13} style={{ padding: 0 }}>
                      <ExpandedRow agent={agent} />
                    </td>
                  </tr>
                ),
              ];
            })}

          </tbody>
        </table>
      </div>
    </div>
  );
}
