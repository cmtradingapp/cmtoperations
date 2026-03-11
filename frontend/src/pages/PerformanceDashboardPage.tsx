import { useCallback, useEffect, useState } from 'react';
import axios from 'axios';
import {
  BarChart,
  Bar,
  XAxis,
  YAxis,
  Tooltip,
  ReferenceLine,
  ResponsiveContainer,
  Cell,
} from 'recharts';

// ---------------------------------------------------------------------------
// Axios instance (same auth pattern as other pages)
// ---------------------------------------------------------------------------

const api = axios.create({ baseURL: import.meta.env.VITE_API_BASE_URL || '/api' });
api.interceptors.request.use((config) => {
  const auth = JSON.parse(localStorage.getItem('auth') || '{}');
  const token = auth?.state?.token;
  if (token) config.headers.Authorization = `Bearer ${token}`;
  return config;
});

// ---------------------------------------------------------------------------
// Types — matched exactly to backend response shape
// ---------------------------------------------------------------------------

interface ScopeInfo {
  role: string;
  scope_type: 'own' | 'team' | 'all';
  username: string;
}

interface SummaryData {
  scope: ScopeInfo;
  target: { net: number };
  actuals: {
    net_deposit: number;
    deposits: number;
    withdrawals: number;
    unique_depositors: number;
    open_volume: number;
    total_exposure: number;
    unique_traders: number;
  };
  run_rate: {
    net_deposit: number;
    days_elapsed: number;
    days_in_month: number;
  };
  portfolio: {
    total_live_equity: number;
    total_exposure_usd: number;
    avg_score: number;
    score_distribution: Record<string, number>;
    status_breakdown: Record<string, number>;
    task_summary: Record<string, number>;
  };
}

// ---------------------------------------------------------------------------
// Design tokens
// ---------------------------------------------------------------------------

const C = {
  bg: '#060a12',
  card: '#0d1421',
  border: '#1a2744',
  teal: '#38bdf8',
  green: '#22c55e',
  red: '#ef4444',
  amber: '#f59e0b',
  textPrimary: '#e2e8f0',
  textMuted: '#64748b',
  textDim: '#334155',
} as const;

const SYNE = "'Syne', sans-serif";
const MONO = "'Space Mono', monospace";

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function fmtShort(n: number, unit = '$'): string {
  if (n == null) return '--';
  const abs = Math.abs(n);
  if (abs >= 1_000_000) return `${unit}${(n / 1_000_000).toFixed(1)}M`;
  if (abs >= 1_000) return `${unit}${(n / 1_000).toFixed(1)}k`;
  return `${unit}${n.toFixed(0)}`;
}

function fmtFull(n: number): string {
  if (n == null) return '--';
  return n.toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
}

function getStatusColor(pct: number): string {
  if (pct >= 100) return C.green;
  if (pct >= 80) return C.amber;
  return C.red;
}

function getStatusLabel(pct: number): string {
  if (pct >= 100) return 'Ahead';
  if (pct >= 80) return 'On Track';
  return 'Behind';
}

// ---------------------------------------------------------------------------
// Skeleton
// ---------------------------------------------------------------------------

function SkeletonCard() {
  return (
    <div
      style={{ background: C.card, border: `1px solid ${C.border}` }}
      className="rounded-xl p-4 animate-pulse space-y-3"
    >
      <div className="h-3 rounded w-24" style={{ background: C.border }} />
      <div className="h-7 rounded w-32" style={{ background: C.border }} />
      <div className="h-2 rounded w-full" style={{ background: C.border }} />
    </div>
  );
}

// ---------------------------------------------------------------------------
// KPI Card
// ---------------------------------------------------------------------------

interface KpiCardProps {
  label: string;
  actual: number;
  target?: number;
  unit?: string;
}

function KpiCard({ label, actual, target, unit = '$' }: KpiCardProps) {
  const hasTarget = target != null && target > 0;
  const pct = hasTarget ? Math.min((actual / target!) * 100, 100) : 0;
  const statusColor = hasTarget ? getStatusColor(pct) : C.textMuted;
  const statusLabel = hasTarget ? getStatusLabel(pct) : null;

  return (
    <div
      style={{ background: C.card, border: `1px solid ${C.border}` }}
      className="rounded-xl p-4 space-y-3"
    >
      {/* Label row */}
      <div className="flex items-center justify-between">
        <span
          style={{ fontFamily: SYNE, color: C.textMuted }}
          className="text-xs uppercase tracking-widest"
        >
          {label}
        </span>
        {statusLabel && (
          <span
            style={{
              fontFamily: SYNE,
              color: statusColor,
              background: statusColor + '20',
            }}
            className="text-xs font-bold px-2 py-0.5 rounded-full"
          >
            {statusLabel}
          </span>
        )}
      </div>

      {/* Value */}
      <div style={{ fontFamily: MONO, color: C.textPrimary }} className="text-2xl font-bold">
        {fmtShort(actual, unit)}
      </div>

      {/* Progress bar */}
      {hasTarget ? (
        <div className="space-y-1">
          <div className="h-1.5 rounded-full" style={{ background: '#1e293b' }}>
            <div
              className="h-full rounded-full transition-all duration-700"
              style={{ width: `${pct}%`, background: C.teal }}
            />
          </div>
          <div className="flex justify-between" style={{ color: C.textMuted }}>
            <span className="text-xs">Target: {fmtShort(target!, unit)}</span>
            <span className="text-xs" style={{ fontFamily: MONO }}>{pct.toFixed(0)}%</span>
          </div>
        </div>
      ) : (
        <div className="h-1.5 rounded-full" style={{ background: C.border }} />
      )}
    </div>
  );
}

// ---------------------------------------------------------------------------
// Run Rate Chart (Recharts)
// ---------------------------------------------------------------------------

interface ChartPoint {
  name: string;
  actual: number;
  target: number;
}

interface RunRateChartProps {
  data: SummaryData;
}

function RunRateChart({ data }: RunRateChartProps) {
  const chartData: ChartPoint[] = [
    { name: 'Net Deposit', actual: data.actuals.net_deposit, target: data.target.net },
    { name: 'Deposits', actual: data.actuals.deposits, target: 0 },
    { name: 'Withdrawals', actual: data.actuals.withdrawals, target: 0 },
  ].filter(d => d.actual !== 0 || d.target !== 0);

  const runRateValue = data.run_rate.net_deposit;

  const customTooltipStyle = {
    background: C.card,
    border: `1px solid ${C.border}`,
    borderRadius: '8px',
    color: C.textPrimary,
    fontFamily: MONO,
    fontSize: '12px',
  };

  return (
    <div
      style={{ background: C.card, border: `1px solid ${C.border}` }}
      className="rounded-xl p-4"
    >
      <div className="flex items-center justify-between mb-4">
        <span style={{ fontFamily: SYNE, color: C.textMuted }} className="text-xs uppercase tracking-widest">
          Run Rate vs Target
        </span>
        <div className="flex items-center gap-4 text-xs">
          <span className="flex items-center gap-1.5" style={{ color: C.teal }}>
            <span className="inline-block w-3 h-1.5 rounded-full" style={{ background: C.teal }} />
            Actual
          </span>
          <span className="flex items-center gap-1.5" style={{ color: C.textMuted }}>
            <span className="inline-block w-3 h-1.5 rounded-full" style={{ background: C.textDim }} />
            Target
          </span>
          <span className="flex items-center gap-1.5" style={{ color: C.amber }}>
            <span className="inline-block w-3 h-0.5 rounded-full" style={{ background: C.amber }} />
            Projected
          </span>
        </div>
      </div>
      <ResponsiveContainer width="100%" height={160}>
        <BarChart data={chartData} margin={{ top: 8, right: 8, left: 0, bottom: 0 }}>
          <XAxis
            dataKey="name"
            tick={{ fill: C.textMuted, fontSize: 11, fontFamily: SYNE }}
            axisLine={false}
            tickLine={false}
          />
          <YAxis
            tick={{ fill: C.textMuted, fontSize: 10, fontFamily: MONO }}
            axisLine={false}
            tickLine={false}
            tickFormatter={(v: number) => fmtShort(v, '$')}
            width={60}
          />
          <Tooltip
            contentStyle={customTooltipStyle}
            formatter={(value: number | undefined) => [`$${fmtFull(value ?? 0)}`, '']}
            labelStyle={{ color: C.textPrimary, fontFamily: SYNE }}
            cursor={{ fill: C.border }}
          />
          <Bar dataKey="actual" name="Actual" radius={[4, 4, 0, 0]} maxBarSize={48}>
            {chartData.map((_, i) => (
              <Cell key={i} fill={C.teal} />
            ))}
          </Bar>
          <Bar dataKey="target" name="Target" radius={[4, 4, 0, 0]} maxBarSize={48}>
            {chartData.map((_, i) => (
              <Cell key={i} fill={C.textDim} />
            ))}
          </Bar>
          {runRateValue > 0 && (
            <ReferenceLine
              y={runRateValue}
              stroke={C.amber}
              strokeDasharray="4 4"
              strokeWidth={1.5}
              label={{
                value: `Projected: ${fmtShort(runRateValue, '$')}`,
                fill: C.amber,
                fontSize: 10,
                fontFamily: MONO,
                position: 'insideTopRight',
              }}
            />
          )}
        </BarChart>
      </ResponsiveContainer>
    </div>
  );
}

// ---------------------------------------------------------------------------
// Month Progress
// ---------------------------------------------------------------------------

interface MonthProgressProps {
  daysElapsed: number;
  totalDays: number;
}

function MonthProgress({ daysElapsed, totalDays }: MonthProgressProps) {
  const pct = totalDays > 0 ? (daysElapsed / totalDays) * 100 : 0;
  return (
    <div
      style={{ background: C.card, border: `1px solid ${C.border}` }}
      className="rounded-xl p-4"
    >
      <div className="flex justify-between mb-2">
        <span style={{ fontFamily: SYNE, color: C.textMuted }} className="text-xs uppercase tracking-widest">
          Month Progress
        </span>
        <span style={{ fontFamily: MONO, color: C.textMuted }} className="text-xs">
          Day {daysElapsed} / {totalDays}
        </span>
      </div>
      <div className="h-2 rounded-full" style={{ background: '#1e293b' }}>
        <div
          className="h-full rounded-full transition-all duration-700"
          style={{ width: `${pct}%`, background: C.teal }}
        />
      </div>
      <div className="flex justify-between mt-1">
        <span style={{ color: C.textMuted }} className="text-xs">0%</span>
        <span style={{ fontFamily: MONO, color: C.teal }} className="text-xs font-bold">{pct.toFixed(0)}%</span>
      </div>
    </div>
  );
}

// ---------------------------------------------------------------------------
// Status Breakdown Panel
// ---------------------------------------------------------------------------

interface StatusBreakdownProps {
  breakdown: Record<string, number>;
}

function StatusBreakdown({ breakdown }: StatusBreakdownProps) {
  const entries = Object.entries(breakdown).sort((a, b) => b[1] - a[1]);
  const maxVal = Math.max(...entries.map(([, v]) => v), 1);

  return (
    <div
      style={{ background: C.card, border: `1px solid ${C.border}` }}
      className="rounded-xl p-4"
    >
      <span style={{ fontFamily: SYNE, color: C.textMuted }} className="text-xs uppercase tracking-widest block mb-4">
        Retention Status
      </span>
      {entries.length === 0 ? (
        <p style={{ color: C.textMuted }} className="text-sm text-center py-4">No status data</p>
      ) : (
        <div className="space-y-2.5 max-h-72 overflow-y-auto pr-1">
          {entries.map(([name, count]) => (
            <div key={name} className="flex items-center gap-3">
              <span
                style={{ color: C.textMuted, fontFamily: SYNE }}
                className="text-xs w-32 truncate flex-shrink-0"
                title={name}
              >
                {name}
              </span>
              <div className="flex-1 h-1.5 rounded-full" style={{ background: '#1e293b' }}>
                <div
                  className="h-full rounded-full transition-all duration-700"
                  style={{ width: `${(count / maxVal) * 100}%`, background: C.teal }}
                />
              </div>
              <span
                style={{ fontFamily: MONO, color: C.textPrimary }}
                className="text-xs w-8 text-right flex-shrink-0"
              >
                {count}
              </span>
            </div>
          ))}
        </div>
      )}
    </div>
  );
}

// ---------------------------------------------------------------------------
// Score Distribution Panel
// ---------------------------------------------------------------------------

interface ScoreDistributionProps {
  distribution: Record<string, number>;
}

const SCORE_COLORS = ['#1d4ed8', '#2563eb', '#3b82f6', '#22c55e'];

function ScoreDistribution({ distribution }: ScoreDistributionProps) {
  const buckets = ['0-25', '26-50', '51-75', '76-100'];
  const values = buckets.map(k => distribution[k] ?? 0);
  const maxVal = Math.max(...values, 1);

  return (
    <div
      style={{ background: C.card, border: `1px solid ${C.border}` }}
      className="rounded-xl p-4"
    >
      <span style={{ fontFamily: SYNE, color: C.textMuted }} className="text-xs uppercase tracking-widest block mb-4">
        Score Distribution
      </span>
      <div className="space-y-2.5">
        {buckets.map((bucket, i) => {
          const count = values[i];
          const color = SCORE_COLORS[i];
          return (
            <div key={bucket} className="flex items-center gap-3">
              <span
                style={{ color: C.textMuted, fontFamily: MONO }}
                className="text-xs w-14 flex-shrink-0"
              >
                {bucket}
              </span>
              <div className="flex-1 h-1.5 rounded-full" style={{ background: '#1e293b' }}>
                <div
                  className="h-full rounded-full transition-all duration-700"
                  style={{ width: `${(count / maxVal) * 100}%`, background: color }}
                />
              </div>
              <span
                style={{ fontFamily: MONO, color: C.textPrimary }}
                className="text-xs w-10 text-right flex-shrink-0"
              >
                {count}
              </span>
            </div>
          );
        })}
      </div>
    </div>
  );
}

// ---------------------------------------------------------------------------
// Task Summary Panel
// ---------------------------------------------------------------------------

interface TaskSummaryProps {
  tasks: Record<string, number>;
}

const TASK_PILL_COLORS = [C.teal, C.green, C.amber, '#a78bfa', '#f472b6', '#fb923c'];

function TaskSummary({ tasks }: TaskSummaryProps) {
  const entries = Object.entries(tasks).sort((a, b) => b[1] - a[1]);

  return (
    <div
      style={{ background: C.card, border: `1px solid ${C.border}` }}
      className="rounded-xl p-4"
    >
      <span style={{ fontFamily: SYNE, color: C.textMuted }} className="text-xs uppercase tracking-widest block mb-4">
        Task Summary
      </span>
      {entries.length === 0 ? (
        <p style={{ color: C.textMuted }} className="text-sm text-center py-4">No task data</p>
      ) : (
        <div className="flex flex-wrap gap-2">
          {entries.map(([name, count], i) => {
            const color = TASK_PILL_COLORS[i % TASK_PILL_COLORS.length];
            return (
              <span
                key={name}
                style={{
                  border: `1px solid ${color}40`,
                  background: color + '15',
                  fontFamily: SYNE,
                }}
                className="inline-flex items-center gap-1.5 px-3 py-1.5 rounded-full text-xs"
              >
                <span style={{ color: C.textPrimary }}>{name}</span>
                <span
                  style={{ background: color, fontFamily: MONO }}
                  className="text-xs font-bold rounded-full w-5 h-5 flex items-center justify-center text-white"
                >
                  {count}
                </span>
              </span>
            );
          })}
        </div>
      )}
    </div>
  );
}

// ---------------------------------------------------------------------------
// Portfolio Summary Panel
// ---------------------------------------------------------------------------

interface PortfolioSummaryProps {
  portfolio: SummaryData['portfolio'];
  netDepositMtd: number;
}

interface MetricTileProps {
  label: string;
  value: string;
  color?: string;
}

function MetricTile({ label, value, color }: MetricTileProps) {
  return (
    <div
      style={{ background: '#0a101e', border: `1px solid ${C.border}` }}
      className="rounded-lg p-3 space-y-1"
    >
      <span style={{ fontFamily: SYNE, color: C.textMuted }} className="text-xs uppercase tracking-widest block">
        {label}
      </span>
      <span
        style={{ fontFamily: MONO, color: color ?? C.textPrimary }}
        className="text-lg font-bold block"
      >
        {value}
      </span>
    </div>
  );
}

function PortfolioSummary({ portfolio, netDepositMtd }: PortfolioSummaryProps) {
  return (
    <div
      style={{ background: C.card, border: `1px solid ${C.border}` }}
      className="rounded-xl p-4"
    >
      <span style={{ fontFamily: SYNE, color: C.textMuted }} className="text-xs uppercase tracking-widest block mb-4">
        Portfolio Overview
      </span>
      <div className="grid grid-cols-2 gap-2">
        <MetricTile label="Live Equity" value={fmtShort(portfolio.total_live_equity)} color={C.teal} />
        <MetricTile label="Total Exposure" value={fmtShort(portfolio.total_exposure_usd)} />
        <MetricTile label="Avg Score" value={portfolio.avg_score.toFixed(1)} color={C.amber} />
        <MetricTile label="Net MTD" value={fmtShort(netDepositMtd)} color={netDepositMtd >= 0 ? C.green : C.red} />
      </div>
    </div>
  );
}

// ---------------------------------------------------------------------------
// Main Page
// ---------------------------------------------------------------------------

export function PerformanceDashboardPage() {
  const [data, setData] = useState<SummaryData | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState('');

  const fetchData = useCallback(async () => {
    try {
      const res = await api.get('/performance-dashboard/summary');
      setData(res.data);
      setError('');
    } catch (e: unknown) {
      const err = e as { response?: { data?: { detail?: string } } };
      setError(err.response?.data?.detail ?? 'Failed to load dashboard data');
    } finally {
      setLoading(false);
    }
  }, []);

  // Initial fetch + auto-refresh every 60s.
  // Polling is paused when the tab is hidden to avoid unnecessary server load
  // with 70 concurrent users (CLAUD-37).
  useEffect(() => {
    fetchData();
    const intervalRef = { current: setInterval(fetchData, 60_000) };

    const handleVisibility = () => {
      if (document.hidden) {
        clearInterval(intervalRef.current);
      } else {
        fetchData();
        intervalRef.current = setInterval(fetchData, 60_000);
      }
    };
    document.addEventListener('visibilitychange', handleVisibility);

    return () => {
      clearInterval(intervalRef.current);
      document.removeEventListener('visibilitychange', handleVisibility);
    };
  }, [fetchData]);

  // ---- Loading state ----
  if (loading && !data) {
    return (
      <div style={{ background: C.bg, fontFamily: SYNE }} className="min-h-full p-3 md:p-6 space-y-4 md:space-y-6">
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-3 md:gap-4">
          {Array.from({ length: 7 }).map((_, i) => <SkeletonCard key={i} />)}
        </div>
        <div className="grid grid-cols-1 lg:grid-cols-2 gap-3 md:gap-4">
          {Array.from({ length: 4 }).map((_, i) => <SkeletonCard key={i} />)}
        </div>
      </div>
    );
  }

  // ---- Error state ----
  if (error && !data) {
    return (
      <div style={{ background: C.bg }} className="min-h-full p-6 flex items-center justify-center">
        <div
          style={{ background: C.card, border: `1px solid ${C.red}40` }}
          className="rounded-xl p-8 text-center max-w-md"
        >
          <p style={{ color: C.red, fontFamily: SYNE }} className="font-semibold mb-4">{error}</p>
          <button
            onClick={() => { setLoading(true); fetchData(); }}
            style={{ background: C.red, fontFamily: SYNE }}
            className="px-4 py-2 rounded-lg text-white text-sm font-semibold hover:opacity-80 transition-opacity"
          >
            Retry
          </button>
        </div>
      </div>
    );
  }

  if (!data) return null;

  const { scope, target, actuals, run_rate, portfolio } = data;
  const de = run_rate.days_elapsed;
  const dm = run_rate.days_in_month;

  // Derive month name from current date (browser locale)
  const monthName = new Date().toLocaleString('en-US', { month: 'long', year: 'numeric' });

  // CLAUD-107: Role-aware page title and target label
  const pageTitle =
    scope.scope_type === 'own'
      ? `My Performance — ${scope.username}`
      : scope.scope_type === 'team'
        ? 'Team Performance'
        : 'Performance Dashboard';
  const targetLabel =
    scope.scope_type === 'own'
      ? 'Your monthly target'
      : scope.scope_type === 'team'
        ? 'Monthly target (team)'
        : 'Monthly target (all agents)';

  return (
    <div style={{ background: C.bg, fontFamily: SYNE }} className="min-h-full p-3 md:p-6 space-y-4 md:space-y-6">

      {/* ------------------------------------------------------------------ */}
      {/* Header                                                              */}
      {/* ------------------------------------------------------------------ */}
      <div className="flex items-center justify-between flex-wrap gap-2">
        <div className="flex items-center gap-2 md:gap-3">
          <span className="w-2 h-2 rounded-full bg-green-400 animate-pulse" />
          <h1 style={{ fontFamily: SYNE, color: C.textPrimary }} className="text-lg md:text-xl font-bold">
            {pageTitle}
          </h1>
          <span style={{ color: C.textMuted }} className="text-xs hidden md:inline">{monthName}</span>
          {loading && (
            <span style={{ color: C.textMuted }} className="text-xs animate-pulse">refreshing…</span>
          )}
        </div>
      </div>

      {/* ------------------------------------------------------------------ */}
      {/* KPI Cards                                                            */}
      {/* ------------------------------------------------------------------ */}
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-3 md:gap-4">
        <KpiCard label="Net Deposit" actual={actuals.net_deposit} target={target.net} unit="$" />
        <KpiCard label="Deposits" actual={actuals.deposits} unit="$" />
        <KpiCard label="Withdrawals" actual={actuals.withdrawals} unit="$" />
        <KpiCard label="Unique Depositors" actual={actuals.unique_depositors} unit="" />
        <KpiCard label="Open Volume" actual={actuals.open_volume} unit="$" />
        <KpiCard label="Total Exposure" actual={actuals.total_exposure} unit="$" />
        <KpiCard label="Unique Traders" actual={actuals.unique_traders} unit="" />

        {/* Run Rate Summary card */}
        <div
          style={{ background: C.card, border: `1px solid ${C.border}` }}
          className="rounded-xl p-4 space-y-3"
        >
          <span style={{ fontFamily: SYNE, color: C.textMuted }} className="text-xs uppercase tracking-widest">
            Run Rate
          </span>
          <div style={{ fontFamily: MONO, color: C.amber }} className="text-2xl font-bold">
            {fmtShort(run_rate.net_deposit, '$')}
          </div>
          <div>
            <div className="text-xs space-y-0.5" style={{ color: C.textMuted }}>
              <div>Projected net MTD</div>
              {target.net > 0 && (
                <div>
                  Gap:{' '}
                  <span
                    style={{
                      fontFamily: MONO,
                      color: run_rate.net_deposit >= target.net ? C.green : C.red,
                    }}
                  >
                    {run_rate.net_deposit >= target.net ? '+' : ''}
                    {fmtShort(run_rate.net_deposit - target.net, '$')}
                  </span>
                </div>
              )}
            </div>
          </div>
        </div>
      </div>

      {/* ------------------------------------------------------------------ */}
      {/* Run Rate Chart + Month Progress                                      */}
      {/* ------------------------------------------------------------------ */}
      <div className="grid grid-cols-1 lg:grid-cols-3 gap-3 md:gap-4">
        <div className="lg:col-span-2">
          <RunRateChart data={data} />
        </div>
        <div className="flex flex-col gap-3 md:gap-4">
          <MonthProgress daysElapsed={de} totalDays={dm} />

          {/* Net Target tile */}
          <div
            style={{ background: C.card, border: `1px solid ${C.border}` }}
            className="rounded-xl p-4 flex-1 flex flex-col justify-center"
          >
            <span style={{ fontFamily: SYNE, color: C.textMuted }} className="text-xs uppercase tracking-widest block mb-2">
              Net Target
            </span>
            <span style={{ fontFamily: MONO, color: C.textPrimary }} className="text-3xl font-bold">
              {fmtShort(target.net, '$')}
            </span>
            <span style={{ color: C.textMuted }} className="text-xs mt-1">
              {targetLabel}
            </span>
          </div>
        </div>
      </div>

      {/* ------------------------------------------------------------------ */}
      {/* Bottom panels                                                        */}
      {/* ------------------------------------------------------------------ */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-3 md:gap-4">
        <StatusBreakdown breakdown={portfolio.status_breakdown} />
        <ScoreDistribution distribution={portfolio.score_distribution} />
        <TaskSummary tasks={portfolio.task_summary} />
        <PortfolioSummary portfolio={portfolio} netDepositMtd={actuals.net_deposit} />
      </div>

    </div>
  );
}
