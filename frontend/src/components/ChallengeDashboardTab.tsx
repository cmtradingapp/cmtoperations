/**
 * CLAUD-181: Challenges Dashboard Tab — analytics overview with KPIs, charts, and tables.
 * Auto-refreshes every 60 seconds. Supports dark/light theme.
 */
import { useCallback, useEffect, useRef, useState } from 'react';
import {
  LineChart, Line, XAxis, YAxis, Tooltip, ResponsiveContainer, Legend,
  PieChart, Pie, Cell,
  BarChart, Bar,
} from 'recharts';

import { getChallengeDashboard } from '../api/challenges';
import type {
  ChallengeDashboardData,
  PerChallengeItem,
  TopEarnerItem,
  StreakItem,
  DiversityItem,
} from '../api/challenges';

// ---------------------------------------------------------------------------
// Colour palette (theme-aware via CSS vars / Tailwind dark:)
// ---------------------------------------------------------------------------

const CHART_COLORS = ['#3b82f6', '#22c55e', '#f59e0b', '#ef4444', '#8b5cf6', '#ec4899', '#06b6d4', '#f97316'];

// ---------------------------------------------------------------------------
// Formatters
// ---------------------------------------------------------------------------

function fmtUSD(n: number | null | undefined): string {
  if (n == null) return '$0.00';
  return '$' + Number(n).toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
}

function fmtPct(n: number | null | undefined): string {
  if (n == null) return '0.0%';
  return Number(n).toFixed(1) + '%';
}

function fmtDate(s: string | null | undefined): string {
  if (!s) return '--';
  const d = new Date(s);
  if (isNaN(d.getTime())) return s;
  const dd = String(d.getDate()).padStart(2, '0');
  const mm = String(d.getMonth() + 1).padStart(2, '0');
  const yyyy = d.getFullYear();
  return `${dd}/${mm}/${yyyy}`;
}

function fmtShortDate(s: string | null | undefined): string {
  if (!s) return '--';
  const d = new Date(s);
  if (isNaN(d.getTime())) return s;
  return `${String(d.getDate()).padStart(2, '0')}/${String(d.getMonth() + 1).padStart(2, '0')}`;
}

function fmtNum(n: number | null | undefined): string {
  if (n == null) return '0';
  return Number(n).toLocaleString('en-US');
}

// ---------------------------------------------------------------------------
// KPI Card
// ---------------------------------------------------------------------------

interface KpiCardProps {
  label: string;
  value: string;
  sublabel?: string;
  accent?: 'blue' | 'green' | 'amber' | 'red' | 'purple' | 'default';
}

function KpiCard({ label, value, sublabel, accent = 'default' }: KpiCardProps) {
  const accentBorder: Record<string, string> = {
    blue: 'border-l-blue-500',
    green: 'border-l-green-500',
    amber: 'border-l-amber-500',
    red: 'border-l-red-500',
    purple: 'border-l-purple-500',
    default: 'border-l-gray-300 dark:border-l-gray-600',
  };
  const accentText: Record<string, string> = {
    blue: 'text-blue-600 dark:text-blue-400',
    green: 'text-green-600 dark:text-green-400',
    amber: 'text-amber-600 dark:text-amber-400',
    red: 'text-red-600 dark:text-red-400',
    purple: 'text-purple-600 dark:text-purple-400',
    default: 'text-gray-900 dark:text-gray-100',
  };

  return (
    <div className={`bg-white dark:bg-gray-800 border border-gray-200 dark:border-gray-700 border-l-4 ${accentBorder[accent]} rounded-lg p-4 shadow-sm`}>
      <p className="text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider mb-1">{label}</p>
      <p className={`text-2xl font-bold ${accentText[accent]}`}>{value}</p>
      {sublabel && <p className="text-xs text-gray-400 dark:text-gray-500 mt-1">{sublabel}</p>}
    </div>
  );
}

// ---------------------------------------------------------------------------
// Loading skeleton
// ---------------------------------------------------------------------------

function Skeleton() {
  return (
    <div className="space-y-6 animate-pulse">
      <div className="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-5 gap-4">
        {Array.from({ length: 9 }).map((_, i) => (
          <div key={i} className="bg-gray-200 dark:bg-gray-700 rounded-lg h-24" />
        ))}
      </div>
      <div className="grid grid-cols-1 xl:grid-cols-2 gap-4">
        <div className="bg-gray-200 dark:bg-gray-700 rounded-lg h-72" />
        <div className="bg-gray-200 dark:bg-gray-700 rounded-lg h-72" />
      </div>
      <div className="bg-gray-200 dark:bg-gray-700 rounded-lg h-64" />
    </div>
  );
}

// ---------------------------------------------------------------------------
// Sub-table tab selector for bottom tables section
// ---------------------------------------------------------------------------

type TableTab = 'per_challenge' | 'top_earners' | 'streaks' | 'diversity';

// ---------------------------------------------------------------------------
// Main Component
// ---------------------------------------------------------------------------

export function ChallengeDashboardTab() {
  const [data, setData] = useState<ChallengeDashboardData | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState('');
  const [tableTab, setTableTab] = useState<TableTab>('per_challenge');
  const intervalRef = useRef<ReturnType<typeof setInterval> | null>(null);

  const fetchData = useCallback(async () => {
    try {
      const res = await getChallengeDashboard();
      setData(res);
      setError('');
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Failed to load dashboard');
    } finally {
      setLoading(false);
    }
  }, []);

  // Initial fetch + 60s auto-refresh with visibility pause
  useEffect(() => {
    fetchData();
    intervalRef.current = setInterval(fetchData, 60_000);

    const handleVisibility = () => {
      if (document.hidden) {
        if (intervalRef.current) clearInterval(intervalRef.current);
      } else {
        fetchData();
        intervalRef.current = setInterval(fetchData, 60_000);
      }
    };
    document.addEventListener('visibilitychange', handleVisibility);

    return () => {
      if (intervalRef.current) clearInterval(intervalRef.current);
      document.removeEventListener('visibilitychange', handleVisibility);
    };
  }, [fetchData]);

  // -- Loading state --
  if (loading && !data) return <Skeleton />;

  // -- Error state --
  if (error && !data) {
    return (
      <div className="flex items-center justify-center py-20">
        <div className="bg-red-50 dark:bg-red-900/20 border border-red-200 dark:border-red-700 rounded-lg p-6 text-center max-w-md">
          <p className="text-red-600 dark:text-red-400 font-medium mb-3">{error}</p>
          <button
            onClick={() => { setLoading(true); fetchData(); }}
            className="px-4 py-2 bg-red-600 text-white text-sm rounded-lg hover:bg-red-700 transition-colors"
          >
            Retry
          </button>
        </div>
      </div>
    );
  }

  if (!data) return null;

  const { snapshot, funnel_7d, daily_trend, type_distribution, payout_by_group, optimove_health, per_challenge, top_earners, streak_leaderboard, diversity } = data;

  // Reverse daily_trend so oldest is on left
  const trendData = [...daily_trend].reverse();

  // Credit API health colour
  const apiHealthOk = snapshot.credit_api_success_rate_pct >= 95;

  return (
    <div className="space-y-6">
      {/* Refreshing indicator */}
      {loading && data && (
        <div className="text-xs text-gray-400 dark:text-gray-500 animate-pulse">Refreshing...</div>
      )}

      {/* ============================================================ */}
      {/* Row 1 — Live Snapshot KPIs                                    */}
      {/* ============================================================ */}
      <div className="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-5 gap-4">
        <KpiCard
          label="Active Challenges"
          value={fmtNum(snapshot.active_challenges)}
          accent="blue"
        />
        <KpiCard
          label="Clients Today"
          value={fmtNum(snapshot.clients_in_progress_today)}
          sublabel="In Progress"
          accent="amber"
        />
        <KpiCard
          label="Completions Today"
          value={fmtNum(snapshot.completions_today)}
          accent="green"
        />
        <KpiCard
          label="USD Paid Today"
          value={fmtUSD(snapshot.usd_paid_today)}
          accent="purple"
        />
        <KpiCard
          label="Credit API Health"
          value={fmtPct(snapshot.credit_api_success_rate_pct)}
          accent={apiHealthOk ? 'green' : 'red'}
          sublabel={apiHealthOk ? 'Healthy' : 'Degraded'}
        />
      </div>

      {/* ============================================================ */}
      {/* Row 2 — 7-Day Funnel KPIs                                     */}
      {/* ============================================================ */}
      <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
        <KpiCard
          label="Clients Started (7d)"
          value={fmtNum(funnel_7d.started_7d)}
          accent="blue"
        />
        <KpiCard
          label="Clients Completed (7d)"
          value={fmtNum(funnel_7d.completed_7d)}
          accent="green"
        />
        <KpiCard
          label="Completion Rate"
          value={fmtPct(funnel_7d.completion_rate_pct)}
          accent="amber"
        />
        <KpiCard
          label="Total USD Paid (All-Time)"
          value={fmtUSD(funnel_7d.total_usd_paid_alltime)}
          accent="purple"
        />
      </div>

      {/* ============================================================ */}
      {/* Charts Row 1                                                  */}
      {/* ============================================================ */}
      <div className="grid grid-cols-1 xl:grid-cols-2 gap-4">
        {/* Daily Trend Line Chart */}
        <div className="bg-white dark:bg-gray-800 border border-gray-200 dark:border-gray-700 rounded-lg p-4 shadow-sm">
          <h3 className="text-sm font-semibold text-gray-700 dark:text-gray-300 mb-4">Daily Trend (Last 7 Days)</h3>
          <ResponsiveContainer width="100%" height={280}>
            <LineChart data={trendData} margin={{ top: 8, right: 16, left: 0, bottom: 0 }}>
              <XAxis
                dataKey="day"
                tickFormatter={fmtShortDate}
                tick={{ fontSize: 11, fill: '#94a3b8' }}
                axisLine={false}
                tickLine={false}
              />
              <YAxis
                yAxisId="count"
                tick={{ fontSize: 11, fill: '#94a3b8' }}
                axisLine={false}
                tickLine={false}
                width={40}
              />
              <YAxis
                yAxisId="usd"
                orientation="right"
                tick={{ fontSize: 11, fill: '#94a3b8' }}
                axisLine={false}
                tickLine={false}
                tickFormatter={(v: number) => `$${v}`}
                width={60}
              />
              <Tooltip
                contentStyle={{
                  backgroundColor: '#1f2937',
                  border: '1px solid #374151',
                  borderRadius: '8px',
                  fontSize: 12,
                }}
                labelFormatter={(label) => fmtDate(String(label))}
                // eslint-disable-next-line @typescript-eslint/no-explicit-any
                formatter={((value: any, name: any) => {
                  const v = Number(value ?? 0);
                  if (name === 'usd_paid') return [fmtUSD(v), 'USD Paid'];
                  if (name === 'started') return [fmtNum(v), 'Started'];
                  if (name === 'completed') return [fmtNum(v), 'Completed'];
                  return [v, name];
                }) as any}
              />
              <Legend
                iconType="circle"
                iconSize={8}
                formatter={(value: string) => {
                  const map: Record<string, string> = { started: 'Started', completed: 'Completed', usd_paid: 'USD Paid' };
                  return map[value] || value;
                }}
              />
              <Line yAxisId="count" type="monotone" dataKey="started" stroke="#3b82f6" strokeWidth={2} dot={{ r: 3 }} />
              <Line yAxisId="count" type="monotone" dataKey="completed" stroke="#22c55e" strokeWidth={2} dot={{ r: 3 }} />
              <Line yAxisId="usd" type="monotone" dataKey="usd_paid" stroke="#f59e0b" strokeWidth={2} dot={{ r: 3 }} strokeDasharray="5 5" />
            </LineChart>
          </ResponsiveContainer>
        </div>

        {/* Type Distribution Donut */}
        <div className="bg-white dark:bg-gray-800 border border-gray-200 dark:border-gray-700 rounded-lg p-4 shadow-sm">
          <h3 className="text-sm font-semibold text-gray-700 dark:text-gray-300 mb-4">Challenge Type Distribution</h3>
          {type_distribution.length === 0 ? (
            <p className="text-sm text-gray-400 dark:text-gray-500 text-center py-16">No active challenges</p>
          ) : (
            <ResponsiveContainer width="100%" height={280}>
              <PieChart>
                <Pie
                  data={type_distribution}
                  dataKey="count"
                  nameKey="type"
                  cx="50%"
                  cy="50%"
                  innerRadius={60}
                  outerRadius={100}
                  paddingAngle={2}
                  // eslint-disable-next-line @typescript-eslint/no-explicit-any
                  label={((props: any) => `${props.type ?? ''} (${props.count ?? 0})`) as any}
                  labelLine={{ stroke: '#94a3b8' }}
                >
                  {type_distribution.map((_, i) => (
                    <Cell key={i} fill={CHART_COLORS[i % CHART_COLORS.length]} />
                  ))}
                </Pie>
                <Tooltip formatter={((value: any) => [value ?? 0, 'Groups']) as any} />
              </PieChart>
            </ResponsiveContainer>
          )}
        </div>
      </div>

      {/* ============================================================ */}
      {/* Charts Row 2                                                  */}
      {/* ============================================================ */}
      <div className="grid grid-cols-1 xl:grid-cols-2 gap-4">
        {/* Payout by Group — horizontal bar */}
        <div className="bg-white dark:bg-gray-800 border border-gray-200 dark:border-gray-700 rounded-lg p-4 shadow-sm">
          <h3 className="text-sm font-semibold text-gray-700 dark:text-gray-300 mb-4">Total Payout by Challenge Group</h3>
          {payout_by_group.length === 0 ? (
            <p className="text-sm text-gray-400 dark:text-gray-500 text-center py-16">No payouts yet</p>
          ) : (
            <ResponsiveContainer width="100%" height={Math.max(200, payout_by_group.length * 36)}>
              <BarChart data={payout_by_group} layout="vertical" margin={{ top: 4, right: 16, left: 8, bottom: 4 }}>
                <XAxis
                  type="number"
                  tick={{ fontSize: 11, fill: '#94a3b8' }}
                  axisLine={false}
                  tickLine={false}
                  tickFormatter={(v: number) => `$${v}`}
                />
                <YAxis
                  type="category"
                  dataKey="group_name"
                  tick={{ fontSize: 11, fill: '#94a3b8' }}
                  axisLine={false}
                  tickLine={false}
                  width={120}
                />
                <Tooltip formatter={((value: any) => [fmtUSD(Number(value ?? 0)), 'Total Paid']) as any} />
                <Bar dataKey="total_paid" fill="#8b5cf6" radius={[0, 4, 4, 0]} maxBarSize={28} />
              </BarChart>
            </ResponsiveContainer>
          )}
        </div>

        {/* Optimove Event Health — stacked bar */}
        <div className="bg-white dark:bg-gray-800 border border-gray-200 dark:border-gray-700 rounded-lg p-4 shadow-sm">
          <h3 className="text-sm font-semibold text-gray-700 dark:text-gray-300 mb-4">Optimove Event Health</h3>
          {optimove_health.length === 0 ? (
            <p className="text-sm text-gray-400 dark:text-gray-500 text-center py-16">No events logged</p>
          ) : (
            <ResponsiveContainer width="100%" height={280}>
              <BarChart data={optimove_health} margin={{ top: 4, right: 16, left: 0, bottom: 4 }}>
                <XAxis
                  dataKey="event_name"
                  tick={{ fontSize: 10, fill: '#94a3b8' }}
                  axisLine={false}
                  tickLine={false}
                  interval={0}
                  angle={-20}
                  textAnchor="end"
                  height={60}
                />
                <YAxis
                  tick={{ fontSize: 11, fill: '#94a3b8' }}
                  axisLine={false}
                  tickLine={false}
                  width={40}
                />
                <Tooltip
                  // eslint-disable-next-line @typescript-eslint/no-explicit-any
                  formatter={((value: any, name: any) => [
                    fmtNum(Number(value ?? 0)),
                    name === 'success_count' ? 'Success' : 'Failure',
                  ]) as any}
                />
                <Legend
                  formatter={(v: string) => (v === 'success_count' ? 'Success' : 'Failure')}
                />
                <Bar dataKey="success_count" stackId="a" fill="#22c55e" radius={[0, 0, 0, 0]} />
                <Bar dataKey="failure_count" stackId="a" fill="#ef4444" radius={[4, 4, 0, 0]} />
              </BarChart>
            </ResponsiveContainer>
          )}
        </div>
      </div>

      {/* ============================================================ */}
      {/* Tables Section (tabbed)                                       */}
      {/* ============================================================ */}
      <div className="bg-white dark:bg-gray-800 border border-gray-200 dark:border-gray-700 rounded-lg shadow-sm">
        {/* Tab bar */}
        <div className="flex border-b border-gray-200 dark:border-gray-700 overflow-x-auto">
          {([
            ['per_challenge', 'Per-Challenge Breakdown'],
            ['top_earners', 'Top Earning Clients'],
            ['streaks', 'Streak Leaderboard'],
            ['diversity', 'Asset Class Diversity'],
          ] as [TableTab, string][]).map(([key, label]) => (
            <button
              key={key}
              onClick={() => setTableTab(key)}
              className={`px-4 py-3 text-xs font-medium whitespace-nowrap border-b-2 transition-colors ${
                tableTab === key
                  ? 'border-blue-600 text-blue-600 dark:border-blue-400 dark:text-blue-400'
                  : 'border-transparent text-gray-500 dark:text-gray-400 hover:text-gray-700 dark:hover:text-gray-300'
              }`}
            >
              {label}
            </button>
          ))}
        </div>

        <div className="p-4 overflow-x-auto">
          {/* Per-Challenge Breakdown */}
          {tableTab === 'per_challenge' && (
            <PerChallengeTable items={per_challenge} />
          )}

          {/* Top Earners */}
          {tableTab === 'top_earners' && (
            <TopEarnersTable items={top_earners} />
          )}

          {/* Streak Leaderboard */}
          {tableTab === 'streaks' && (
            <StreakTable items={streak_leaderboard} />
          )}

          {/* Diversity */}
          {tableTab === 'diversity' && (
            <DiversityTable items={diversity} />
          )}
        </div>
      </div>
    </div>
  );
}

// ---------------------------------------------------------------------------
// Table: Per-Challenge Breakdown
// ---------------------------------------------------------------------------

function PerChallengeTable({ items }: { items: PerChallengeItem[] }) {
  if (items.length === 0) return <EmptyTable />;
  return (
    <table className="w-full text-sm">
      <thead>
        <tr className="text-left text-xs text-gray-500 dark:text-gray-400 uppercase tracking-wider border-b border-gray-100 dark:border-gray-700">
          <th className="py-2 pr-3">Group Name</th>
          <th className="py-2 pr-3">Type</th>
          <th className="py-2 pr-3">Period</th>
          <th className="py-2 pr-3">Active</th>
          <th className="py-2 pr-3 text-right">Multiplier</th>
          <th className="py-2 pr-3 text-right">Started</th>
          <th className="py-2 pr-3 text-right">Completed</th>
          <th className="py-2 pr-3 text-right">Rate</th>
          <th className="py-2 pr-3 text-right">Payouts</th>
          <th className="py-2 text-right">USD Paid</th>
        </tr>
      </thead>
      <tbody className="text-gray-700 dark:text-gray-300">
        {items.map((r, i) => (
          <tr key={i} className="border-b border-gray-50 dark:border-gray-700/50 hover:bg-gray-50 dark:hover:bg-gray-700/30">
            <td className="py-2 pr-3 font-medium">{r.group_name}</td>
            <td className="py-2 pr-3">
              <span className="px-2 py-0.5 rounded-full text-xs bg-blue-100 dark:bg-blue-900/40 text-blue-700 dark:text-blue-300">
                {r.type}
              </span>
            </td>
            <td className="py-2 pr-3">{r.timeperiod}</td>
            <td className="py-2 pr-3">
              <span className={`inline-block w-2 h-2 rounded-full ${r.is_active ? 'bg-green-500' : 'bg-gray-400'}`} />
            </td>
            <td className="py-2 pr-3 text-right">{r.reward_multiplier}x</td>
            <td className="py-2 pr-3 text-right">{fmtNum(r.clients_started)}</td>
            <td className="py-2 pr-3 text-right">{fmtNum(r.clients_completed)}</td>
            <td className="py-2 pr-3 text-right">{fmtPct(r.completion_rate_pct)}</td>
            <td className="py-2 pr-3 text-right">{fmtNum(r.payout_count)}</td>
            <td className="py-2 text-right font-medium">{fmtUSD(r.total_usd_paid)}</td>
          </tr>
        ))}
      </tbody>
    </table>
  );
}

// ---------------------------------------------------------------------------
// Table: Top Earners
// ---------------------------------------------------------------------------

function TopEarnersTable({ items }: { items: TopEarnerItem[] }) {
  if (items.length === 0) return <EmptyTable />;
  return (
    <table className="w-full text-sm">
      <thead>
        <tr className="text-left text-xs text-gray-500 dark:text-gray-400 uppercase tracking-wider border-b border-gray-100 dark:border-gray-700">
          <th className="py-2 pr-3">#</th>
          <th className="py-2 pr-3">Client ID</th>
          <th className="py-2 pr-3 text-right">Total USD Earned</th>
          <th className="py-2 pr-3 text-right">Payouts</th>
          <th className="py-2 pr-3 text-right">Groups</th>
          <th className="py-2 pr-3">First Reward</th>
          <th className="py-2">Last Reward</th>
        </tr>
      </thead>
      <tbody className="text-gray-700 dark:text-gray-300">
        {items.map((r, i) => (
          <tr key={i} className="border-b border-gray-50 dark:border-gray-700/50 hover:bg-gray-50 dark:hover:bg-gray-700/30">
            <td className="py-2 pr-3 text-gray-400 dark:text-gray-500">{i + 1}</td>
            <td className="py-2 pr-3 font-mono font-medium">{r.client_id}</td>
            <td className="py-2 pr-3 text-right font-medium text-green-600 dark:text-green-400">{fmtUSD(r.total_usd_earned)}</td>
            <td className="py-2 pr-3 text-right">{fmtNum(r.total_payouts)}</td>
            <td className="py-2 pr-3 text-right">{fmtNum(r.groups_participated)}</td>
            <td className="py-2 pr-3">{fmtDate(r.first_reward)}</td>
            <td className="py-2">{fmtDate(r.last_reward)}</td>
          </tr>
        ))}
      </tbody>
    </table>
  );
}

// ---------------------------------------------------------------------------
// Table: Streak Leaderboard
// ---------------------------------------------------------------------------

function StreakTable({ items }: { items: StreakItem[] }) {
  if (items.length === 0) return <EmptyTable />;
  return (
    <table className="w-full text-sm">
      <thead>
        <tr className="text-left text-xs text-gray-500 dark:text-gray-400 uppercase tracking-wider border-b border-gray-100 dark:border-gray-700">
          <th className="py-2 pr-3">#</th>
          <th className="py-2 pr-3">Account ID</th>
          <th className="py-2 pr-3">Group</th>
          <th className="py-2 pr-3 text-right">Streak</th>
          <th className="py-2 pr-3">Last Trade</th>
          <th className="py-2 pr-3 text-right">Last Tier</th>
          <th className="py-2 text-right">Total Reward</th>
        </tr>
      </thead>
      <tbody className="text-gray-700 dark:text-gray-300">
        {items.map((r, i) => (
          <tr key={i} className="border-b border-gray-50 dark:border-gray-700/50 hover:bg-gray-50 dark:hover:bg-gray-700/30">
            <td className="py-2 pr-3 text-gray-400 dark:text-gray-500">{i + 1}</td>
            <td className="py-2 pr-3 font-mono font-medium">{r.accountid}</td>
            <td className="py-2 pr-3">{r.group_name}</td>
            <td className="py-2 pr-3 text-right">
              <span className="px-2 py-0.5 rounded-full text-xs bg-amber-100 dark:bg-amber-900/40 text-amber-700 dark:text-amber-300 font-bold">
                {r.current_streak}d
              </span>
            </td>
            <td className="py-2 pr-3">{fmtDate(r.last_trade_date)}</td>
            <td className="py-2 pr-3 text-right">{r.last_rewarded_tier}</td>
            <td className="py-2 text-right font-medium">{fmtUSD(r.total_streak_reward)}</td>
          </tr>
        ))}
      </tbody>
    </table>
  );
}

// ---------------------------------------------------------------------------
// Table: Diversity
// ---------------------------------------------------------------------------

function DiversityTable({ items }: { items: DiversityItem[] }) {
  if (items.length === 0) return <EmptyTable />;
  return (
    <table className="w-full text-sm">
      <thead>
        <tr className="text-left text-xs text-gray-500 dark:text-gray-400 uppercase tracking-wider border-b border-gray-100 dark:border-gray-700">
          <th className="py-2 pr-3">Group</th>
          <th className="py-2 pr-3">Asset Class</th>
          <th className="py-2 pr-3 text-right">Unique Clients</th>
          <th className="py-2">Week Start</th>
        </tr>
      </thead>
      <tbody className="text-gray-700 dark:text-gray-300">
        {items.map((r, i) => (
          <tr key={i} className="border-b border-gray-50 dark:border-gray-700/50 hover:bg-gray-50 dark:hover:bg-gray-700/30">
            <td className="py-2 pr-3 font-medium">{r.group_name}</td>
            <td className="py-2 pr-3">
              <span className="px-2 py-0.5 rounded-full text-xs bg-purple-100 dark:bg-purple-900/40 text-purple-700 dark:text-purple-300">
                {r.asset_class}
              </span>
            </td>
            <td className="py-2 pr-3 text-right">{fmtNum(r.unique_clients)}</td>
            <td className="py-2">{fmtDate(r.week_start)}</td>
          </tr>
        ))}
      </tbody>
    </table>
  );
}

// ---------------------------------------------------------------------------
// Empty table placeholder
// ---------------------------------------------------------------------------

function EmptyTable() {
  return (
    <div className="py-12 text-center text-sm text-gray-400 dark:text-gray-500">
      No data available
    </div>
  );
}
