import { useEffect, useState } from 'react';
import axios from 'axios';

const api = axios.create({ baseURL: import.meta.env.VITE_API_BASE_URL || '/api' });
api.interceptors.request.use((config) => {
  const auth = JSON.parse(localStorage.getItem('auth') || '{}');
  const token = auth?.state?.token;
  if (token) config.headers.Authorization = `Bearer ${token}`;
  return config;
});

interface AgentRow {
  agent_id: string;
  agent_name: string;
  total_calls: number;
  total_duration_mins: number;
  success_count: number;
  success_duration_mins: number;
  failure_count: number;
  failure_duration_mins: number;
  unknown_count: number;
  unknown_duration_mins: number;
}

const PERIODS = [
  { label: 'Last 7 days', value: 7 },
  { label: 'Last 30 days', value: 30 },
  { label: 'Last 90 days', value: 90 },
  { label: 'All time', value: 0 },
];

function fmt(n: number) {
  return n.toLocaleString(undefined, { minimumFractionDigits: 0, maximumFractionDigits: 1 });
}

export function AiCallDashboardPage() {
  const [agents, setAgents] = useState<AgentRow[]>([]);
  const [totalCalls, setTotalCalls] = useState(0);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState('');
  const [days, setDays] = useState(30);

  const load = async (d: number) => {
    setLoading(true);
    setError('');
    try {
      const res = await api.get('/calls/dashboard', { params: { days: d } });
      setAgents(res.data.agents);
      setTotalCalls(res.data.total_calls);
    } catch (err: any) {
      setError(err.response?.data?.detail || 'Failed to load dashboard');
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => { load(days); }, [days]);

  const th = 'px-4 py-3 text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider whitespace-nowrap';
  const thL = th + ' text-left';
  const thR = th + ' text-right';

  const totals = {
    success_count: agents.reduce((s, a) => s + a.success_count, 0),
    success_duration_mins: agents.reduce((s, a) => s + a.success_duration_mins, 0),
    failure_count: agents.reduce((s, a) => s + a.failure_count, 0),
    failure_duration_mins: agents.reduce((s, a) => s + a.failure_duration_mins, 0),
    unknown_count: agents.reduce((s, a) => s + a.unknown_count, 0),
    unknown_duration_mins: agents.reduce((s, a) => s + a.unknown_duration_mins, 0),
    total_calls: agents.reduce((s, a) => s + a.total_calls, 0),
    total_duration_mins: agents.reduce((s, a) => s + a.total_duration_mins, 0),
  };

  return (
    <div className="space-y-4">
      {/* Period selector */}
      <div className="bg-white dark:bg-gray-800 rounded-lg shadow dark:shadow-gray-900 px-4 py-3 flex items-center gap-3 flex-wrap">
        <span className="text-sm font-medium text-gray-700 dark:text-gray-300">Period:</span>
        {PERIODS.map((opt) => (
          <button
            key={opt.value}
            onClick={() => setDays(opt.value)}
            className={`px-3 py-1.5 rounded-md text-sm font-medium transition-colors ${
              days === opt.value
                ? 'bg-blue-600 text-white'
                : 'border border-gray-300 dark:border-gray-600 text-gray-600 dark:text-gray-400 hover:bg-gray-50 dark:hover:bg-gray-700'
            }`}
          >
            {opt.label}
          </button>
        ))}
        {!loading && (
          <span className="ml-auto text-sm text-gray-500 dark:text-gray-400">
            {totalCalls.toLocaleString()} total calls fetched
          </span>
        )}
      </div>

      {/* Table */}
      <div className="bg-white dark:bg-gray-800 rounded-lg shadow dark:shadow-gray-900 overflow-hidden">
        {error && <div className="px-4 py-3 bg-red-50 border-b border-red-100 text-sm text-red-600">{error}</div>}

        <div className="overflow-x-auto">
          <table className="w-full">
            <thead className="bg-gray-50 dark:bg-gray-900">
              <tr>
                <th className={thL} rowSpan={2}>Agent</th>
                <th className={thR + ' text-green-600'} colSpan={2}>Success</th>
                <th className={thR + ' text-red-600'} colSpan={2}>Failure</th>
                <th className={thR + ' text-gray-500 dark:text-gray-400'} colSpan={2}>Unknown</th>
                <th className={thR} colSpan={2}>Total</th>
              </tr>
              <tr className="border-t border-gray-200 dark:border-gray-700">
                <th className={thR + ' text-green-600'}>Calls</th>
                <th className={thR + ' text-green-600'}>Mins</th>
                <th className={thR + ' text-red-600'}>Calls</th>
                <th className={thR + ' text-red-600'}>Mins</th>
                <th className={thR + ' text-gray-500 dark:text-gray-400'}>Calls</th>
                <th className={thR + ' text-gray-500 dark:text-gray-400'}>Mins</th>
                <th className={thR}>Calls</th>
                <th className={thR}>Mins</th>
              </tr>
            </thead>
            <tbody>
              {loading ? (
                <tr><td colSpan={9} className="px-4 py-12 text-center text-sm text-gray-400 dark:text-gray-400">Loading…</td></tr>
              ) : agents.length === 0 ? (
                <tr><td colSpan={9} className="px-4 py-12 text-center text-sm text-gray-400 dark:text-gray-400">No data found.</td></tr>
              ) : (
                <>
                  {agents.map((a) => (
                    <tr key={a.agent_id} className="border-t border-gray-100 dark:border-gray-700 hover:bg-gray-50 dark:hover:bg-gray-700">
                      <td className="px-4 py-3 text-sm font-medium text-gray-900 dark:text-gray-100">{a.agent_name}</td>
                      <td className="px-4 py-3 text-sm text-right font-medium text-green-600">{fmt(a.success_count)}</td>
                      <td className="px-4 py-3 text-sm text-right text-green-600">{fmt(a.success_duration_mins)}</td>
                      <td className="px-4 py-3 text-sm text-right font-medium text-red-600">{fmt(a.failure_count)}</td>
                      <td className="px-4 py-3 text-sm text-right text-red-600">{fmt(a.failure_duration_mins)}</td>
                      <td className="px-4 py-3 text-sm text-right font-medium text-gray-500 dark:text-gray-400">{fmt(a.unknown_count)}</td>
                      <td className="px-4 py-3 text-sm text-right text-gray-500 dark:text-gray-400">{fmt(a.unknown_duration_mins)}</td>
                      <td className="px-4 py-3 text-sm text-right font-semibold text-gray-900 dark:text-gray-100">{fmt(a.total_calls)}</td>
                      <td className="px-4 py-3 text-sm text-right font-semibold text-gray-900 dark:text-gray-100">{fmt(a.total_duration_mins)}</td>
                    </tr>
                  ))}
                  <tr className="border-t-2 border-gray-300 dark:border-gray-600 bg-gray-50 dark:bg-gray-900">
                    <td className="px-4 py-3 text-sm font-bold text-gray-700 dark:text-gray-300">Total</td>
                    <td className="px-4 py-3 text-sm text-right font-bold text-green-700">{fmt(totals.success_count)}</td>
                    <td className="px-4 py-3 text-sm text-right font-bold text-green-700">{fmt(totals.success_duration_mins)}</td>
                    <td className="px-4 py-3 text-sm text-right font-bold text-red-700">{fmt(totals.failure_count)}</td>
                    <td className="px-4 py-3 text-sm text-right font-bold text-red-700">{fmt(totals.failure_duration_mins)}</td>
                    <td className="px-4 py-3 text-sm text-right font-bold text-gray-600 dark:text-gray-400">{fmt(totals.unknown_count)}</td>
                    <td className="px-4 py-3 text-sm text-right font-bold text-gray-600 dark:text-gray-400">{fmt(totals.unknown_duration_mins)}</td>
                    <td className="px-4 py-3 text-sm text-right font-bold text-gray-900 dark:text-gray-100">{fmt(totals.total_calls)}</td>
                    <td className="px-4 py-3 text-sm text-right font-bold text-gray-900 dark:text-gray-100">{fmt(totals.total_duration_mins)}</td>
                  </tr>
                </>
              )}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  );
}
