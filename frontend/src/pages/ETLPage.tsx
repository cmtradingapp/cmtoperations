import { useEffect, useRef, useState } from 'react';
import axios from 'axios';

const api = axios.create({ baseURL: import.meta.env.VITE_API_BASE_URL || '/api' });
api.interceptors.request.use((config) => {
  const auth = JSON.parse(localStorage.getItem('auth') || '{}');
  const token = auth?.state?.token;
  if (token) config.headers.Authorization = `Bearer ${token}`;
  return config;
});

interface SyncLog {
  id: number;
  sync_type: string;
  status: 'running' | 'completed' | 'error';
  started_at: string | null;
  completed_at: string | null;
  rows_synced: number | null;
  error_message: string | null;
}

function StatusBadge({ status }: { status: SyncLog['status'] }) {
  const styles: Record<string, string> = { running: 'bg-yellow-100 text-yellow-800 dark:bg-yellow-900 dark:text-yellow-200', completed: 'bg-green-100 text-green-800 dark:bg-green-900 dark:text-green-200', error: 'bg-red-100 text-red-800 dark:bg-red-900 dark:text-red-200' };
  const labels: Record<string, string> = { running: '⟳ Running', completed: '✓ Completed', error: '✗ Error' };
  return <span className={`px-2 py-0.5 rounded-full text-xs font-medium ${styles[status]}`}>{labels[status]}</span>;
}

function formatDate(iso: string | null) {
  if (!iso) return '—';
  return new Date(iso).toLocaleString();
}

function calcDuration(log: SyncLog) {
  if (!log.started_at || !log.completed_at) return '—';
  const s = Math.round((new Date(log.completed_at).getTime() - new Date(log.started_at).getTime()) / 1000);
  return s < 60 ? `${s}s` : `${Math.floor(s / 60)}m ${s % 60}s`;
}

function SyncSection({
  source,
  rowCount,
  lastRecord,
  description,
  syncEndpoint,
  logs,
  onSync,
}: {
  source: string;
  rowCount: number | null;
  lastRecord: { id: string; modified: string | null } | null;
  description: string;
  syncEndpoint: string;
  logs: SyncLog[];
  onSync: () => void;
}) {
  const [syncing, setSyncing] = useState(false);
  const [error, setError] = useState('');
  const [showHistory, setShowHistory] = useState(false);
  const hasRunning = logs.some((l) => l.status === 'running');
  const lastCompleted = logs.find((l) => l.status === 'completed');

  const run = async () => {
    setSyncing(true);
    setError('');
    try {
      await api.post(syncEndpoint);
      onSync();
    } catch (e: any) {
      setError(e.response?.data?.detail || 'Failed to start sync');
    } finally {
      setSyncing(false);
    }
  };

  return (
    <div className="space-y-3">
      {/* Stats */}
      <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-3">
        <div className="bg-white dark:bg-gray-800 rounded-lg shadow px-4 py-3">
          <p className="text-xs text-gray-500 dark:text-gray-400 mb-0.5">Local Rows</p>
          <p className="text-xl font-bold text-gray-800 dark:text-gray-100">{rowCount?.toLocaleString() ?? '—'}</p>
        </div>
        <div className="bg-white dark:bg-gray-800 rounded-lg shadow px-4 py-3">
          <p className="text-xs text-gray-500 dark:text-gray-400 mb-0.5">Last Sync Rows</p>
          <p className="text-xl font-bold text-gray-800 dark:text-gray-100">{lastCompleted?.rows_synced?.toLocaleString() ?? '—'}</p>
        </div>
        <div className="bg-white dark:bg-gray-800 rounded-lg shadow px-4 py-3">
          <p className="text-xs text-gray-500 dark:text-gray-400 mb-0.5">Last Synced</p>
          <p className="text-sm font-semibold text-gray-800 dark:text-gray-100">{formatDate(lastCompleted?.completed_at ?? null)}</p>
        </div>
        <div className="bg-white dark:bg-gray-800 rounded-lg shadow px-4 py-3">
          <p className="text-xs text-gray-500 dark:text-gray-400 mb-0.5">Last Record ID</p>
          <p className="text-sm font-bold text-gray-800 dark:text-gray-100 truncate">{lastRecord?.id ?? '—'}</p>
          <p className="text-xs text-gray-400 dark:text-gray-500 mt-0.5">{lastRecord?.modified ? formatDate(lastRecord.modified) : '—'}</p>
        </div>
      </div>

      {/* Full sync button */}
      <div className="bg-white dark:bg-gray-800 rounded-lg shadow p-4">
        <h3 className="text-sm font-semibold text-gray-700 dark:text-gray-300 mb-1">
          Full Sync — <code className="bg-gray-100 dark:bg-gray-700 dark:text-gray-300 px-1 rounded text-xs">{source}</code>
        </h3>
        <p className="text-xs text-gray-500 dark:text-gray-400 mb-3">{description}</p>
        {error && <p className="text-sm text-red-600 mb-2">{error}</p>}
        <button
          onClick={run}
          disabled={syncing || hasRunning}
          className="px-4 py-1.5 bg-blue-600 text-white rounded-md text-sm font-medium hover:bg-blue-700 disabled:opacity-50 transition-colors"
        >
          {syncing ? 'Starting…' : hasRunning ? 'Sync Running…' : 'Run Full Sync'}
        </button>
      </div>

      {/* Collapsible history */}
      <div className="bg-white dark:bg-gray-800 rounded-lg shadow overflow-hidden">
        <button
          onClick={() => setShowHistory((v) => !v)}
          className="w-full px-4 py-3 border-b border-gray-200 dark:border-gray-700 bg-gray-50 dark:bg-gray-750 flex items-center justify-between hover:bg-gray-100 dark:hover:bg-gray-700 transition-colors"
        >
          <span className="text-sm font-medium text-gray-700 dark:text-gray-300">
            Sync History
            {logs.length > 0 && <span className="ml-2 text-xs text-gray-400 dark:text-gray-500">({logs.length})</span>}
          </span>
          <span className="text-gray-400 dark:text-gray-500 text-xs">{showHistory ? '▲ Hide' : '▼ Show'}</span>
        </button>
        {showHistory && (
          <div className="overflow-x-auto">
            <table className="w-full">
              <thead className="bg-gray-50 dark:bg-gray-700">
                <tr>
                  {['Type', 'Status', 'Started', 'Completed', 'Duration', 'Rows', 'Error'].map((h) => (
                    <th key={h} className="px-4 py-2 text-left text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider">{h}</th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {logs.length === 0 ? (
                  <tr><td colSpan={7} className="px-4 py-8 text-center text-sm text-gray-400 dark:text-gray-500">No sync history yet.</td></tr>
                ) : (
                  logs.map((log) => (
                    <tr key={log.id} className="border-t border-gray-100 dark:border-gray-700 hover:bg-gray-50 dark:hover:bg-gray-700">
                      <td className="px-4 py-2 text-sm font-medium text-gray-800 dark:text-gray-200 capitalize">{log.sync_type.replace(/_/g, ' ')}</td>
                      <td className="px-4 py-2"><StatusBadge status={log.status} /></td>
                      <td className="px-4 py-2 text-sm text-gray-600 dark:text-gray-400 whitespace-nowrap">{formatDate(log.started_at)}</td>
                      <td className="px-4 py-2 text-sm text-gray-600 dark:text-gray-400 whitespace-nowrap">{formatDate(log.completed_at)}</td>
                      <td className="px-4 py-2 text-sm text-gray-600 dark:text-gray-400">{calcDuration(log)}</td>
                      <td className="px-4 py-2 text-sm text-gray-700 dark:text-gray-300">{log.rows_synced?.toLocaleString() ?? '—'}</td>
                      <td className="px-4 py-2 text-xs text-red-600 max-w-xs truncate">{log.error_message ?? '—'}</td>
                    </tr>
                  ))
                )}
              </tbody>
            </table>
          </div>
        )}
      </div>
    </div>
  );
}

interface LastRecord {
  id: string;
  modified: string | null;
}

interface StatusData {
  trades_row_count: number;
  open_pnl_row_count: number;
  ant_acc_row_count: number;
  vta_row_count: number;
  mtt_row_count: number;
  dealio_users_row_count: number;
  vtiger_users_row_count: number;
  vtiger_campaigns_row_count: number;
  extensions_row_count: number;
  proline_row_count: number;
  agent_targets_row_count: number;
  trades_last: LastRecord | null;
  open_pnl_last: LastRecord | null;
  ant_acc_last: LastRecord | null;
  vta_last: LastRecord | null;
  mtt_last: LastRecord | null;
  dealio_users_last: LastRecord | null;
  vtiger_users_last: LastRecord | null;
  vtiger_campaigns_last: LastRecord | null;
  extensions_last: LastRecord | null;
  proline_last: LastRecord | null;
  agent_targets_last: LastRecord | null;
  logs: SyncLog[];
}

export function ETLPage() {
  const [data, setData] = useState<StatusData | null>(null);
  const intervalRef = useRef<ReturnType<typeof setInterval> | null>(null);

  const fetchStatus = async () => {
    try {
      const res = await api.get('/etl/sync-status');
      setData(res.data);
    } catch { /* ignore */ }
  };

  // Start polling and pause when tab is hidden to reduce server load (CLAUD-37)
  // Interval raised from 10s to 30s — ETL status changes infrequently and
  // 70 users hitting this every 10s generates 7 req/s just for status checks.
  useEffect(() => {
    fetchStatus();
    intervalRef.current = setInterval(fetchStatus, 30_000);

    const handleVisibility = () => {
      if (document.hidden) {
        if (intervalRef.current) { clearInterval(intervalRef.current); intervalRef.current = null; }
      } else {
        fetchStatus();
        if (!intervalRef.current) intervalRef.current = setInterval(fetchStatus, 30_000);
      }
    };
    document.addEventListener('visibilitychange', handleVisibility);

    return () => {
      if (intervalRef.current) clearInterval(intervalRef.current);
      document.removeEventListener('visibilitychange', handleVisibility);
    };
  }, []);

  const logs = data?.logs ?? [];
  const filter = (prefix: string) => logs.filter((l) =>
    l.sync_type.startsWith(prefix) || (prefix === 'trades' && (l.sync_type === 'full' || l.sync_type === 'incremental'))
  );

  const sections = [
    { key: 'trades', label: 'Trades', source: 'dealio.trades_mt4', endpoint: '/etl/sync-trades', count: data?.trades_row_count ?? null, last: data?.trades_last ?? null, desc: 'Full refresh from dealio replica. Incremental sync runs every 30 min using last_modified (3h lookback).' },
    { key: 'open_pnl', label: 'Trades - Open', source: 'dealio.positions', endpoint: '/etl/sync-open-pnl', count: data?.open_pnl_row_count ?? null, last: data?.open_pnl_last ?? null, desc: 'Aggregated open positions (login → PNL) synced from dealio.positions every 3 minutes via full truncate+reload. Closed trades are removed automatically. Used by Retention Manager instead of live replica queries.' },
    { key: 'dealio_users', label: 'Dealio Users', source: 'dealio.users', endpoint: '/etl/sync-dealio-users', count: data?.dealio_users_row_count ?? null, last: data?.dealio_users_last ?? null, desc: 'Full refresh from dealio replica. Incremental sync runs every 30 min using lastupdate (3h lookback). Includes equity field.' },
    { key: 'ant_acc', label: 'Accounts', source: 'report.ant_acc', endpoint: '/etl/sync-ant-acc', count: data?.ant_acc_row_count ?? null, last: data?.ant_acc_last ?? null, desc: 'Full refresh from MSSQL. Incremental sync runs every 30 min using modifiedtime (3h lookback).' },
    { key: 'vta', label: 'Trading Accounts', source: 'report.vtiger_trading_accounts', endpoint: '/etl/sync-vta', count: data?.vta_row_count ?? null, last: data?.vta_last ?? null, desc: 'Full refresh from MSSQL. Incremental sync runs every 30 min using last_update (3h lookback).' },
    { key: 'mtt', label: 'MT Transactions', source: 'report.vtiger_mttransactions', endpoint: '/etl/sync-mtt', count: data?.mtt_row_count ?? null, last: data?.mtt_last ?? null, desc: 'Full refresh from MSSQL. Incremental sync runs every 30 min using modifiedtime (3h lookback).' },
    { key: 'vtiger_users', label: 'Vtiger Users', source: 'report.vtiger_users', endpoint: '/etl/sync-vtiger-users', count: data?.vtiger_users_row_count ?? null, last: data?.vtiger_users_last ?? null, desc: 'Full reload from MSSQL every hour (truncate + reload). No incremental.' },
    { key: 'vtiger_campaigns', label: 'Vtiger Campaigns', source: 'report.vtiger_campaigns', endpoint: '/etl/sync-vtiger-campaigns', count: data?.vtiger_campaigns_row_count ?? null, last: data?.vtiger_campaigns_last ?? null, desc: 'Full reload from MSSQL every hour (truncate + reload). No incremental.' },
    { key: 'extensions', label: 'Extensions', source: 'report.Extension_new', endpoint: '/etl/sync-extensions', count: data?.extensions_row_count ?? null, last: data?.extensions_last ?? null, desc: 'Full reload from MSSQL every hour (truncate + reload). No incremental.' },
    { key: 'proline', label: 'Proline Affiliate Data', source: 'partners.cmtrading.com (API)', endpoint: '/etl/sync-proline', count: data?.proline_row_count ?? null, last: data?.proline_last ?? null, desc: 'Pulls last 31 days of qualified customer data from the Proline affiliate API. Stores account_id, affiliate_id, performance_commission, qualified_ftd_date. Runs daily at 02:00 UTC. Requires "Proline" entry in Integrations & Config.' },
    { key: 'agent_targets', label: 'Agent Targets', source: 'report.target + report.vtiger_user', endpoint: '/etl/sync-agent-targets', count: data?.agent_targets_row_count ?? null, last: data?.agent_targets_last ?? null, desc: 'Manual sync of agent monthly net targets from MSSQL report.target joined with report.vtiger_user for email. Also runs daily at 01:30 UTC and on startup.' },
  ];

  return (
    <div className="space-y-8">
      {sections.map((s) => (
        <div key={s.key}>
          <h2 className="text-sm font-semibold text-gray-600 dark:text-gray-400 uppercase tracking-wider mb-3">{s.label} — {s.source}</h2>
          <SyncSection
            source={s.source}
            rowCount={s.count}
            lastRecord={s.last}
            description={s.desc}
            syncEndpoint={s.endpoint}
            logs={filter(s.key)}
            onSync={fetchStatus}
          />
        </div>
      ))}
      <p className="text-xs text-gray-400 dark:text-gray-500 text-right">Auto-refreshes every 10s</p>
    </div>
  );
}
