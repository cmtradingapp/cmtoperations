import { useEffect, useState } from 'react';
import {
  addProtectedClient,
  fetchProtectedClients,
  fetchProtectionGroups,
  reactivateAll,
  type AddProtectedResult,
} from '../../api/protectedClients';

const GROUPS = [32, 33, 34] as const;
type Group = (typeof GROUPS)[number];

const TABS = ['Add Protected Client', 'Clients in Protected', 'Protection Groups'] as const;
type Tab = (typeof TABS)[number];

// ── Generic read-only table ────────────────────────────────────────────────

function DataTable({ rows, loading, error }: {
  rows: Record<string, unknown>[];
  loading: boolean;
  error: string;
}) {
  if (loading) return <p className="text-sm text-gray-500 dark:text-gray-400 py-6 text-center">Loading…</p>;
  if (error) return <p className="text-sm text-red-500 py-6 text-center">{error}</p>;
  if (!rows.length) return <p className="text-sm text-gray-500 dark:text-gray-400 py-6 text-center">No records found.</p>;

  const cols = Object.keys(rows[0]);

  return (
    <div className="overflow-x-auto rounded-lg border border-gray-200 dark:border-gray-700">
      <table className="min-w-full text-xs">
        <thead className="bg-gray-50 dark:bg-gray-800 border-b border-gray-200 dark:border-gray-700">
          <tr>
            {cols.map((col) => (
              <th
                key={col}
                className="px-3 py-2 text-left font-semibold text-gray-600 dark:text-gray-300 whitespace-nowrap"
              >
                {col}
              </th>
            ))}
          </tr>
        </thead>
        <tbody className="divide-y divide-gray-100 dark:divide-gray-800">
          {rows.map((row, i) => (
            <tr key={i} className="bg-white dark:bg-gray-900 hover:bg-gray-50 dark:hover:bg-gray-800 transition-colors">
              {cols.map((col) => (
                <td key={col} className="px-3 py-2 text-gray-700 dark:text-gray-300 whitespace-nowrap">
                  {row[col] === null || row[col] === undefined ? (
                    <span className="text-gray-400">—</span>
                  ) : String(row[col])}
                </td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

// ── Page ──────────────────────────────────────────────────────────────────

export function ProtectedClientsPage() {
  const [activeTab, setActiveTab] = useState<Tab>(TABS[0]);

  // Add form state
  const [accountid, setAccountid] = useState('');
  const [group, setGroup] = useState<Group>(32);
  const [loading, setLoading] = useState(false);
  const [result, setResult] = useState<AddProtectedResult | null>(null);
  const [error, setError] = useState('');

  // Clients in Protected
  const [clientRows, setClientRows] = useState<Record<string, unknown>[]>([]);
  const [clientsLoading, setClientsLoading] = useState(false);
  const [clientsError, setClientsError] = useState('');
  const [activeFilter, setActiveFilter] = useState<'all' | 'active' | 'inactive'>('all');
  const [reactivating, setReactivating] = useState(false);
  const [confirmReactivate, setConfirmReactivate] = useState(false);

  // Protection Groups
  const [groupRows, setGroupRows] = useState<Record<string, unknown>[]>([]);
  const [groupsLoading, setGroupsLoading] = useState(false);
  const [groupsError, setGroupsError] = useState('');

  const loadClients = (filter: 'all' | 'active' | 'inactive') => {
    setClientsLoading(true);
    setClientsError('');
    setClientRows([]);
    const param = filter === 'active' ? 1 : filter === 'inactive' ? 0 : undefined;
    fetchProtectedClients(param)
      .then(setClientRows)
      .catch((e) => setClientsError(e.message))
      .finally(() => setClientsLoading(false));
  };

  useEffect(() => {
    if (activeTab === 'Clients in Protected' && !clientRows.length && !clientsLoading) {
      loadClients(activeFilter);
    }
    if (activeTab === 'Protection Groups' && !groupRows.length && !groupsLoading) {
      setGroupsLoading(true);
      setGroupsError('');
      fetchProtectionGroups()
        .then(setGroupRows)
        .catch((e) => setGroupsError(e.message))
        .finally(() => setGroupsLoading(false));
    }
  }, [activeTab]); // eslint-disable-line react-hooks/exhaustive-deps

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    const id = accountid.trim();
    if (!id) { setError('Account ID is required'); return; }
    setLoading(true);
    setResult(null);
    setError('');
    try {
      const res = await addProtectedClient(id, group);
      setResult(res);
      if (res.status === 'success') {
        setAccountid('');
        // Invalidate clients list so it refreshes next time
        setClientRows([]);
      }
    } catch (err: unknown) {
      setError(err instanceof Error ? err.message : 'An unexpected error occurred');
    } finally {
      setLoading(false);
    }
  };

  const statusColors: Record<string, string> = {
    success: 'bg-green-50 border-green-200 text-green-800 dark:bg-green-900/20 dark:border-green-700 dark:text-green-300',
    already_protected: 'bg-yellow-50 border-yellow-200 text-yellow-800 dark:bg-yellow-900/20 dark:border-yellow-700 dark:text-yellow-300',
    client_not_found: 'bg-red-50 border-red-200 text-red-800 dark:bg-red-900/20 dark:border-red-700 dark:text-red-300',
  };
  const statusIcons: Record<string, string> = { success: '✓', already_protected: '⚠', client_not_found: '✗' };
  const statusLabels: Record<string, string> = { success: 'Success', already_protected: 'Already Protected', client_not_found: 'Client Not Found' };

  return (
    <div className="space-y-6">
      {/* Tabs */}
      <div className="border-b border-gray-200 dark:border-gray-700">
        <nav className="-mb-px flex gap-4">
          {TABS.map((tab) => (
            <button
              key={tab}
              onClick={() => setActiveTab(tab)}
              className={`py-2 px-1 border-b-2 text-sm font-medium transition-colors whitespace-nowrap ${
                activeTab === tab
                  ? 'border-blue-500 text-blue-600 dark:text-blue-400'
                  : 'border-transparent text-gray-500 hover:text-gray-700 hover:border-gray-300 dark:text-gray-400 dark:hover:text-gray-200'
              }`}
            >
              {tab}
            </button>
          ))}
        </nav>
      </div>

      {/* ── Tab: Add Protected Client ── */}
      {activeTab === 'Add Protected Client' && (
        <div className="max-w-2xl bg-white dark:bg-gray-800 rounded-lg border border-gray-200 dark:border-gray-700 p-6 space-y-6">
          <div>
            <h2 className="text-base font-semibold text-gray-900 dark:text-gray-100">Add Client to Protected Group</h2>
            <p className="mt-1 text-sm text-gray-500 dark:text-gray-400">
              Assigns a client to a retention protection group and sends an Optimove notification.
            </p>
          </div>

          <form onSubmit={handleSubmit} className="space-y-4">
            <div>
              <label htmlFor="accountid" className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">
                Account ID
              </label>
              <input
                id="accountid"
                type="text"
                value={accountid}
                onChange={(e) => { setAccountid(e.target.value); setError(''); setResult(null); }}
                placeholder="e.g. 123456"
                disabled={loading}
                className="w-full px-3 py-2 rounded-md border border-gray-300 dark:border-gray-600 bg-white dark:bg-gray-700 text-gray-900 dark:text-gray-100 placeholder-gray-400 dark:placeholder-gray-500 text-sm focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-transparent disabled:opacity-50"
              />
            </div>

            <div>
              <label htmlFor="group" className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">
                Protection Group
              </label>
              <select
                id="group"
                value={group}
                onChange={(e) => setGroup(Number(e.target.value) as Group)}
                disabled={loading}
                className="w-full px-3 py-2 rounded-md border border-gray-300 dark:border-gray-600 bg-white dark:bg-gray-700 text-gray-900 dark:text-gray-100 text-sm focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-transparent disabled:opacity-50"
              >
                {GROUPS.map((g) => <option key={g} value={g}>Group {g}</option>)}
              </select>
            </div>

            {error && <p className="text-sm text-red-600 dark:text-red-400">{error}</p>}

            <button
              type="submit"
              disabled={loading || !accountid.trim()}
              className="w-full py-2 px-4 rounded-md bg-blue-600 hover:bg-blue-700 disabled:bg-blue-400 dark:disabled:bg-blue-800 text-white text-sm font-medium transition-colors focus:outline-none focus:ring-2 focus:ring-blue-500 focus:ring-offset-2 dark:focus:ring-offset-gray-800"
            >
              {loading ? 'Processing…' : 'Add to Protected Group'}
            </button>
          </form>

          {result && (
            <div className={`rounded-md border p-4 ${statusColors[result.status] ?? ''}`}>
              <div className="flex items-start gap-3">
                <span className="text-lg leading-none mt-0.5">{statusIcons[result.status]}</span>
                <div className="space-y-1 min-w-0">
                  <p className="font-semibold text-sm">
                    {statusLabels[result.status] ?? result.status}
                    {result.action && <span className="ml-2 font-normal opacity-75">— client {result.action}</span>}
                  </p>
                  {result.message && <p className="text-sm opacity-90">{result.message}</p>}
                  {result.status === 'success' && (
                    <dl className="mt-2 grid grid-cols-2 gap-x-4 gap-y-1 text-xs opacity-80">
                      <dt className="font-medium">Account ID</dt><dd>{result.accountid}</dd>
                      <dt className="font-medium">Group</dt><dd>{result.group}</dd>
                      <dt className="font-medium">MT4 Login</dt><dd>{result.mt4login || '—'}</dd>
                      <dt className="font-medium">Trading Account ID</dt><dd>{result.trading_account_id || '—'}</dd>
                    </dl>
                  )}
                  {result.status === 'already_protected' && result.current_group && (
                    <p className="text-xs opacity-80 mt-1">Current group: {result.current_group}</p>
                  )}
                </div>
              </div>
            </div>
          )}
        </div>
      )}

      {/* ── Tab: Clients in Protected ── */}
      {activeTab === 'Clients in Protected' && (
        <div className="space-y-3">
          <div className="flex items-center justify-between gap-3 flex-wrap">
            {/* Filter buttons */}
            <div className="flex gap-1">
              {(['all', 'active', 'inactive'] as const).map((f) => (
                <button
                  key={f}
                  onClick={() => { setActiveFilter(f); loadClients(f); }}
                  className={`px-3 py-1 rounded-md text-xs font-medium transition-colors ${
                    activeFilter === f
                      ? 'bg-blue-600 text-white'
                      : 'bg-gray-100 dark:bg-gray-700 text-gray-600 dark:text-gray-300 hover:bg-gray-200 dark:hover:bg-gray-600'
                  }`}
                >
                  {f.charAt(0).toUpperCase() + f.slice(1)}
                </button>
              ))}
            </div>
            <div className="flex items-center gap-3">
              {confirmReactivate ? (
                <div className="flex items-center gap-2">
                  <span className="text-xs text-gray-600 dark:text-gray-300">Reactivate all in groups 32/33/34?</span>
                  <button
                    disabled={reactivating}
                    onClick={async () => {
                      setReactivating(true);
                      try {
                        const r = await reactivateAll();
                        setConfirmReactivate(false);
                        loadClients(activeFilter);
                        alert(`Reactivated ${r.reactivated} client(s)`);
                      } catch (e: unknown) {
                        alert(e instanceof Error ? e.message : 'Error');
                      } finally {
                        setReactivating(false);
                      }
                    }}
                    className="px-2 py-1 rounded text-xs bg-green-600 hover:bg-green-700 text-white disabled:opacity-50"
                  >
                    {reactivating ? 'Working…' : 'Yes, confirm'}
                  </button>
                  <button onClick={() => setConfirmReactivate(false)} className="text-xs text-gray-500 hover:underline">Cancel</button>
                </div>
              ) : (
                <button
                  onClick={() => setConfirmReactivate(true)}
                  className="px-3 py-1 rounded-md text-xs font-medium bg-green-600 hover:bg-green-700 text-white transition-colors"
                >
                  Reactivate All
                </button>
              )}
              <button
                onClick={() => loadClients(activeFilter)}
                className="text-xs text-blue-600 dark:text-blue-400 hover:underline"
              >
                Refresh
              </button>
            </div>
          </div>
          <DataTable rows={clientRows} loading={clientsLoading} error={clientsError} />
        </div>
      )}

      {/* ── Tab: Protection Groups ── */}
      {activeTab === 'Protection Groups' && (
        <div className="space-y-3">
          <div className="flex items-center justify-between">
            <p className="text-sm text-gray-500 dark:text-gray-400">
              All retention protection groups
            </p>
            <button
              onClick={() => {
                setGroupRows([]);
                setGroupsLoading(true);
                setGroupsError('');
                fetchProtectionGroups()
                  .then(setGroupRows)
                  .catch((e) => setGroupsError(e.message))
                  .finally(() => setGroupsLoading(false));
              }}
              className="text-xs text-blue-600 dark:text-blue-400 hover:underline"
            >
              Refresh
            </button>
          </div>
          <DataTable rows={groupRows} loading={groupsLoading} error={groupsError} />
        </div>
      )}
    </div>
  );
}
