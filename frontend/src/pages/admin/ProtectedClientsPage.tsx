import { useState } from 'react';
import { addProtectedClient, type AddProtectedResult } from '../../api/protectedClients';

const GROUPS = [32, 33, 34] as const;
type Group = (typeof GROUPS)[number];

const TABS = ['Add Protected Client'] as const;

export function ProtectedClientsPage() {
  const [activeTab, setActiveTab] = useState<(typeof TABS)[number]>(TABS[0]);
  const [accountid, setAccountid] = useState('');
  const [group, setGroup] = useState<Group>(32);
  const [loading, setLoading] = useState(false);
  const [result, setResult] = useState<AddProtectedResult | null>(null);
  const [error, setError] = useState('');

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
      if (res.status === 'success') setAccountid('');
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

  const statusIcons: Record<string, string> = {
    success: '✓',
    already_protected: '⚠',
    client_not_found: '✗',
  };

  const statusLabels: Record<string, string> = {
    success: 'Success',
    already_protected: 'Already Protected',
    client_not_found: 'Client Not Found',
  };

  return (
    <div className="max-w-2xl mx-auto space-y-6">
      {/* Tabs */}
      <div className="border-b border-gray-200 dark:border-gray-700">
        <nav className="-mb-px flex gap-4">
          {TABS.map((tab) => (
            <button
              key={tab}
              onClick={() => setActiveTab(tab)}
              className={`py-2 px-1 border-b-2 text-sm font-medium transition-colors ${
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

      {/* Tab Content */}
      {activeTab === 'Add Protected Client' && (
        <div className="bg-white dark:bg-gray-800 rounded-lg border border-gray-200 dark:border-gray-700 p-6 space-y-6">
          <div>
            <h2 className="text-base font-semibold text-gray-900 dark:text-gray-100">
              Add Client to Protected Group
            </h2>
            <p className="mt-1 text-sm text-gray-500 dark:text-gray-400">
              Assigns a client to a retention protection group in the system and sends an Optimove notification.
            </p>
          </div>

          <form onSubmit={handleSubmit} className="space-y-4">
            {/* Account ID */}
            <div>
              <label
                htmlFor="accountid"
                className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1"
              >
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

            {/* Group */}
            <div>
              <label
                htmlFor="group"
                className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1"
              >
                Protection Group
              </label>
              <select
                id="group"
                value={group}
                onChange={(e) => setGroup(Number(e.target.value) as Group)}
                disabled={loading}
                className="w-full px-3 py-2 rounded-md border border-gray-300 dark:border-gray-600 bg-white dark:bg-gray-700 text-gray-900 dark:text-gray-100 text-sm focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-transparent disabled:opacity-50"
              >
                {GROUPS.map((g) => (
                  <option key={g} value={g}>
                    Group {g}
                  </option>
                ))}
              </select>
            </div>

            {/* Error */}
            {error && (
              <p className="text-sm text-red-600 dark:text-red-400">{error}</p>
            )}

            {/* Submit */}
            <button
              type="submit"
              disabled={loading || !accountid.trim()}
              className="w-full py-2 px-4 rounded-md bg-blue-600 hover:bg-blue-700 disabled:bg-blue-400 dark:disabled:bg-blue-800 text-white text-sm font-medium transition-colors focus:outline-none focus:ring-2 focus:ring-blue-500 focus:ring-offset-2 dark:focus:ring-offset-gray-800"
            >
              {loading ? 'Processing…' : 'Add to Protected Group'}
            </button>
          </form>

          {/* Result */}
          {result && (
            <div className={`rounded-md border p-4 ${statusColors[result.status] ?? ''}`}>
              <div className="flex items-start gap-3">
                <span className="text-lg leading-none mt-0.5">
                  {statusIcons[result.status]}
                </span>
                <div className="space-y-1 min-w-0">
                  <p className="font-semibold text-sm">
                    {statusLabels[result.status] ?? result.status}
                    {result.action && (
                      <span className="ml-2 font-normal opacity-75">
                        — client {result.action}
                      </span>
                    )}
                  </p>
                  {result.message && (
                    <p className="text-sm opacity-90">{result.message}</p>
                  )}
                  {result.status === 'success' && (
                    <dl className="mt-2 grid grid-cols-2 gap-x-4 gap-y-1 text-xs opacity-80">
                      <dt className="font-medium">Account ID</dt>
                      <dd>{result.accountid}</dd>
                      <dt className="font-medium">Group</dt>
                      <dd>{result.group}</dd>
                      <dt className="font-medium">MT4 Login</dt>
                      <dd>{result.mt4login || '—'}</dd>
                      <dt className="font-medium">Trading Account ID</dt>
                      <dd>{result.trading_account_id || '—'}</dd>
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
    </div>
  );
}
