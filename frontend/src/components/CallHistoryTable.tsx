import { useEffect, useState } from 'react';
import axios from 'axios';

import { getCallHistory } from '../api/client';
import type { ElevenLabsConversation } from '../types';

const api = axios.create({ baseURL: import.meta.env.VITE_API_BASE_URL || '/api' });
api.interceptors.request.use((config) => {
  const auth = JSON.parse(localStorage.getItem('auth') || '{}');
  const token = auth?.state?.token;
  if (token) config.headers.Authorization = `Bearer ${token}`;
  return config;
});

const STATUS_OPTIONS = [
  { value: '', label: 'All' },
  { value: 'success', label: 'Success' },
  { value: 'failure', label: 'Failure' },
  { value: 'unknown', label: 'Unknown' },
];

function CallSuccessBadge({ value }: { value?: string }) {
  if (!value) return <span className="text-gray-400 text-xs">—</span>;
  const styles: Record<string, string> = {
    success: 'bg-green-100 text-green-800',
    failure: 'bg-red-100 text-red-800',
    unknown: 'bg-gray-100 text-gray-600',
  };
  const labels: Record<string, string> = {
    success: 'Success',
    failure: 'Failure',
    unknown: 'Unknown',
  };
  return (
    <span className={`px-2 py-0.5 rounded-full text-xs font-medium ${styles[value] ?? styles.unknown}`}>
      {labels[value] ?? value}
    </span>
  );
}

export function CallHistoryTable() {
  const [allConversations, setAllConversations] = useState<ElevenLabsConversation[]>([]);
  const [accountMap, setAccountMap] = useState<Record<string, string>>({});
  const [nextCursor, setNextCursor] = useState<string | undefined>(undefined);
  const [loading, setLoading] = useState(false);
  const [loadingMore, setLoadingMore] = useState(false);
  const [exporting, setExporting] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [agentId, setAgentId] = useState('');
  const [callSuccessful, setCallSuccessful] = useState('');

  const fetchMappings = async (conversations: ElevenLabsConversation[]) => {
    const ids = conversations.map((c) => c.conversation_id).filter(Boolean);
    if (ids.length === 0) return;
    try {
      const res = await api.post('/call-mappings/lookup', { conversation_ids: ids });
      setAccountMap((prev) => ({ ...prev, ...res.data.mappings }));
    } catch {
      // non-critical — account IDs just won't show
    }
  };

  const load = async () => {
    setLoading(true);
    setError(null);
    setAllConversations([]);
    setAccountMap({});
    setNextCursor(undefined);
    try {
      const data = await getCallHistory({
        agent_id: agentId || undefined,
        page_size: 100,
      });
      const convs = data.conversations ?? [];
      setAllConversations(convs);
      setNextCursor(data.next_cursor);
      await fetchMappings(convs);
    } catch {
      setError('Failed to load call history from ElevenLabs');
    } finally {
      setLoading(false);
    }
  };

  const loadMore = async () => {
    if (!nextCursor) return;
    setLoadingMore(true);
    try {
      const data = await getCallHistory({
        agent_id: agentId || undefined,
        page_size: 100,
        cursor: nextCursor,
      });
      const convs = data.conversations ?? [];
      setAllConversations((prev) => [...prev, ...convs]);
      setNextCursor(data.next_cursor);
      await fetchMappings(convs);
    } catch {
      setError('Failed to load more results');
    } finally {
      setLoadingMore(false);
    }
  };

  const exportUnknown = async () => {
    setExporting(true);
    try {
      const params = new URLSearchParams();
      if (agentId) params.set('agent_id', agentId);
      const res = await api.get(`/call-mappings/export-unknown?${params}`, { responseType: 'blob' });
      const url = URL.createObjectURL(new Blob([res.data], { type: 'text/csv' }));
      const a = document.createElement('a');
      a.href = url;
      a.download = agentId ? `unknown_calls_${agentId}.csv` : 'unknown_calls.csv';
      a.click();
      URL.revokeObjectURL(url);
    } catch {
      setError('Failed to export');
    } finally {
      setExporting(false);
    }
  };

  const conversations = callSuccessful
    ? allConversations.filter((c) => c.call_successful === callSuccessful)
    : allConversations;

  useEffect(() => {
    load();
  }, []);

  const formatDuration = (secs?: number) => {
    if (secs == null) return '—';
    const m = Math.floor(secs / 60);
    const s = secs % 60;
    return m > 0 ? `${m}m ${s}s` : `${s}s`;
  };

  const formatDate = (unixSec?: number) => {
    if (!unixSec) return '—';
    return new Date(unixSec * 1000).toLocaleString();
  };

  return (
    <div className="space-y-4">
      {/* Filters */}
      <div className="bg-white dark:bg-gray-800 rounded-lg shadow p-4 flex flex-wrap gap-3 items-end">
        <div className="flex-1 min-w-48">
          <label className="block text-xs font-medium text-gray-600 dark:text-gray-400 mb-1">Agent ID</label>
          <input
            type="text"
            placeholder="e.g. agent_0101khtww71ve…"
            value={agentId}
            onChange={(e) => setAgentId(e.target.value)}
            className="w-full border border-gray-300 dark:border-gray-600 rounded-md px-3 py-1.5 text-sm bg-white dark:bg-gray-700 text-gray-900 dark:text-gray-100 placeholder-gray-400 dark:placeholder-gray-500 focus:outline-none focus:ring-2 focus:ring-blue-500"
          />
        </div>
        <div>
          <label className="block text-xs font-medium text-gray-600 dark:text-gray-400 mb-1">Call Result</label>
          <select
            value={callSuccessful}
            onChange={(e) => setCallSuccessful(e.target.value)}
            className="border border-gray-300 dark:border-gray-600 rounded-md px-3 py-1.5 text-sm bg-white dark:bg-gray-700 text-gray-900 dark:text-gray-100 focus:outline-none focus:ring-2 focus:ring-blue-500"
          >
            {STATUS_OPTIONS.map((o) => (
              <option key={o.value} value={o.value}>{o.label}</option>
            ))}
          </select>
        </div>
        <button
          onClick={load}
          disabled={loading}
          className="px-4 py-1.5 bg-blue-600 text-white rounded-md text-sm font-medium hover:bg-blue-700 disabled:opacity-50 transition-colors"
        >
          {loading ? 'Loading…' : 'Search'}
        </button>
      </div>

      {/* Table */}
      <div className="bg-white dark:bg-gray-800 rounded-lg shadow overflow-hidden">
        {error ? (
          <div className="p-6 text-red-600 dark:text-red-400 text-sm">{error}</div>
        ) : loading ? (
          <div className="p-12 text-center text-gray-400 dark:text-gray-500 text-sm">Loading…</div>
        ) : conversations.length === 0 ? (
          <div className="p-12 text-center text-gray-400 dark:text-gray-500 text-sm">No conversations found.</div>
        ) : (
          <>
            <div className="px-4 py-3 border-b border-gray-200 dark:border-gray-700 bg-gray-50 dark:bg-gray-700/50 flex items-center justify-between">
              <span className="text-sm text-gray-600 dark:text-gray-300">
                {conversations.length} conversation{conversations.length !== 1 ? 's' : ''}
                {nextCursor && <span className="text-gray-400 dark:text-gray-500 ml-1">(more available)</span>}
              </span>
              {conversations.some((c) => c.call_successful === 'unknown' || !c.call_successful) && (
                <button
                  onClick={exportUnknown}
                  disabled={exporting}
                  className="px-3 py-1.5 bg-yellow-500 text-white rounded-md text-xs font-medium hover:bg-yellow-600 disabled:opacity-50 transition-colors"
                >
                  {exporting ? 'Exporting…' : `Export Unknown as CSV`}
                </button>
              )}
            </div>
            <div className="overflow-x-auto">
              <table className="w-full">
                <thead className="bg-gray-50 dark:bg-gray-700/50">
                  <tr>
                    {['Account ID', 'Date', 'Conversation ID', 'Agent Name', 'Duration', 'Result'].map((h) => (
                      <th
                        key={h}
                        className="px-4 py-3 text-left text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider"
                      >
                        {h}
                      </th>
                    ))}
                  </tr>
                </thead>
                <tbody>
                  {conversations.map((c) => (
                    <tr key={c.conversation_id} className="border-b border-gray-100 dark:border-gray-700 hover:bg-gray-50 dark:hover:bg-gray-700/50">
                      <td className="px-4 py-3 text-sm font-medium text-gray-900 dark:text-gray-100">
                        {accountMap[c.conversation_id] ?? <span className="text-gray-400 dark:text-gray-500">—</span>}
                      </td>
                      <td className="px-4 py-3 text-sm text-gray-700 dark:text-gray-300 whitespace-nowrap">
                        {formatDate(c.start_time_unix_secs)}
                      </td>
                      <td className="px-4 py-3 text-sm text-gray-500 dark:text-gray-400 font-mono text-xs">
                        {c.conversation_id}
                      </td>
                      <td className="px-4 py-3 text-sm text-gray-700 dark:text-gray-300">{c.agent_name ?? '—'}</td>
                      <td className="px-4 py-3 text-sm text-gray-600 dark:text-gray-400">
                        {formatDuration(c.call_duration_secs)}
                      </td>
                      <td className="px-4 py-3 text-sm">
                        <CallSuccessBadge value={c.call_successful} />
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          {nextCursor && (
            <div className="px-4 py-4 border-t border-gray-100 dark:border-gray-700 text-center">
              <button
                onClick={loadMore}
                disabled={loadingMore}
                className="px-5 py-2 bg-blue-600 text-white rounded-md text-sm font-medium hover:bg-blue-700 disabled:opacity-50 transition-colors"
              >
                {loadingMore ? 'Loading…' : 'Load More'}
              </button>
            </div>
          )}
          </>
        )}
      </div>
    </div>
  );
}
