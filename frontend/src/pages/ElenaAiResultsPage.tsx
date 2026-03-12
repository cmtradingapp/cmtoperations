/**
 * Elena AI Results — sync SquareTalk call results to MSSQL and view them.
 * Replaces the Make.com Data_Read_Elena_AI scenario.
 */

import { useEffect, useRef, useState } from 'react';

const API_BASE = import.meta.env.VITE_API_BASE_URL || '/api';

function authHeaders(): Record<string, string> {
  const auth = JSON.parse(localStorage.getItem('auth') || '{}');
  const token = auth?.state?.token;
  return token ? { Authorization: `Bearer ${token}`, 'Content-Type': 'application/json' } : { 'Content-Type': 'application/json' };
}

// ── Types ──────────────────────────────────────────────────────────────

interface CampaignConfig {
  id: number;
  campaign_id: string;
  label: string | null;
  created_at: string | null;
}

interface SyncProgress {
  fetched: number;
  inserted: number;
  skipped: number;
  errors: number;
}

interface CampaignProgress extends SyncProgress {
  campaign_id: string;
  label: string;
  done: boolean;
}

interface ResultItem {
  call_id: string;
  user_id: string | null;
  campaign: string | null;
  duration: number | null;
  call_start: string | null;
  call_status: string | null;
  goal_reached: boolean;
  modification_date: string | null;
}

// ── Helpers ────────────────────────────────────────────────────────────

function fmt(n: number | null | undefined): string {
  if (n === null || n === undefined) return '—';
  return n.toLocaleString();
}

function fmtDate(s: string | null | undefined): string {
  if (!s) return '—';
  try { return new Date(s).toLocaleString('en-GB'); } catch { return s; }
}

function fmtDuration(secs: number | null | undefined): string {
  if (!secs) return '—';
  const m = Math.floor(secs / 60);
  const s = secs % 60;
  return m > 0 ? `${m}m ${s}s` : `${s}s`;
}

// ── Main component ─────────────────────────────────────────────────────

export function ElenaAiResultsPage() {
  const [tab, setTab] = useState<'sync' | 'results' | 'summary'>('sync');

  // Campaign configs
  const [configs, setConfigs] = useState<CampaignConfig[]>([]);
  const [newCampaignId, setNewCampaignId] = useState('');
  const [newLabel, setNewLabel] = useState('');
  const [configLoading, setConfigLoading] = useState(false);
  const [configError, setConfigError] = useState('');

  // Sync state
  const [syncing, setSyncing] = useState(false);
  const [syncDone, setSyncDone] = useState(false);
  const [campaignProgress, setCampaignProgress] = useState<Record<string, CampaignProgress>>({});
  const [grandTotal, setGrandTotal] = useState<SyncProgress | null>(null);
  const esRef = useRef<EventSource | null>(null);

  // Summary tab
  interface CampaignSummary {
    campaign: string;
    total_calls: number;
    goal_reached: number;
    total_duration_minutes: number;
    statuses: Record<string, number>;
  }
  const [summary, setSummary] = useState<CampaignSummary[]>([]);
  const [summaryLoading, setSummaryLoading] = useState(false);

  // Results tab
  const [results, setResults] = useState<ResultItem[]>([]);
  const [resultsTotal, setResultsTotal] = useState(0);
  const [resultsPage, setResultsPage] = useState(1);
  const [resultsLoading, setResultsLoading] = useState(false);
  const [campaignFilter, setCampaignFilter] = useState('');
  const [userIdFilter, setUserIdFilter] = useState('');
  const [statusFilter, setStatusFilter] = useState('');
  const [availableCampaigns, setAvailableCampaigns] = useState<string[]>([]);

  const PAGE_SIZE = 50;

  // ── Load campaign configs ──────────────────────────────────────────

  const loadConfigs = async () => {
    setConfigLoading(true);
    try {
      const res = await fetch(`${API_BASE}/elena-ai/campaign-configs`, { headers: authHeaders() });
      if (!res.ok) throw new Error(`${res.status}`);
      setConfigs(await res.json());
    } catch (e: any) {
      setConfigError(`Failed to load configs: ${e.message}`);
    } finally {
      setConfigLoading(false);
    }
  };

  useEffect(() => { loadConfigs(); }, []);

  const addConfig = async () => {
    if (!newCampaignId.trim()) return;
    setConfigError('');
    try {
      const res = await fetch(`${API_BASE}/elena-ai/campaign-configs`, {
        method: 'POST',
        headers: authHeaders(),
        body: JSON.stringify({ campaign_id: newCampaignId.trim(), label: newLabel.trim() || null }),
      });
      if (!res.ok) throw new Error(`${res.status}`);
      setNewCampaignId('');
      setNewLabel('');
      await loadConfigs();
    } catch (e: any) {
      setConfigError(`Failed to add: ${e.message}`);
    }
  };

  const deleteConfig = async (id: number) => {
    if (!confirm('Remove this campaign config?')) return;
    try {
      await fetch(`${API_BASE}/elena-ai/campaign-configs/${id}`, { method: 'DELETE', headers: authHeaders() });
      await loadConfigs();
    } catch (e: any) {
      setConfigError(`Failed to delete: ${e.message}`);
    }
  };

  // ── Sync via SSE ───────────────────────────────────────────────────

  const startSync = () => {
    if (syncing) return;
    if (configs.length === 0) { setConfigError('Add at least one campaign first.'); return; }

    setSyncing(true);
    setSyncDone(false);
    setGrandTotal(null);
    setCampaignProgress({});
    setConfigError('');

    // Build URL with auth token as query param (EventSource doesn't support custom headers)
    const auth = JSON.parse(localStorage.getItem('auth') || '{}');
    const token = auth?.state?.token || '';
    const url = `${API_BASE}/elena-ai/sync-stream?token=${encodeURIComponent(token)}`;

    const es = new EventSource(url);
    esRef.current = es;

    es.onmessage = (e) => {
      try {
        const data = JSON.parse(e.data);

        if (data.type === 'campaign_start') {
          setCampaignProgress(prev => ({
            ...prev,
            [data.campaign_id]: {
              campaign_id: data.campaign_id,
              label: data.label,
              fetched: 0, inserted: 0, skipped: 0, errors: 0,
              done: false,
            },
          }));
        } else if (data.type === 'page_progress') {
          setCampaignProgress(prev => ({
            ...prev,
            [data.campaign_id]: {
              ...prev[data.campaign_id],
              fetched: data.total_fetched,
              inserted: data.total_inserted,
              skipped: data.total_skipped,
              errors: data.total_errors,
            },
          }));
        } else if (data.type === 'campaign_done') {
          setCampaignProgress(prev => ({
            ...prev,
            [data.campaign_id]: {
              ...prev[data.campaign_id],
              fetched: data.fetched,
              inserted: data.inserted,
              skipped: data.skipped,
              errors: data.errors,
              done: true,
            },
          }));
        } else if (data.type === 'complete') {
          setGrandTotal({ fetched: data.fetched, inserted: data.inserted, skipped: data.skipped, errors: data.errors });
          setSyncing(false);
          setSyncDone(true);
          es.close();
        } else if (data.type === 'page_error') {
          setCampaignProgress(prev => ({
            ...prev,
            [data.campaign_id]: {
              ...prev[data.campaign_id],
              errors: (prev[data.campaign_id]?.errors || 0) + 1,
            },
          }));
        }
      } catch { /* ignore parse errors */ }
    };

    es.onerror = () => {
      setSyncing(false);
      setSyncDone(true);
      es.close();
    };
  };

  const stopSync = () => {
    esRef.current?.close();
    setSyncing(false);
    setSyncDone(true);
  };

  // ── Load results ───────────────────────────────────────────────────

  const loadResults = async (p = 1) => {
    setResultsLoading(true);
    try {
      const params = new URLSearchParams({ page: String(p), page_size: String(PAGE_SIZE) });
      if (campaignFilter) params.set('campaign', campaignFilter);
      if (userIdFilter.trim()) params.set('user_id', userIdFilter.trim());
      if (statusFilter) params.set('call_status', statusFilter);
      const res = await fetch(`${API_BASE}/elena-ai/results?${params}`, { headers: authHeaders() });
      if (!res.ok) throw new Error(`${res.status}`);
      const data = await res.json();
      setResults(data.items);
      setResultsTotal(data.total);
      setResultsPage(p);
    } catch (e: any) {
      console.error('Failed to load results:', e);
    } finally {
      setResultsLoading(false);
    }
  };

  const loadAvailableCampaigns = async () => {
    try {
      const res = await fetch(`${API_BASE}/elena-ai/results/campaigns`, { headers: authHeaders() });
      if (res.ok) setAvailableCampaigns(await res.json());
    } catch { /* ignore */ }
  };

  const loadSummary = async () => {
    setSummaryLoading(true);
    try {
      const res = await fetch(`${API_BASE}/elena-ai/results/summary`, { headers: authHeaders() });
      if (!res.ok) throw new Error(`${res.status}`);
      setSummary(await res.json());
    } catch (e: any) {
      console.error('Failed to load summary:', e);
    } finally {
      setSummaryLoading(false);
    }
  };

  useEffect(() => {
    if (tab === 'results') {
      loadResults(1);
      loadAvailableCampaigns();
    }
    if (tab === 'summary') {
      loadSummary();
    }
  }, [tab]);

  const totalPages = Math.ceil(resultsTotal / PAGE_SIZE);

  // ── Render ─────────────────────────────────────────────────────────

  return (
    <div className="space-y-5">
      {/* Tabs */}
      <div className="flex gap-1 border-b border-gray-200 dark:border-gray-700">
        {([['sync', 'Sync Results'], ['results', 'View Results'], ['summary', 'Campaign Summary']] as const).map(([t, label]) => (
          <button
            key={t}
            onClick={() => setTab(t)}
            className={`px-4 py-2 text-sm font-medium border-b-2 transition-colors ${
              tab === t
                ? 'border-blue-600 text-blue-600 dark:text-blue-400 dark:border-blue-400'
                : 'border-transparent text-gray-500 dark:text-gray-400 hover:text-gray-700 dark:hover:text-gray-300'
            }`}
          >
            {label}
          </button>
        ))}
      </div>

      {/* ── SYNC TAB ── */}
      {tab === 'sync' && (
        <div className="space-y-5">

          {/* Campaign config management */}
          <div className="bg-white dark:bg-gray-800 rounded-lg shadow dark:shadow-gray-900 p-5 space-y-4">
            <h2 className="text-sm font-semibold text-gray-700 dark:text-gray-300">Campaign Configuration</h2>
            <p className="text-xs text-gray-500 dark:text-gray-400">
              Add the SquareTalk campaign IDs you want to sync. These replace the Google Sheets input.
            </p>

            {/* Add form */}
            <div className="flex flex-wrap gap-2">
              <input
                type="text"
                placeholder="Campaign ID"
                value={newCampaignId}
                onChange={e => setNewCampaignId(e.target.value)}
                onKeyDown={e => e.key === 'Enter' && addConfig()}
                className="border border-gray-300 dark:border-gray-600 rounded-md px-3 py-1.5 text-sm dark:bg-gray-700 dark:text-gray-100 w-48"
              />
              <input
                type="text"
                placeholder="Label (optional)"
                value={newLabel}
                onChange={e => setNewLabel(e.target.value)}
                onKeyDown={e => e.key === 'Enter' && addConfig()}
                className="border border-gray-300 dark:border-gray-600 rounded-md px-3 py-1.5 text-sm dark:bg-gray-700 dark:text-gray-100 w-48"
              />
              <button
                onClick={addConfig}
                disabled={!newCampaignId.trim() || configLoading}
                className="px-4 py-1.5 bg-blue-600 text-white rounded-md text-sm font-medium hover:bg-blue-700 disabled:opacity-50"
              >
                Add Campaign
              </button>
            </div>

            {configError && <p className="text-xs text-red-600">{configError}</p>}

            {/* Config list */}
            {configs.length > 0 && (
              <div className="border border-gray-200 dark:border-gray-700 rounded-md overflow-hidden">
                <table className="w-full text-sm">
                  <thead className="bg-gray-50 dark:bg-gray-900">
                    <tr>
                      {['Campaign ID', 'Label', 'Added', ''].map(h => (
                        <th key={h} className="px-4 py-2 text-left text-xs font-medium text-gray-500 dark:text-gray-400 uppercase">{h}</th>
                      ))}
                    </tr>
                  </thead>
                  <tbody>
                    {configs.map(c => (
                      <tr key={c.id} className="border-t border-gray-100 dark:border-gray-700">
                        <td className="px-4 py-2 font-mono text-xs text-gray-800 dark:text-gray-200">{c.campaign_id}</td>
                        <td className="px-4 py-2 text-xs text-gray-600 dark:text-gray-400">{c.label || '—'}</td>
                        <td className="px-4 py-2 text-xs text-gray-500 dark:text-gray-500">{fmtDate(c.created_at)}</td>
                        <td className="px-4 py-2">
                          <button
                            onClick={() => deleteConfig(c.id)}
                            className="text-xs text-red-500 hover:text-red-700"
                          >
                            Remove
                          </button>
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            )}

            {configs.length === 0 && !configLoading && (
              <p className="text-xs text-gray-400 dark:text-gray-500">No campaigns configured yet.</p>
            )}
          </div>

          {/* Sync controls */}
          <div className="bg-white dark:bg-gray-800 rounded-lg shadow dark:shadow-gray-900 p-5 space-y-4">
            <div className="flex items-center justify-between flex-wrap gap-3">
              <div>
                <h2 className="text-sm font-semibold text-gray-700 dark:text-gray-300">Sync Call Results</h2>
                <p className="text-xs text-gray-500 dark:text-gray-400 mt-0.5">
                  Fetches up to 3,000 calls per campaign and inserts new records into MSSQL.
                </p>
              </div>
              <div className="flex gap-2">
                {syncing && (
                  <button
                    onClick={stopSync}
                    className="px-4 py-2 bg-red-600 text-white rounded-md text-sm font-medium hover:bg-red-700"
                  >
                    Stop
                  </button>
                )}
                <button
                  onClick={startSync}
                  disabled={syncing || configs.length === 0}
                  className="flex items-center gap-2 px-4 py-2 bg-green-600 text-white rounded-md text-sm font-medium hover:bg-green-700 disabled:opacity-50"
                >
                  {syncing && (
                    <svg className="animate-spin h-4 w-4" fill="none" viewBox="0 0 24 24">
                      <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4" />
                      <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4z" />
                    </svg>
                  )}
                  {syncing ? 'Syncing...' : 'Start Sync'}
                </button>
              </div>
            </div>

            {/* Grand total bar */}
            {(syncing || syncDone) && grandTotal === null && Object.keys(campaignProgress).length > 0 && (
              <div className="grid grid-cols-4 gap-3">
                {(['fetched', 'inserted', 'skipped', 'errors'] as const).map(k => {
                  const total = Object.values(campaignProgress).reduce((s, c) => s + c[k], 0);
                  const colors: Record<string, string> = {
                    fetched: 'text-blue-600', inserted: 'text-green-600',
                    skipped: 'text-yellow-600', errors: 'text-red-500',
                  };
                  return (
                    <div key={k} className="bg-gray-50 dark:bg-gray-900 rounded-lg p-3 text-center">
                      <p className={`text-2xl font-bold ${colors[k]}`}>{fmt(total)}</p>
                      <p className="text-xs text-gray-500 dark:text-gray-400 mt-1 capitalize">{k}</p>
                    </div>
                  );
                })}
              </div>
            )}

            {/* Final grand total */}
            {grandTotal && (
              <div className="grid grid-cols-4 gap-3">
                {([['fetched', 'text-blue-600', 'Fetched'], ['inserted', 'text-green-600', 'Inserted'],
                   ['skipped', 'text-yellow-600', 'Skipped'], ['errors', 'text-red-500', 'Errors']] as const).map(([k, color, label]) => (
                  <div key={k} className="bg-gray-50 dark:bg-gray-900 rounded-lg p-3 text-center">
                    <p className={`text-2xl font-bold ${color}`}>{fmt(grandTotal[k])}</p>
                    <p className="text-xs text-gray-500 dark:text-gray-400 mt-1">{label}</p>
                  </div>
                ))}
              </div>
            )}

            {/* Per-campaign progress */}
            {Object.keys(campaignProgress).length > 0 && (
              <div className="space-y-3">
                {Object.values(campaignProgress).map(cp => (
                  <div key={cp.campaign_id} className="border border-gray-200 dark:border-gray-700 rounded-lg p-4">
                    <div className="flex items-center justify-between mb-3">
                      <div>
                        <span className="text-sm font-medium text-gray-800 dark:text-gray-200">{cp.label}</span>
                        <span className="ml-2 text-xs text-gray-400 font-mono">{cp.campaign_id}</span>
                      </div>
                      {cp.done
                        ? <span className="text-xs px-2 py-0.5 rounded-full bg-green-100 text-green-700 font-medium">Done</span>
                        : <span className="text-xs px-2 py-0.5 rounded-full bg-blue-100 text-blue-700 font-medium animate-pulse">Reading...</span>
                      }
                    </div>
                    <div className="grid grid-cols-4 gap-2">
                      {([['fetched', 'bg-blue-50 dark:bg-blue-950 text-blue-700 dark:text-blue-300', 'Read'],
                         ['inserted', 'bg-green-50 dark:bg-green-950 text-green-700 dark:text-green-300', 'New'],
                         ['skipped', 'bg-yellow-50 dark:bg-yellow-950 text-yellow-700 dark:text-yellow-300', 'Existing'],
                         ['errors', 'bg-red-50 dark:bg-red-950 text-red-700 dark:text-red-300', 'Failed']] as const).map(([k, cls, label]) => (
                        <div key={k} className={`${cls} rounded-md p-2 text-center`}>
                          <p className="text-lg font-bold">{fmt(cp[k])}</p>
                          <p className="text-xs mt-0.5">{label}</p>
                        </div>
                      ))}
                    </div>
                  </div>
                ))}
              </div>
            )}

            {syncDone && grandTotal && (
              <p className="text-xs text-green-600 dark:text-green-400 font-medium">
                Sync complete — {fmt(grandTotal.inserted)} new records inserted into MSSQL.
              </p>
            )}
          </div>
        </div>
      )}

      {/* ── CAMPAIGN SUMMARY TAB ── */}
      {tab === 'summary' && (
        <div className="space-y-4">
          <div className="flex items-center justify-between">
            <p className="text-xs text-gray-500 dark:text-gray-400">Aggregated stats per campaign from MSSQL.</p>
            <button
              onClick={loadSummary}
              disabled={summaryLoading}
              className="px-3 py-1.5 bg-blue-600 text-white rounded-md text-xs font-medium hover:bg-blue-700 disabled:opacity-50"
            >
              {summaryLoading ? 'Loading...' : 'Refresh'}
            </button>
          </div>

          {summaryLoading && (
            <div className="text-center py-10 text-sm text-gray-400">Loading...</div>
          )}

          {!summaryLoading && summary.length === 0 && (
            <div className="text-center py-10 text-sm text-gray-400">No data yet — run a sync first.</div>
          )}

          {!summaryLoading && summary.map(cam => {
            const goalPct = cam.total_calls > 0
              ? Math.round((cam.goal_reached / cam.total_calls) * 100)
              : 0;
            const statusEntries = Object.entries(cam.statuses).sort((a, b) => b[1] - a[1]);

            return (
              <div key={cam.campaign} className="bg-white dark:bg-gray-800 rounded-lg shadow dark:shadow-gray-900 p-5">
                {/* Campaign header */}
                <h3 className="text-sm font-semibold text-gray-800 dark:text-gray-100 mb-4">{cam.campaign}</h3>

                {/* KPI strip */}
                <div className="grid grid-cols-2 sm:grid-cols-4 gap-3 mb-4">
                  <div className="bg-gray-50 dark:bg-gray-900 rounded-lg p-3 text-center">
                    <p className="text-xl font-bold text-gray-800 dark:text-gray-100">{cam.total_calls.toLocaleString()}</p>
                    <p className="text-xs text-gray-500 dark:text-gray-400 mt-0.5">Total Calls</p>
                  </div>
                  <div className="bg-green-50 dark:bg-green-950 rounded-lg p-3 text-center">
                    <p className="text-xl font-bold text-green-700 dark:text-green-300">
                      {cam.goal_reached.toLocaleString()}
                      <span className="text-sm font-normal ml-1 text-green-500">({goalPct}%)</span>
                    </p>
                    <p className="text-xs text-gray-500 dark:text-gray-400 mt-0.5">Goal Reached</p>
                  </div>
                  <div className="bg-blue-50 dark:bg-blue-950 rounded-lg p-3 text-center">
                    <p className="text-xl font-bold text-blue-700 dark:text-blue-300">
                      {cam.total_duration_minutes.toLocaleString()}
                      <span className="text-sm font-normal ml-1">min</span>
                    </p>
                    <p className="text-xs text-gray-500 dark:text-gray-400 mt-0.5">Total Duration</p>
                  </div>
                  <div className="bg-purple-50 dark:bg-purple-950 rounded-lg p-3 text-center">
                    <p className="text-xl font-bold text-purple-700 dark:text-purple-300">
                      {cam.total_calls > 0 ? Math.round(cam.total_duration_minutes / cam.total_calls * 60) : 0}
                      <span className="text-sm font-normal ml-1">s avg</span>
                    </p>
                    <p className="text-xs text-gray-500 dark:text-gray-400 mt-0.5">Avg Call Duration</p>
                  </div>
                </div>

                {/* Status breakdown */}
                {statusEntries.length > 0 && (
                  <div>
                    <p className="text-xs font-medium text-gray-500 dark:text-gray-400 mb-2 uppercase tracking-wide">Status Breakdown</p>
                    <div className="space-y-1.5">
                      {statusEntries.map(([status, count]) => {
                        const pct = cam.total_calls > 0 ? Math.round((count / cam.total_calls) * 100) : 0;
                        return (
                          <div key={status} className="flex items-center gap-3">
                            <span className="text-xs text-gray-600 dark:text-gray-300 w-32 shrink-0 capitalize">{status}</span>
                            <div className="flex-1 bg-gray-100 dark:bg-gray-700 rounded-full h-2">
                              <div
                                className="bg-blue-500 h-2 rounded-full transition-all"
                                style={{ width: `${pct}%` }}
                              />
                            </div>
                            <span className="text-xs font-medium text-gray-700 dark:text-gray-300 w-20 text-right">
                              {count.toLocaleString()} ({pct}%)
                            </span>
                          </div>
                        );
                      })}
                    </div>
                  </div>
                )}
              </div>
            );
          })}
        </div>
      )}

      {/* ── RESULTS TAB ── */}
      {tab === 'results' && (
        <div className="space-y-4">
          {/* Filters */}
          <div className="bg-white dark:bg-gray-800 rounded-lg shadow dark:shadow-gray-900 p-4">
            <div className="flex flex-wrap gap-3 items-end">
              <div>
                <label className="block text-xs font-medium text-gray-600 dark:text-gray-400 mb-1">Campaign</label>
                <select
                  value={campaignFilter}
                  onChange={e => setCampaignFilter(e.target.value)}
                  className="border border-gray-300 dark:border-gray-600 rounded-md px-3 py-1.5 text-sm dark:bg-gray-700 dark:text-gray-100 w-48"
                >
                  <option value="">All campaigns</option>
                  {availableCampaigns.map(c => <option key={c} value={c}>{c}</option>)}
                </select>
              </div>
              <div>
                <label className="block text-xs font-medium text-gray-600 dark:text-gray-400 mb-1">User ID</label>
                <input
                  type="text"
                  placeholder="Account ID"
                  value={userIdFilter}
                  onChange={e => setUserIdFilter(e.target.value)}
                  className="border border-gray-300 dark:border-gray-600 rounded-md px-3 py-1.5 text-sm dark:bg-gray-700 dark:text-gray-100 w-40"
                />
              </div>
              <div>
                <label className="block text-xs font-medium text-gray-600 dark:text-gray-400 mb-1">Status</label>
                <input
                  type="text"
                  placeholder="e.g. completed"
                  value={statusFilter}
                  onChange={e => setStatusFilter(e.target.value)}
                  className="border border-gray-300 dark:border-gray-600 rounded-md px-3 py-1.5 text-sm dark:bg-gray-700 dark:text-gray-100 w-40"
                />
              </div>
              <button
                onClick={() => loadResults(1)}
                className="px-4 py-1.5 bg-blue-600 text-white rounded-md text-sm font-medium hover:bg-blue-700"
              >
                Search
              </button>
            </div>
          </div>

          {/* Table */}
          <div className="bg-white dark:bg-gray-800 rounded-lg shadow dark:shadow-gray-900 overflow-hidden">
            <div className="px-4 py-3 border-b border-gray-200 dark:border-gray-700 flex items-center justify-between">
              <span className="text-sm text-gray-600 dark:text-gray-400">
                {resultsTotal.toLocaleString()} records
              </span>
              {totalPages > 1 && (
                <div className="flex items-center gap-2">
                  <button
                    onClick={() => loadResults(resultsPage - 1)}
                    disabled={resultsPage <= 1 || resultsLoading}
                    className="px-2 py-1 text-xs border rounded disabled:opacity-40 dark:border-gray-600 dark:text-gray-300"
                  >
                    Prev
                  </button>
                  <span className="text-xs text-gray-500 dark:text-gray-400">{resultsPage} / {totalPages}</span>
                  <button
                    onClick={() => loadResults(resultsPage + 1)}
                    disabled={resultsPage >= totalPages || resultsLoading}
                    className="px-2 py-1 text-xs border rounded disabled:opacity-40 dark:border-gray-600 dark:text-gray-300"
                  >
                    Next
                  </button>
                </div>
              )}
            </div>

            <div className="overflow-x-auto">
              <table className="w-full text-sm">
                <thead className="bg-gray-50 dark:bg-gray-900">
                  <tr>
                    {['Call ID', 'User ID', 'Campaign', 'Status', 'Goal Reached', 'Duration', 'Call Start'].map(h => (
                      <th key={h} className="px-4 py-3 text-left text-xs font-medium text-gray-500 dark:text-gray-400 uppercase whitespace-nowrap">{h}</th>
                    ))}
                  </tr>
                </thead>
                <tbody>
                  {resultsLoading && (
                    <tr>
                      <td colSpan={7} className="px-4 py-8 text-center text-sm text-gray-400">Loading...</td>
                    </tr>
                  )}
                  {!resultsLoading && results.length === 0 && (
                    <tr>
                      <td colSpan={7} className="px-4 py-8 text-center text-sm text-gray-400">No records found.</td>
                    </tr>
                  )}
                  {!resultsLoading && results.map(r => (
                    <tr key={r.call_id} className="border-t border-gray-100 dark:border-gray-700 hover:bg-gray-50 dark:hover:bg-gray-750">
                      <td className="px-4 py-2 font-mono text-xs text-gray-600 dark:text-gray-400">{r.call_id}</td>
                      <td className="px-4 py-2 text-sm font-medium text-gray-900 dark:text-gray-100">{r.user_id || '—'}</td>
                      <td className="px-4 py-2 text-xs text-gray-600 dark:text-gray-400">{r.campaign || '—'}</td>
                      <td className="px-4 py-2">
                        <span className="px-2 py-0.5 rounded-full text-xs font-medium bg-gray-100 dark:bg-gray-700 text-gray-600 dark:text-gray-300">
                          {r.call_status || '—'}
                        </span>
                      </td>
                      <td className="px-4 py-2">
                        {r.goal_reached
                          ? <span className="px-2 py-0.5 rounded-full text-xs font-medium bg-green-100 text-green-700">Yes</span>
                          : <span className="px-2 py-0.5 rounded-full text-xs font-medium bg-gray-100 dark:bg-gray-700 text-gray-500 dark:text-gray-400">No</span>
                        }
                      </td>
                      <td className="px-4 py-2 text-sm text-gray-700 dark:text-gray-300">{fmtDuration(r.duration)}</td>
                      <td className="px-4 py-2 text-xs text-gray-500 dark:text-gray-400 whitespace-nowrap">{fmtDate(r.call_start)}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}
