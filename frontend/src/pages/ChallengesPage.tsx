/**
 * CLAUD-89/90: Challenges Module — Admin page for managing daily trade challenges
 * and viewing client progress.
 */
import { useEffect, useState } from 'react';

import { ChallengeDashboardTab } from '../components/ChallengeDashboardTab';
import {
  getChallenges,
  createChallenge,
  toggleChallenge,
  deleteChallenge,
  getChallengeProgress,
  getOptimoveEvents,
  getSymbolMappings,
  upsertSymbolMapping,
  deleteSymbolMapping,
} from '../api/challenges';
import type {
  ChallengeGroup,
  CreateChallengePayload,
  AudienceCriteria,
  ChallengeProgressItem,
  OptimoveEventItem,
  SymbolMapping,
} from '../api/challenges';

// ---------------------------------------------------------------------------
// Empty form state
// ---------------------------------------------------------------------------

interface TierForm {
  name: string;
  targetvalue: string;
  rewardamount: string;
  symbol: string;
}

const EMPTY_TIER: TierForm = { name: '', targetvalue: '', rewardamount: '', symbol: '' };

interface ChallengeForm {
  group_name: string;
  type: 'trade' | 'volume' | 'streak' | 'pnl' | 'diversity' | 'instrument';
  tiers: TierForm[];
  // Audience
  audience_mode: 'all' | 'filter' | 'csv';  // replaces all_clients boolean
  countries: string;
  languages: string;
  balance_min: string;
  balance_max: string;
  last_trade_before: string;
  qualified_before: string;
  account_ids: string[];   // populated from CSV
  // Flash (CLAUD-95)
  timeperiod: 'daily' | 'weekly';
  is_flash: boolean;
  valid_until: string;      // datetime-local input value
  reward_multiplier: string; // number input value
  // Expiry
  expires_on: string;  // YYYY-MM-DD or ''
}

const EMPTY_FORM: ChallengeForm = {
  group_name: '',
  type: 'trade',
  tiers: [{ ...EMPTY_TIER }],
  audience_mode: 'all',
  countries: '',
  languages: '',
  balance_min: '',
  balance_max: '',
  last_trade_before: '',
  qualified_before: '',
  account_ids: [],
  timeperiod: 'daily',
  is_flash: false,
  valid_until: '',
  reward_multiplier: '1',
  expires_on: '',
};

// ---------------------------------------------------------------------------
// Helper: today as YYYY-MM-DD
// ---------------------------------------------------------------------------
function todayStr(): string {
  const d = new Date();
  const yyyy = d.getFullYear();
  const mm = String(d.getMonth() + 1).padStart(2, '0');
  const dd = String(d.getDate()).padStart(2, '0');
  return `${yyyy}-${mm}-${dd}`;
}

// ---------------------------------------------------------------------------
// Helper: parse CSV file content into deduplicated account ID list
// ---------------------------------------------------------------------------
function parseCsvAccountIds(text: string): string[] {
  const ids: string[] = [];
  const lines = text.split(/\r?\n/);
  for (const line of lines) {
    const cells = line.split(',');
    for (const cell of cells) {
      const val = cell.trim().replace(/"/g, '');
      if (/^\d+$/.test(val) && val.length > 3) {
        ids.push(val);
        break;
      }
    }
  }
  return [...new Set(ids)]; // deduplicate
}

// ---------------------------------------------------------------------------
// Status badge colors
// ---------------------------------------------------------------------------
function statusBadge(status: string) {
  switch (status) {
    case 'Open':
      return 'bg-gray-100 dark:bg-gray-600 text-gray-600 dark:text-gray-300';
    case 'In Progress':
      return 'bg-yellow-100 dark:bg-yellow-900/40 text-yellow-800 dark:text-yellow-300';
    case 'Completed':
      return 'bg-green-100 dark:bg-green-900/40 text-green-800 dark:text-green-300';
    case 'Cancelled':
      return 'bg-red-100 dark:bg-red-900/40 text-red-800 dark:text-red-300';
    default:
      return 'bg-gray-100 dark:bg-gray-600 text-gray-600 dark:text-gray-300';
  }
}

// ---------------------------------------------------------------------------
// Component
// ---------------------------------------------------------------------------

export function ChallengesPage() {
  const [activeTab, setActiveTab] = useState<'dashboard' | 'challenges' | 'progress' | 'events' | 'symbols'>('dashboard');

  // ---- Challenges tab state ------------------------------------------------
  const [groups, setGroups] = useState<ChallengeGroup[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState('');
  const [showForm, setShowForm] = useState(false);
  const [form, setForm] = useState<ChallengeForm>({ ...EMPTY_FORM, tiers: [{ ...EMPTY_TIER }] });
  const [saving, setSaving] = useState(false);
  const [formError, setFormError] = useState('');
  const [expanded, setExpanded] = useState<Set<string>>(new Set());

  // ---- Progress tab state --------------------------------------------------
  const [progressItems, setProgressItems] = useState<ChallengeProgressItem[]>([]);
  const [progressTotal, setProgressTotal] = useState(0);
  const [progressLoading, setProgressLoading] = useState(false);
  const [progressError, setProgressError] = useState('');
  const [progressDate, setProgressDate] = useState(todayStr());
  const [progressGroup, setProgressGroup] = useState('');
  const [progressPage, setProgressPage] = useState(1);
  const PAGE_SIZE = 50;

  // ---- Events Log tab state (CLAUD-91) ------------------------------------
  const [eventsItems, setEventsItems] = useState<OptimoveEventItem[]>([]);
  const [eventsTotal, setEventsTotal] = useState(0);
  const [eventsLoading, setEventsLoading] = useState(false);
  const [eventsError, setEventsError] = useState('');
  const [eventsDate, setEventsDate] = useState(todayStr());
  const [eventsEventName, setEventsEventName] = useState('');
  const [eventsPage, setEventsPage] = useState(1);

  // ---- Symbol Mapping tab state (CLAUD-94) ---------------------------------
  const [symbols, setSymbols] = useState<SymbolMapping[]>([]);
  const [symbolsLoading, setSymbolsLoading] = useState(false);
  const [symbolsError, setSymbolsError] = useState('');
  const [newSymbol, setNewSymbol] = useState('');
  const [newAssetClass, setNewAssetClass] = useState<SymbolMapping['asset_class']>('forex');
  const [symbolSaving, setSymbolSaving] = useState(false);

  // ---- Symbol Mappings for instrument challenge form -----------------------
  const [symbolMappings, setSymbolMappings] = useState<SymbolMapping[]>([]);
  const [addSymbolModal, setAddSymbolModal] = useState<{ tierIdx: number } | null>(null);
  const [newSymbolForm, setNewSymbolForm] = useState({ symbol: '', asset_class: 'forex' as SymbolMapping['asset_class'] });
  const [addSymbolLoading, setAddSymbolLoading] = useState(false);

  // ---- Data loading (Challenges) -------------------------------------------

  const load = async () => {
    setLoading(true);
    setError('');
    try {
      const data = await getChallenges();
      setGroups(data);
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Failed to load challenges');
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    load();
    getSymbolMappings().then(setSymbolMappings).catch(() => {/* non-critical */});
  }, []);

  // ---- Data loading (Progress) ---------------------------------------------

  const loadProgress = async () => {
    setProgressLoading(true);
    setProgressError('');
    try {
      const data = await getChallengeProgress({
        date: progressDate,
        group_name: progressGroup || undefined,
        page: progressPage,
        page_size: PAGE_SIZE,
      });
      setProgressItems(data.items);
      setProgressTotal(data.total);
    } catch (e) {
      setProgressError(e instanceof Error ? e.message : 'Failed to load progress');
    } finally {
      setProgressLoading(false);
    }
  };

  useEffect(() => {
    if (activeTab === 'progress') {
      loadProgress();
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [activeTab, progressDate, progressGroup, progressPage]);

  // ---- Data loading (Events Log - CLAUD-91) --------------------------------

  const loadEvents = async () => {
    setEventsLoading(true);
    setEventsError('');
    try {
      const data = await getOptimoveEvents({
        date: eventsDate,
        event_name: eventsEventName || undefined,
        page: eventsPage,
        page_size: PAGE_SIZE,
      });
      setEventsItems(data.items);
      setEventsTotal(data.total);
    } catch (e) {
      setEventsError(e instanceof Error ? e.message : 'Failed to load events');
    } finally {
      setEventsLoading(false);
    }
  };

  useEffect(() => {
    if (activeTab === 'events') {
      loadEvents();
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [activeTab, eventsDate, eventsEventName, eventsPage]);

  // ---- Data loading (Symbol Mappings - CLAUD-94) ---------------------------

  const loadSymbols = async () => {
    setSymbolsLoading(true);
    setSymbolsError('');
    try {
      const data = await getSymbolMappings();
      setSymbols(data);
    } catch (e) {
      setSymbolsError(e instanceof Error ? e.message : 'Failed to load symbol mappings');
    } finally {
      setSymbolsLoading(false);
    }
  };

  useEffect(() => {
    if (activeTab === 'symbols') {
      loadSymbols();
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [activeTab]);

  // ---- Expand / collapse row -----------------------------------------------

  const toggleExpand = (gn: string) => {
    setExpanded((prev) => {
      const next = new Set(prev);
      if (next.has(gn)) next.delete(gn);
      else next.add(gn);
      return next;
    });
  };

  // ---- Toggle active / inactive --------------------------------------------

  const handleToggle = async (groupName: string) => {
    try {
      await toggleChallenge(groupName);
      await load();
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Failed to toggle challenge');
    }
  };

  // ---- Delete --------------------------------------------------------------

  const handleDelete = async (groupName: string) => {
    if (!confirm(`Delete challenge group "${groupName}"? This cannot be undone.`)) return;
    try {
      await deleteChallenge(groupName);
      await load();
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Failed to delete challenge');
    }
  };

  // ---- Form: tiers management ----------------------------------------------

  const addTier = () => {
    setForm((f) => ({ ...f, tiers: [...f.tiers, { ...EMPTY_TIER }] }));
  };

  const removeTier = (idx: number) => {
    setForm((f) => ({ ...f, tiers: f.tiers.filter((_, i) => i !== idx) }));
  };

  const updateTier = (idx: number, field: keyof TierForm, value: string) => {
    setForm((f) => {
      const tiers = [...f.tiers];
      tiers[idx] = { ...tiers[idx], [field]: value };
      return { ...f, tiers };
    });
  };

  // ---- Form submit ---------------------------------------------------------

  const handleSubmit = async () => {
    setFormError('');
    if (!form.group_name.trim()) {
      setFormError('Group name is required');
      return;
    }
    if (form.tiers.length === 0) {
      setFormError('At least one tier is required');
      return;
    }
    for (let i = 0; i < form.tiers.length; i++) {
      const t = form.tiers[i];
      if (!t.name.trim()) {
        setFormError(`Tier ${i + 1}: name is required`);
        return;
      }
      if (form.type !== 'instrument' && (!t.targetvalue || Number(t.targetvalue) <= 0)) {
        setFormError(`Tier ${i + 1}: target value must be > 0`);
        return;
      }
      if (!t.rewardamount || Number(t.rewardamount) <= 0) {
        setFormError(`Tier ${i + 1}: reward amount must be > 0`);
        return;
      }
    }

    if (form.type === 'instrument') {
      for (let i = 0; i < form.tiers.length; i++) {
        if (!form.tiers[i].symbol) {
          setFormError(`Tier ${i + 1}: please select a symbol`);
          return;
        }
      }
    }

    if (form.audience_mode === 'csv' && form.account_ids.length === 0) {
      setFormError('Please upload a CSV file with at least one account ID');
      return;
    }

    const audience: AudienceCriteria | null =
      form.audience_mode === 'all'
        ? { all_clients: true }
        : form.audience_mode === 'csv'
        ? { account_ids: form.account_ids }
        : {
            ...(form.countries.trim() ? { countries: form.countries.split(',').map((s) => s.trim()).filter(Boolean) } : {}),
            ...(form.languages.trim() ? { languages: form.languages.split(',').map((s) => s.trim()).filter(Boolean) } : {}),
            ...(form.balance_min ? { balance_min: Number(form.balance_min) } : {}),
            ...(form.balance_max ? { balance_max: Number(form.balance_max) } : {}),
            ...(form.last_trade_before ? { last_trade_before: form.last_trade_before } : {}),
            ...(form.qualified_before ? { qualified_before: form.qualified_before } : {}),
          };

    const payload: CreateChallengePayload = {
      group_name: form.group_name.trim(),
      type: form.type,
      tiers: form.tiers.map((t) => ({
        name: t.name.trim(),
        targetvalue: form.type === 'instrument' ? 1 : Number(t.targetvalue),
        rewardamount: Number(t.rewardamount),
        ...(form.type === 'instrument' && t.symbol ? { symbol: t.symbol } : {}),
      })),
      audience_criteria: audience,
      timeperiod: form.type === 'diversity' ? 'weekly' : 'daily',
      valid_until: form.is_flash && form.valid_until ? new Date(form.valid_until).toISOString() : null,
      reward_multiplier: form.is_flash ? Number(form.reward_multiplier) || 1.0 : 1.0,
      expires_on: form.expires_on || null,
    };

    setSaving(true);
    try {
      await createChallenge(payload);
      setForm({ ...EMPTY_FORM, tiers: [{ ...EMPTY_TIER }] });
      setShowForm(false);
      await load();
    } catch (e) {
      setFormError(e instanceof Error ? e.message : 'Failed to create challenge');
    } finally {
      setSaving(false);
    }
  };

  // ---- Pagination helpers --------------------------------------------------
  const totalPages = Math.max(1, Math.ceil(progressTotal / PAGE_SIZE));
  const eventsTotalPages = Math.max(1, Math.ceil(eventsTotal / PAGE_SIZE));

  // ---- Render --------------------------------------------------------------

  return (
    <div className="space-y-6">
      {/* Error banner */}
      {error && (
        <div className="bg-red-50 dark:bg-red-900/30 border border-red-200 dark:border-red-700 rounded-lg p-4 text-red-700 dark:text-red-300 text-sm">
          {error}
          <button onClick={() => setError('')} className="ml-3 underline text-xs">dismiss</button>
        </div>
      )}

      {/* Tab bar */}
      <div className="flex items-center gap-1 border-b border-gray-200 dark:border-gray-700">
        <button
          onClick={() => setActiveTab('dashboard')}
          className={`px-4 py-2.5 text-sm font-medium border-b-2 transition-colors ${
            activeTab === 'dashboard'
              ? 'border-blue-600 text-blue-600 dark:border-blue-400 dark:text-blue-400'
              : 'border-transparent text-gray-500 dark:text-gray-400 hover:text-gray-700 dark:hover:text-gray-300'
          }`}
        >
          Dashboard
        </button>
        <button
          onClick={() => setActiveTab('challenges')}
          className={`px-4 py-2.5 text-sm font-medium border-b-2 transition-colors ${
            activeTab === 'challenges'
              ? 'border-blue-600 text-blue-600 dark:border-blue-400 dark:text-blue-400'
              : 'border-transparent text-gray-500 dark:text-gray-400 hover:text-gray-700 dark:hover:text-gray-300'
          }`}
        >
          Challenges
        </button>
        <button
          onClick={() => setActiveTab('progress')}
          className={`px-4 py-2.5 text-sm font-medium border-b-2 transition-colors ${
            activeTab === 'progress'
              ? 'border-blue-600 text-blue-600 dark:border-blue-400 dark:text-blue-400'
              : 'border-transparent text-gray-500 dark:text-gray-400 hover:text-gray-700 dark:hover:text-gray-300'
          }`}
        >
          Client Progress
        </button>
        <button
          onClick={() => setActiveTab('events')}
          className={`px-4 py-2.5 text-sm font-medium border-b-2 transition-colors ${
            activeTab === 'events'
              ? 'border-blue-600 text-blue-600 dark:border-blue-400 dark:text-blue-400'
              : 'border-transparent text-gray-500 dark:text-gray-400 hover:text-gray-700 dark:hover:text-gray-300'
          }`}
        >
          Events Log
        </button>
        <button
          onClick={() => setActiveTab('symbols')}
          className={`px-4 py-2.5 text-sm font-medium border-b-2 transition-colors ${
            activeTab === 'symbols'
              ? 'border-blue-600 text-blue-600 dark:border-blue-400 dark:text-blue-400'
              : 'border-transparent text-gray-500 dark:text-gray-400 hover:text-gray-700 dark:hover:text-gray-300'
          }`}
        >
          Symbol Map
        </button>
      </div>

      {/* ============================================================ */}
      {/* TAB 0: Dashboard (CLAUD-181) */}
      {/* ============================================================ */}
      {activeTab === 'dashboard' && <ChallengeDashboardTab />}

      {/* ============================================================ */}
      {/* TAB 1: Challenges */}
      {/* ============================================================ */}
      {activeTab === 'challenges' && (
        <>
          {/* Header */}
          <div className="flex items-center justify-between">
            <div>
              <h2 className="text-xl font-semibold text-gray-800 dark:text-gray-100">Challenge Groups</h2>
              <p className="text-sm text-gray-500 dark:text-gray-400 mt-1">
                Manage daily trade challenges with credit rewards for clients.
              </p>
            </div>
            <button
              onClick={() => {
                setShowForm(!showForm);
                setFormError('');
                if (!showForm) setForm({ ...EMPTY_FORM, tiers: [{ ...EMPTY_TIER }] });
              }}
              className="px-4 py-2 bg-blue-600 hover:bg-blue-700 text-white text-sm font-medium rounded-lg transition-colors"
            >
              {showForm ? 'Cancel' : 'Create New Challenge'}
            </button>
          </div>

          {/* Create form */}
          {showForm && (
            <div className="bg-white dark:bg-gray-800 rounded-lg shadow border border-gray-200 dark:border-gray-700 p-6 space-y-5">
              <h3 className="text-lg font-semibold text-gray-800 dark:text-gray-100">New Challenge Group</h3>

              {formError && (
                <div className="bg-red-50 dark:bg-red-900/30 border border-red-200 dark:border-red-700 rounded-lg p-3 text-red-700 dark:text-red-300 text-sm">
                  {formError}
                </div>
              )}

              {/* Basic info */}
              <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                <div>
                  <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">Group Name</label>
                  <input
                    type="text"
                    value={form.group_name}
                    onChange={(e) => setForm((f) => ({ ...f, group_name: e.target.value }))}
                    className="w-full border border-gray-300 dark:border-gray-600 rounded-md px-3 py-2 text-sm bg-white dark:bg-gray-700 text-gray-800 dark:text-gray-100 focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
                    placeholder="e.g. Daily Trade Rush"
                  />
                </div>
                <div>
                  <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">Type</label>
                  <select
                    value={form.type}
                    onChange={(e) => setForm((f) => ({ ...f, type: e.target.value as ChallengeForm['type'] }))}
                    className="w-full border border-gray-300 dark:border-gray-600 rounded-md px-3 py-2 text-sm bg-white dark:bg-gray-700 text-gray-800 dark:text-gray-100 focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
                  >
                    <option value="trade">Count of Trades</option>
                    <option value="volume">Sum of Volume</option>
                    <option value="streak">Consecutive Trading Days</option>
                    <option value="pnl">Cumulative P&L</option>
                    <option value="diversity">Instrument Diversity</option>
                    <option value="instrument">Instrument</option>
                  </select>
                  {form.type === 'pnl' && (
                    <p className="mt-1 text-xs text-gray-500 dark:text-gray-400">Tracks cumulative realized profit per day (close_trade events)</p>
                  )}
                  {form.type === 'diversity' && (
                    <p className="mt-1 text-xs text-gray-500 dark:text-gray-400">Tracks distinct asset classes traded per week. Requires symbol mapping. Runs on weekly window (Mon–Sun).</p>
                  )}
                  {form.type === 'instrument' && (
                    <p className="mt-1 text-xs text-gray-500 dark:text-gray-400">Each tier targets a specific trading symbol. Tiers are independent — client earns each tier's reward when they trade the matching symbol on a given day.</p>
                  )}
                </div>
              </div>

              {/* Flash Challenge toggle (CLAUD-95) */}
              <div className="border border-amber-200 dark:border-amber-700 rounded-lg p-4 space-y-3 bg-amber-50/40 dark:bg-amber-900/10">
                <label className="flex items-center gap-2 text-sm font-medium text-gray-700 dark:text-gray-300">
                  <input
                    type="checkbox"
                    checked={form.is_flash}
                    onChange={(e) => setForm((f) => ({ ...f, is_flash: e.target.checked }))}
                    className="rounded border-gray-300 dark:border-gray-600 text-amber-500 focus:ring-amber-400"
                  />
                  ⚡ Flash Challenge (time-limited with optional reward multiplier)
                </label>
                {form.is_flash && (
                  <div className="grid grid-cols-1 md:grid-cols-2 gap-4 pt-1">
                    <div>
                      <label className="block text-xs font-medium text-gray-600 dark:text-gray-400 mb-1">Ends at (local time)</label>
                      <input
                        type="datetime-local"
                        value={form.valid_until}
                        onChange={(e) => setForm((f) => ({ ...f, valid_until: e.target.value }))}
                        className="w-full border border-gray-300 dark:border-gray-600 rounded-md px-3 py-2 text-sm bg-white dark:bg-gray-700 text-gray-800 dark:text-gray-100 focus:ring-2 focus:ring-amber-400 focus:border-amber-400"
                      />
                    </div>
                    <div>
                      <label className="block text-xs font-medium text-gray-600 dark:text-gray-400 mb-1">Reward Multiplier</label>
                      <input
                        type="number"
                        min="0.1"
                        step="0.1"
                        value={form.reward_multiplier}
                        onChange={(e) => setForm((f) => ({ ...f, reward_multiplier: e.target.value }))}
                        className="w-full border border-gray-300 dark:border-gray-600 rounded-md px-3 py-2 text-sm bg-white dark:bg-gray-700 text-gray-800 dark:text-gray-100 focus:ring-2 focus:ring-amber-400 focus:border-amber-400"
                        placeholder="2.0"
                      />
                      <p className="mt-1 text-xs text-gray-500 dark:text-gray-400">e.g. 2.0 = double rewards</p>
                    </div>
                  </div>
                )}
              </div>

              {/* Expiration Date */}
              <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                <div>
                  <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">
                    Expiration Date <span className="text-gray-400 font-normal">(optional)</span>
                  </label>
                  <input
                    type="date"
                    value={form.expires_on}
                    onChange={(e) => setForm((f) => ({ ...f, expires_on: e.target.value }))}
                    className="w-full border border-gray-300 dark:border-gray-600 rounded-md px-3 py-2 text-sm bg-white dark:bg-gray-700 text-gray-800 dark:text-gray-100 focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
                  />
                  {form.expires_on && (
                    <p className="mt-1 text-xs text-gray-500 dark:text-gray-400">Challenge will stop processing after this date</p>
                  )}
                </div>
              </div>

              {/* Audience criteria */}
              <div className="border border-gray-200 dark:border-gray-600 rounded-lg p-4 space-y-3">
                <h4 className="text-sm font-semibold text-gray-700 dark:text-gray-300">Audience</h4>

                {/* Mode selector */}
                <div className="flex flex-wrap gap-4">
                  {(['all', 'filter', 'csv'] as const).map((mode) => (
                    <label key={mode} className="flex items-center gap-2 text-sm text-gray-700 dark:text-gray-300 cursor-pointer">
                      <input
                        type="radio"
                        name="audience_mode"
                        value={mode}
                        checked={form.audience_mode === mode}
                        onChange={() => setForm((f) => ({ ...f, audience_mode: mode }))}
                        className="text-blue-600 focus:ring-blue-500"
                      />
                      {mode === 'all' ? 'All Clients' : mode === 'filter' ? 'Filter by Criteria' : 'Specific Accounts (CSV)'}
                    </label>
                  ))}
                </div>

                {/* Filter mode */}
                {form.audience_mode === 'filter' && (
                  <div className="grid grid-cols-1 md:grid-cols-2 gap-4 pt-2">
                    <div>
                      <label className="block text-xs font-medium text-gray-600 dark:text-gray-400 mb-1">Countries (comma-separated)</label>
                      <input
                        type="text"
                        value={form.countries}
                        onChange={(e) => setForm((f) => ({ ...f, countries: e.target.value }))}
                        className="w-full border border-gray-300 dark:border-gray-600 rounded-md px-3 py-2 text-sm bg-white dark:bg-gray-700 text-gray-800 dark:text-gray-100"
                        placeholder="e.g. ZA, NG, KE"
                      />
                    </div>
                    <div>
                      <label className="block text-xs font-medium text-gray-600 dark:text-gray-400 mb-1">Languages (comma-separated)</label>
                      <input
                        type="text"
                        value={form.languages}
                        onChange={(e) => setForm((f) => ({ ...f, languages: e.target.value }))}
                        className="w-full border border-gray-300 dark:border-gray-600 rounded-md px-3 py-2 text-sm bg-white dark:bg-gray-700 text-gray-800 dark:text-gray-100"
                        placeholder="e.g. English, Arabic"
                      />
                    </div>
                    <div>
                      <label className="block text-xs font-medium text-gray-600 dark:text-gray-400 mb-1">Balance Min</label>
                      <input
                        type="number"
                        value={form.balance_min}
                        onChange={(e) => setForm((f) => ({ ...f, balance_min: e.target.value }))}
                        className="w-full border border-gray-300 dark:border-gray-600 rounded-md px-3 py-2 text-sm bg-white dark:bg-gray-700 text-gray-800 dark:text-gray-100"
                        placeholder="0"
                      />
                    </div>
                    <div>
                      <label className="block text-xs font-medium text-gray-600 dark:text-gray-400 mb-1">Balance Max</label>
                      <input
                        type="number"
                        value={form.balance_max}
                        onChange={(e) => setForm((f) => ({ ...f, balance_max: e.target.value }))}
                        className="w-full border border-gray-300 dark:border-gray-600 rounded-md px-3 py-2 text-sm bg-white dark:bg-gray-700 text-gray-800 dark:text-gray-100"
                        placeholder="100000"
                      />
                    </div>
                    <div>
                      <label className="block text-xs font-medium text-gray-600 dark:text-gray-400 mb-1">Last Trade Before</label>
                      <input
                        type="date"
                        value={form.last_trade_before}
                        onChange={(e) => setForm((f) => ({ ...f, last_trade_before: e.target.value }))}
                        className="w-full border border-gray-300 dark:border-gray-600 rounded-md px-3 py-2 text-sm bg-white dark:bg-gray-700 text-gray-800 dark:text-gray-100"
                      />
                    </div>
                    <div>
                      <label className="block text-xs font-medium text-gray-600 dark:text-gray-400 mb-1">Qualified Before</label>
                      <input
                        type="date"
                        value={form.qualified_before}
                        onChange={(e) => setForm((f) => ({ ...f, qualified_before: e.target.value }))}
                        className="w-full border border-gray-300 dark:border-gray-600 rounded-md px-3 py-2 text-sm bg-white dark:bg-gray-700 text-gray-800 dark:text-gray-100"
                      />
                    </div>
                  </div>
                )}

                {/* CSV mode */}
                {form.audience_mode === 'csv' && (
                  <div className="pt-2 space-y-2">
                    <label className="block text-xs font-medium text-gray-600 dark:text-gray-400">
                      Upload CSV file — one account ID (customer) per row
                    </label>
                    <input
                      type="file"
                      accept=".csv,.txt"
                      onChange={(e) => {
                        const file = e.target.files?.[0];
                        if (!file) return;
                        const reader = new FileReader();
                        reader.onload = (ev) => {
                          const text = ev.target?.result as string;
                          const ids = parseCsvAccountIds(text);
                          setForm((f) => ({ ...f, account_ids: ids }));
                        };
                        reader.readAsText(file);
                      }}
                      className="block text-sm text-gray-600 dark:text-gray-400 file:mr-3 file:py-1.5 file:px-3 file:rounded-md file:border-0 file:text-sm file:font-medium file:bg-blue-50 file:text-blue-700 dark:file:bg-blue-900/30 dark:file:text-blue-300 hover:file:bg-blue-100"
                    />
                    {form.account_ids.length > 0 && (
                      <p className="text-xs text-green-600 dark:text-green-400 font-medium">
                        {form.account_ids.length.toLocaleString()} account{form.account_ids.length !== 1 ? 's' : ''} loaded
                      </p>
                    )}
                  </div>
                )}
              </div>

              {/* Tiers */}
              <div className="space-y-3">
                <div className="flex items-center justify-between">
                  <h4 className="text-sm font-semibold text-gray-700 dark:text-gray-300">Tiers</h4>
                  <button
                    type="button"
                    onClick={addTier}
                    className="px-3 py-1 bg-gray-100 dark:bg-gray-700 hover:bg-gray-200 dark:hover:bg-gray-600 text-gray-700 dark:text-gray-300 text-xs font-medium rounded-md transition-colors"
                  >
                    + Add Tier
                  </button>
                </div>

                {form.tiers.map((tier, idx) => (
                  <div key={idx} className="flex items-end gap-3 p-3 bg-gray-50 dark:bg-gray-700/50 rounded-lg border border-gray-200 dark:border-gray-600">
                    <div className="flex-1">
                      <label className="block text-xs font-medium text-gray-600 dark:text-gray-400 mb-1">Tier Name</label>
                      <input
                        type="text"
                        value={tier.name}
                        onChange={(e) => updateTier(idx, 'name', e.target.value)}
                        className="w-full border border-gray-300 dark:border-gray-600 rounded-md px-3 py-2 text-sm bg-white dark:bg-gray-700 text-gray-800 dark:text-gray-100"
                        placeholder={`Tier ${idx + 1}`}
                      />
                    </div>
                    <div className={form.type === 'instrument' ? 'flex-1' : 'w-36'}>
                      <label className="block text-xs font-medium text-gray-600 dark:text-gray-400 mb-1">
                        {form.type === 'instrument' ? 'Symbol' :
                         form.type === 'streak' ? 'Day Number' :
                         form.type === 'pnl' ? 'Profit Threshold ($)' :
                         form.type === 'diversity' ? 'Asset Classes Required' :
                         form.type === 'trade' ? 'Target (trades)' : 'Target (volume)'}
                      </label>
                      {form.type === 'instrument' ? (
                        <div className="flex items-center gap-1">
                          <select
                            value={tier.symbol}
                            onChange={e => {
                              const updated = [...form.tiers];
                              updated[idx] = { ...updated[idx], symbol: e.target.value };
                              setForm(f => ({ ...f, tiers: updated }));
                            }}
                            className="flex-1 rounded border border-gray-300 dark:border-gray-600 bg-white dark:bg-gray-800 text-sm px-2 py-1"
                          >
                            <option value="">Select symbol…</option>
                            {symbolMappings.map(s => (
                              <option key={s.symbol} value={s.symbol}>{s.symbol}</option>
                            ))}
                          </select>
                          <button
                            type="button"
                            title="Add new symbol"
                            onClick={() => setAddSymbolModal({ tierIdx: idx })}
                            className="rounded bg-indigo-600 text-white px-2 py-1 text-sm hover:bg-indigo-700"
                          >+</button>
                        </div>
                      ) : (
                        <input
                          type="number"
                          value={tier.targetvalue}
                          onChange={(e) => updateTier(idx, 'targetvalue', e.target.value)}
                          className="w-full border border-gray-300 dark:border-gray-600 rounded-md px-3 py-2 text-sm bg-white dark:bg-gray-700 text-gray-800 dark:text-gray-100"
                          placeholder="0"
                          min="0"
                          step="any"
                        />
                      )}
                    </div>
                    <div className="w-36">
                      <label className="block text-xs font-medium text-gray-600 dark:text-gray-400 mb-1">Reward ($)</label>
                      <input
                        type="number"
                        value={tier.rewardamount}
                        onChange={(e) => updateTier(idx, 'rewardamount', e.target.value)}
                        className="w-full border border-gray-300 dark:border-gray-600 rounded-md px-3 py-2 text-sm bg-white dark:bg-gray-700 text-gray-800 dark:text-gray-100"
                        placeholder="0"
                        min="0"
                        step="any"
                      />
                    </div>
                    {form.tiers.length > 1 && (
                      <button
                        type="button"
                        onClick={() => removeTier(idx)}
                        className="px-2 py-2 text-red-500 hover:text-red-700 dark:text-red-400 dark:hover:text-red-300 text-sm"
                        title="Remove tier"
                      >
                        X
                      </button>
                    )}
                  </div>
                ))}
              </div>

              {/* Submit buttons */}
              <div className="flex justify-end gap-3 pt-2">
                <button
                  onClick={() => {
                    setShowForm(false);
                    setFormError('');
                  }}
                  className="px-4 py-2 border border-gray-300 dark:border-gray-600 text-gray-700 dark:text-gray-300 text-sm rounded-lg hover:bg-gray-50 dark:hover:bg-gray-700 transition-colors"
                >
                  Cancel
                </button>
                <button
                  onClick={handleSubmit}
                  disabled={saving}
                  className="px-4 py-2 bg-blue-600 hover:bg-blue-700 disabled:opacity-50 text-white text-sm font-medium rounded-lg transition-colors"
                >
                  {saving ? 'Saving...' : 'Save Challenge'}
                </button>
              </div>
            </div>
          )}

          {/* Challenge list */}
          {loading ? (
            <div className="text-center py-12 text-gray-500 dark:text-gray-400">Loading challenges...</div>
          ) : groups.length === 0 ? (
            <div className="bg-white dark:bg-gray-800 rounded-lg shadow border border-gray-200 dark:border-gray-700 p-12 text-center">
              <div className="text-gray-400 dark:text-gray-500 text-4xl mb-3">--</div>
              <p className="text-gray-500 dark:text-gray-400 text-sm">No challenges created yet. Click "Create New Challenge" to get started.</p>
            </div>
          ) : (
            <div className="bg-white dark:bg-gray-800 rounded-lg shadow border border-gray-200 dark:border-gray-700 overflow-hidden">
              <table className="min-w-full divide-y divide-gray-200 dark:divide-gray-700">
                <thead className="bg-gray-50 dark:bg-gray-700/50">
                  <tr>
                    <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider">
                      Group Name
                    </th>
                    <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider">
                      Type
                    </th>
                    <th className="px-6 py-3 text-center text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider">
                      Tiers
                    </th>
                    <th className="px-6 py-3 text-center text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider">
                      Status
                    </th>
                    <th className="px-6 py-3 text-center text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider">
                      Created
                    </th>
                    <th className="px-6 py-3 text-center text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider">
                      Actions
                    </th>
                  </tr>
                </thead>
                <tbody className="divide-y divide-gray-200 dark:divide-gray-700">
                  {groups.map((g) => {
                    const isExpanded = expanded.has(g.group_name);
                    return (
                      <TableGroupRow
                        key={g.group_name}
                        group={g}
                        isExpanded={isExpanded}
                        onToggleExpand={() => toggleExpand(g.group_name)}
                        onToggleActive={() => handleToggle(g.group_name)}
                        onDelete={() => handleDelete(g.group_name)}
                      />
                    );
                  })}
                </tbody>
              </table>
            </div>
          )}
        </>
      )}

      {/* ============================================================ */}
      {/* TAB 2: Client Progress (CLAUD-90) */}
      {/* ============================================================ */}
      {activeTab === 'progress' && (
        <>
          {/* Header */}
          <div>
            <h2 className="text-xl font-semibold text-gray-800 dark:text-gray-100">Client Progress</h2>
            <p className="text-sm text-gray-500 dark:text-gray-400 mt-1">
              Track client challenge participation and reward status.
            </p>
          </div>

          {/* Filters */}
          <div className="flex flex-wrap items-end gap-4">
            <div>
              <label className="block text-xs font-medium text-gray-600 dark:text-gray-400 mb-1">Date</label>
              <input
                type="date"
                value={progressDate}
                onChange={(e) => { setProgressDate(e.target.value); setProgressPage(1); }}
                className="border border-gray-300 dark:border-gray-600 rounded-md px-3 py-2 text-sm bg-white dark:bg-gray-700 text-gray-800 dark:text-gray-100 focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
              />
            </div>
            <div>
              <label className="block text-xs font-medium text-gray-600 dark:text-gray-400 mb-1">Challenge Group</label>
              <select
                value={progressGroup}
                onChange={(e) => { setProgressGroup(e.target.value); setProgressPage(1); }}
                className="border border-gray-300 dark:border-gray-600 rounded-md px-3 py-2 text-sm bg-white dark:bg-gray-700 text-gray-800 dark:text-gray-100 focus:ring-2 focus:ring-blue-500 focus:border-blue-500 min-w-[200px]"
              >
                <option value="">All Groups</option>
                {groups.map((g) => (
                  <option key={g.group_name} value={g.group_name}>{g.group_name}</option>
                ))}
              </select>
            </div>
            <div className="text-sm text-gray-500 dark:text-gray-400 py-2">
              {progressTotal} record{progressTotal !== 1 ? 's' : ''} found
            </div>
          </div>



          {/* Progress error */}
          {progressError && (
            <div className="bg-red-50 dark:bg-red-900/30 border border-red-200 dark:border-red-700 rounded-lg p-4 text-red-700 dark:text-red-300 text-sm">
              {progressError}
              <button onClick={() => setProgressError('')} className="ml-3 underline text-xs">dismiss</button>
            </div>
          )}

          {/* Progress table */}
          {progressLoading ? (
            <div className="text-center py-12 text-gray-500 dark:text-gray-400">Loading progress...</div>
          ) : progressItems.length === 0 ? (
            <div className="bg-white dark:bg-gray-800 rounded-lg shadow border border-gray-200 dark:border-gray-700 p-12 text-center">
              <div className="text-gray-400 dark:text-gray-500 text-4xl mb-3">--</div>
              <p className="text-gray-500 dark:text-gray-400 text-sm">No progress records found for the selected filters.</p>
            </div>
          ) : (
            <div className="bg-white dark:bg-gray-800 rounded-lg shadow border border-gray-200 dark:border-gray-700 overflow-hidden">
              <table className="min-w-full divide-y divide-gray-200 dark:divide-gray-700">
                <thead className="bg-gray-50 dark:bg-gray-700/50">
                  <tr>
                    <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider">
                      Account ID
                    </th>
                    <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider">
                      Challenge
                    </th>
                    <th className="px-6 py-3 text-center text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider">
                      Progress
                    </th>
                    <th className="px-6 py-3 text-right text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider">
                      Total Reward
                    </th>
                    <th className="px-6 py-3 text-center text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider">
                      Status
                    </th>
                    <th className="px-6 py-3 text-center text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider">
                      Date
                    </th>
                  </tr>
                </thead>
                <tbody className="divide-y divide-gray-200 dark:divide-gray-700">
                  {progressItems.map((item, idx) => (
                    <tr key={`${item.accountid}-${item.group_name}-${item.date}-${idx}`} className="hover:bg-gray-50 dark:hover:bg-gray-700/30 transition-colors">
                      <td className="px-6 py-4 text-sm font-medium text-gray-800 dark:text-gray-100 tabular-nums">
                        {item.accountid || '-'}
                      </td>
                      <td className="px-6 py-4 text-sm text-gray-600 dark:text-gray-300">
                        {item.group_name}
                      </td>
                      <td className="px-6 py-4 text-sm text-center text-gray-600 dark:text-gray-300 tabular-nums">
                        {item.challenge_type === 'streak'
                          ? `Day ${item.progress_value}`
                          : item.challenge_type === 'pnl'
                          ? `$${item.progress_value.toFixed(2)} profit`
                          : item.challenge_type === 'diversity'
                          ? `${Math.floor(item.progress_value)} asset class${Math.floor(item.progress_value) !== 1 ? 'es' : ''}`
                          : item.challenge_type === 'instrument'
                          ? `${Math.floor(item.progress_value)} symbol${Math.floor(item.progress_value) !== 1 ? 's' : ''} rewarded`
                          : `${item.last_rewarded_tier} / ${item.total_tiers} tiers`}
                      </td>
                      <td className="px-6 py-4 text-sm text-right font-medium text-green-700 dark:text-green-400 tabular-nums">
                        ${item.total_reward.toFixed(2)}
                      </td>
                      <td className="px-6 py-4 text-center">
                        <span className={`inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium ${statusBadge(item.status)}`}>
                          {item.status}
                        </span>
                      </td>
                      <td className="px-6 py-4 text-sm text-center text-gray-500 dark:text-gray-400 tabular-nums">
                        {item.date}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>

              {/* Pagination */}
              {totalPages > 1 && (
                <div className="flex items-center justify-between px-6 py-3 border-t border-gray-200 dark:border-gray-700 bg-gray-50 dark:bg-gray-700/30">
                  <div className="text-sm text-gray-500 dark:text-gray-400">
                    Page {progressPage} of {totalPages}
                  </div>
                  <div className="flex gap-2">
                    <button
                      onClick={() => setProgressPage((p) => Math.max(1, p - 1))}
                      disabled={progressPage <= 1}
                      className="px-3 py-1.5 text-sm border border-gray-300 dark:border-gray-600 rounded-md text-gray-700 dark:text-gray-300 hover:bg-gray-100 dark:hover:bg-gray-600 disabled:opacity-50 disabled:cursor-not-allowed transition-colors"
                    >
                      Previous
                    </button>
                    <button
                      onClick={() => setProgressPage((p) => Math.min(totalPages, p + 1))}
                      disabled={progressPage >= totalPages}
                      className="px-3 py-1.5 text-sm border border-gray-300 dark:border-gray-600 rounded-md text-gray-700 dark:text-gray-300 hover:bg-gray-100 dark:hover:bg-gray-600 disabled:opacity-50 disabled:cursor-not-allowed transition-colors"
                    >
                      Next
                    </button>
                  </div>
                </div>
              )}
            </div>
          )}
        </>
      )}

      {/* ============================================================ */}
      {/* TAB 3: Events Log (CLAUD-91) */}
      {/* ============================================================ */}
      {activeTab === 'events' && (
        <>
          {/* Header */}
          <div>
            <h2 className="text-xl font-semibold text-gray-800 dark:text-gray-100">Optimove Events Log</h2>
            <p className="text-sm text-gray-500 dark:text-gray-400 mt-1">
              Track Optimove event notifications sent for challenge activity.
            </p>
          </div>

          {/* Filters */}
          <div className="flex flex-wrap items-end gap-4">
            <div>
              <label className="block text-xs font-medium text-gray-600 dark:text-gray-400 mb-1">Date</label>
              <input
                type="date"
                value={eventsDate}
                onChange={(e) => { setEventsDate(e.target.value); setEventsPage(1); }}
                className="border border-gray-300 dark:border-gray-600 rounded-md px-3 py-2 text-sm bg-white dark:bg-gray-700 text-gray-800 dark:text-gray-100 focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
              />
            </div>
            <div>
              <label className="block text-xs font-medium text-gray-600 dark:text-gray-400 mb-1">Event Type</label>
              <select
                value={eventsEventName}
                onChange={(e) => { setEventsEventName(e.target.value); setEventsPage(1); }}
                className="border border-gray-300 dark:border-gray-600 rounded-md px-3 py-2 text-sm bg-white dark:bg-gray-700 text-gray-800 dark:text-gray-100 focus:ring-2 focus:ring-blue-500 focus:border-blue-500 min-w-[200px]"
              >
                <option value="">All Events</option>
                <option value="challenge_started">challenge_started</option>
                <option value="challenge_started_live">challenge_started_live</option>
                <option value="challenge_completed">challenge_completed</option>
              </select>
            </div>
            <div className="text-sm text-gray-500 dark:text-gray-400 py-2">
              {eventsTotal} record{eventsTotal !== 1 ? 's' : ''} found
            </div>
          </div>

          {/* Events error */}
          {eventsError && (
            <div className="bg-red-50 dark:bg-red-900/30 border border-red-200 dark:border-red-700 rounded-lg p-4 text-red-700 dark:text-red-300 text-sm">
              {eventsError}
              <button onClick={() => setEventsError('')} className="ml-3 underline text-xs">dismiss</button>
            </div>
          )}

          {/* Events table */}
          {eventsLoading ? (
            <div className="text-center py-12 text-gray-500 dark:text-gray-400">Loading events...</div>
          ) : eventsItems.length === 0 ? (
            <div className="bg-white dark:bg-gray-800 rounded-lg shadow border border-gray-200 dark:border-gray-700 p-12 text-center">
              <div className="text-gray-400 dark:text-gray-500 text-4xl mb-3">--</div>
              <p className="text-gray-500 dark:text-gray-400 text-sm">No events found for the selected filters.</p>
            </div>
          ) : (
            <div className="bg-white dark:bg-gray-800 rounded-lg shadow border border-gray-200 dark:border-gray-700 overflow-hidden">
              <table className="min-w-full divide-y divide-gray-200 dark:divide-gray-700">
                <thead className="bg-gray-50 dark:bg-gray-700/50">
                  <tr>
                    <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider">
                      Account ID
                    </th>
                    <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider">
                      Event
                    </th>
                    <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider">
                      Challenge
                    </th>
                    <th className="px-6 py-3 text-center text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider">
                      Timestamp
                    </th>
                    <th className="px-6 py-3 text-center text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider">
                      Success
                    </th>
                    <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider">
                      Response
                    </th>
                  </tr>
                </thead>
                <tbody className="divide-y divide-gray-200 dark:divide-gray-700">
                  {eventsItems.map((item) => {
                    const eventBadgeColor =
                      item.event_name === 'challenge_started'
                        ? 'bg-blue-100 dark:bg-blue-900/40 text-blue-800 dark:text-blue-300'
                        : item.event_name === 'challenge_started_live'
                        ? 'bg-purple-100 dark:bg-purple-900/40 text-purple-800 dark:text-purple-300'
                        : item.event_name === 'challenge_completed'
                        ? 'bg-green-100 dark:bg-green-900/40 text-green-800 dark:text-green-300'
                        : 'bg-gray-100 dark:bg-gray-600 text-gray-600 dark:text-gray-300';

                    const ts = item.created_at
                      ? new Date(item.created_at).toLocaleDateString('en-GB', {
                          day: '2-digit',
                          month: '2-digit',
                          year: 'numeric',
                          hour: '2-digit',
                          minute: '2-digit',
                          hour12: false,
                        })
                      : '-';

                    const responseTruncated = item.response && item.response.length > 80
                      ? item.response.slice(0, 80) + '...'
                      : item.response || '';

                    return (
                      <tr key={item.id} className="hover:bg-gray-50 dark:hover:bg-gray-700/30 transition-colors">
                        <td className="px-6 py-4 text-sm font-medium text-gray-800 dark:text-gray-100 tabular-nums">
                          {item.accountid || '-'}
                        </td>
                        <td className="px-6 py-4">
                          <span className={`inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-mono font-medium ${eventBadgeColor}`}>
                            {item.event_name}
                          </span>
                        </td>
                        <td className="px-6 py-4 text-sm text-gray-600 dark:text-gray-300">
                          {item.group_name || '-'}
                        </td>
                        <td className="px-6 py-4 text-sm text-center text-gray-500 dark:text-gray-400 tabular-nums">
                          {ts}
                        </td>
                        <td className="px-6 py-4 text-center">
                          {item.success ? (
                            <span className="inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium bg-green-100 dark:bg-green-900/40 text-green-800 dark:text-green-300">
                              OK
                            </span>
                          ) : (
                            <span className="inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium bg-red-100 dark:bg-red-900/40 text-red-800 dark:text-red-300">
                              Failed
                            </span>
                          )}
                        </td>
                        <td
                          className="px-6 py-4 text-sm text-gray-500 dark:text-gray-400 max-w-[300px] truncate"
                          title={item.response || ''}
                        >
                          {responseTruncated || '-'}
                        </td>
                      </tr>
                    );
                  })}
                </tbody>
              </table>

              {/* Pagination */}
              {eventsTotalPages > 1 && (
                <div className="flex items-center justify-between px-6 py-3 border-t border-gray-200 dark:border-gray-700 bg-gray-50 dark:bg-gray-700/30">
                  <div className="text-sm text-gray-500 dark:text-gray-400">
                    Page {eventsPage} of {eventsTotalPages}
                  </div>
                  <div className="flex gap-2">
                    <button
                      onClick={() => setEventsPage((p) => Math.max(1, p - 1))}
                      disabled={eventsPage <= 1}
                      className="px-3 py-1.5 text-sm border border-gray-300 dark:border-gray-600 rounded-md text-gray-700 dark:text-gray-300 hover:bg-gray-100 dark:hover:bg-gray-600 disabled:opacity-50 disabled:cursor-not-allowed transition-colors"
                    >
                      Previous
                    </button>
                    <button
                      onClick={() => setEventsPage((p) => Math.min(eventsTotalPages, p + 1))}
                      disabled={eventsPage >= eventsTotalPages}
                      className="px-3 py-1.5 text-sm border border-gray-300 dark:border-gray-600 rounded-md text-gray-700 dark:text-gray-300 hover:bg-gray-100 dark:hover:bg-gray-600 disabled:opacity-50 disabled:cursor-not-allowed transition-colors"
                    >
                      Next
                    </button>
                  </div>
                </div>
              )}
            </div>
          )}
        </>
      )}

      {/* ============================================================ */}
      {/* TAB 4: Symbol Mapping (CLAUD-94) */}
      {/* ============================================================ */}
      {/* Add Symbol modal (instrument challenge form) */}
      {addSymbolModal !== null && (
        <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/40">
          <div className="bg-white dark:bg-gray-900 rounded-xl shadow-xl p-6 w-80">
            <h3 className="text-base font-semibold mb-4">Add New Symbol</h3>
            <div className="space-y-3">
              <div>
                <label className="block text-xs text-gray-500 mb-1">Symbol</label>
                <input
                  autoFocus
                  type="text"
                  value={newSymbolForm.symbol}
                  onChange={e => setNewSymbolForm(f => ({ ...f, symbol: e.target.value.toUpperCase() }))}
                  placeholder="e.g. BTCUSD"
                  className="w-full rounded border border-gray-300 dark:border-gray-600 bg-white dark:bg-gray-800 px-3 py-1.5 text-sm"
                />
              </div>
              <div>
                <label className="block text-xs text-gray-500 mb-1">Asset Class</label>
                <select
                  value={newSymbolForm.asset_class}
                  onChange={e => setNewSymbolForm(f => ({ ...f, asset_class: e.target.value as SymbolMapping['asset_class'] }))}
                  className="w-full rounded border border-gray-300 dark:border-gray-600 bg-white dark:bg-gray-800 px-3 py-1.5 text-sm"
                >
                  <option value="forex">Forex</option>
                  <option value="commodity">Commodity</option>
                  <option value="index">Index</option>
                  <option value="crypto">Crypto</option>
                  <option value="stock">Stock</option>
                </select>
              </div>
            </div>
            <div className="flex gap-2 mt-4">
              <button
                type="button"
                disabled={!newSymbolForm.symbol || addSymbolLoading}
                onClick={async () => {
                  if (!newSymbolForm.symbol) return;
                  setAddSymbolLoading(true);
                  try {
                    await upsertSymbolMapping({ symbol: newSymbolForm.symbol, asset_class: newSymbolForm.asset_class });
                    const updated = await getSymbolMappings();
                    setSymbolMappings(updated);
                    // Auto-select the new symbol in the tier
                    const tierIdx = addSymbolModal.tierIdx;
                    setForm(f => {
                      const tiers = [...f.tiers];
                      tiers[tierIdx] = { ...tiers[tierIdx], symbol: newSymbolForm.symbol };
                      return { ...f, tiers };
                    });
                    setAddSymbolModal(null);
                    setNewSymbolForm({ symbol: '', asset_class: 'forex' });
                  } catch (e: unknown) {
                    alert(e instanceof Error ? e.message : 'Failed to add symbol');
                  } finally {
                    setAddSymbolLoading(false);
                  }
                }}
                className="flex-1 rounded bg-indigo-600 text-white py-1.5 text-sm font-medium hover:bg-indigo-700 disabled:opacity-50"
              >
                {addSymbolLoading ? 'Saving…' : 'Add Symbol'}
              </button>
              <button
                type="button"
                onClick={() => { setAddSymbolModal(null); setNewSymbolForm({ symbol: '', asset_class: 'forex' }); }}
                className="flex-1 rounded border border-gray-300 dark:border-gray-600 py-1.5 text-sm hover:bg-gray-50 dark:hover:bg-gray-800"
              >Cancel</button>
            </div>
          </div>
        </div>
      )}

      {activeTab === 'symbols' && (
        <>
          <div>
            <h2 className="text-xl font-semibold text-gray-800 dark:text-gray-100">Symbol Asset Class Mapping</h2>
            <p className="text-sm text-gray-500 dark:text-gray-400 mt-1">
              Map trading symbols to asset classes for Instrument Diversity challenges.
            </p>
          </div>

          {/* Add new mapping form */}
          <div className="bg-white dark:bg-gray-800 rounded-lg shadow border border-gray-200 dark:border-gray-700 p-4">
            <h3 className="text-sm font-semibold text-gray-700 dark:text-gray-300 mb-3">Add / Update Mapping</h3>
            <div className="flex flex-wrap items-end gap-3">
              <div>
                <label className="block text-xs font-medium text-gray-600 dark:text-gray-400 mb-1">Symbol</label>
                <input
                  type="text"
                  value={newSymbol}
                  onChange={(e) => setNewSymbol(e.target.value.toUpperCase())}
                  className="border border-gray-300 dark:border-gray-600 rounded-md px-3 py-2 text-sm bg-white dark:bg-gray-700 text-gray-800 dark:text-gray-100 w-36 focus:ring-2 focus:ring-blue-500 focus:border-blue-500 uppercase"
                  placeholder="EURUSD"
                />
              </div>
              <div>
                <label className="block text-xs font-medium text-gray-600 dark:text-gray-400 mb-1">Asset Class</label>
                <select
                  value={newAssetClass}
                  onChange={(e) => setNewAssetClass(e.target.value as SymbolMapping['asset_class'])}
                  className="border border-gray-300 dark:border-gray-600 rounded-md px-3 py-2 text-sm bg-white dark:bg-gray-700 text-gray-800 dark:text-gray-100 focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
                >
                  <option value="forex">Forex</option>
                  <option value="commodity">Commodity</option>
                  <option value="index">Index</option>
                  <option value="crypto">Crypto</option>
                  <option value="stock">Stock</option>
                </select>
              </div>
              <button
                onClick={async () => {
                  if (!newSymbol.trim()) return;
                  setSymbolSaving(true);
                  setSymbolsError('');
                  try {
                    await upsertSymbolMapping({ symbol: newSymbol.trim(), asset_class: newAssetClass });
                    setNewSymbol('');
                    await loadSymbols();
                  } catch (e) {
                    setSymbolsError(e instanceof Error ? e.message : 'Failed to save');
                  } finally {
                    setSymbolSaving(false);
                  }
                }}
                disabled={symbolSaving || !newSymbol.trim()}
                className="px-4 py-2 bg-blue-600 hover:bg-blue-700 disabled:opacity-50 text-white text-sm font-medium rounded-lg transition-colors"
              >
                {symbolSaving ? 'Saving...' : 'Save'}
              </button>
            </div>
            {symbolsError && (
              <p className="mt-2 text-xs text-red-600 dark:text-red-400">{symbolsError}</p>
            )}
          </div>

          {/* Symbol table */}
          {symbolsLoading ? (
            <div className="text-center py-12 text-gray-500 dark:text-gray-400">Loading...</div>
          ) : (
            <div className="bg-white dark:bg-gray-800 rounded-lg shadow border border-gray-200 dark:border-gray-700 overflow-hidden">
              <table className="min-w-full divide-y divide-gray-200 dark:divide-gray-700">
                <thead className="bg-gray-50 dark:bg-gray-700/50">
                  <tr>
                    <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider">Symbol</th>
                    <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider">Asset Class</th>
                    <th className="px-6 py-3 text-center text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider">Actions</th>
                  </tr>
                </thead>
                <tbody className="divide-y divide-gray-200 dark:divide-gray-700">
                  {symbols.length === 0 ? (
                    <tr>
                      <td colSpan={3} className="px-6 py-8 text-center text-gray-500 dark:text-gray-400 text-sm">
                        No symbol mappings yet.
                      </td>
                    </tr>
                  ) : symbols.map((sym) => (
                    <tr key={sym.symbol} className="hover:bg-gray-50 dark:hover:bg-gray-700/30 transition-colors">
                      <td className="px-6 py-3 text-sm font-mono font-medium text-gray-800 dark:text-gray-100">{sym.symbol}</td>
                      <td className="px-6 py-3 text-sm text-gray-600 dark:text-gray-300">
                        <AssetClassBadge assetClass={sym.asset_class} />
                      </td>
                      <td className="px-6 py-3 text-center">
                        <button
                          onClick={async () => {
                            try {
                              await deleteSymbolMapping(sym.symbol);
                              await loadSymbols();
                            } catch (e) {
                              setSymbolsError(e instanceof Error ? e.message : 'Failed to delete');
                            }
                          }}
                          className="text-red-500 hover:text-red-700 dark:text-red-400 dark:hover:text-red-300 text-sm font-medium transition-colors"
                        >
                          Delete
                        </button>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
              <div className="px-6 py-2 border-t border-gray-200 dark:border-gray-700 bg-gray-50 dark:bg-gray-700/30 text-xs text-gray-400">
                {symbols.length} mapping{symbols.length !== 1 ? 's' : ''}
              </div>
            </div>
          )}
        </>
      )}
    </div>
  );
}

// ---------------------------------------------------------------------------
// Sub-component: table row for a challenge group
// ---------------------------------------------------------------------------

function TableGroupRow({
  group,
  isExpanded,
  onToggleExpand,
  onToggleActive,
  onDelete,
}: {
  group: ChallengeGroup;
  isExpanded: boolean;
  onToggleExpand: () => void;
  onToggleActive: () => void;
  onDelete: () => void;
}) {
  const isActive = group.isactive === 1;
  const dateStr = group.InsertDate
    ? new Date(group.InsertDate).toLocaleDateString('en-GB', {
        day: '2-digit',
        month: '2-digit',
        year: 'numeric',
      })
    : '-';

  return (
    <>
      {/* Main row */}
      <tr
        className="hover:bg-gray-50 dark:hover:bg-gray-700/30 cursor-pointer transition-colors"
        onClick={onToggleExpand}
      >
        <td className="px-6 py-4 text-sm font-medium text-gray-800 dark:text-gray-100">
          <span className="mr-2 text-gray-400 dark:text-gray-500 text-xs">
            {isExpanded ? 'v' : '>'}
          </span>
          {group.group_name}
          {group.audience_criteria?.account_ids && group.audience_criteria.account_ids.length > 0 && (
            <span className="ml-2 text-xs text-purple-600 dark:text-purple-400 font-medium">
              {group.audience_criteria.account_ids.length} accounts
            </span>
          )}
        </td>
        <td className="px-6 py-4 text-sm text-gray-600 dark:text-gray-300">
          <span className={`inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium ${
            group.type === 'streak'
              ? 'bg-orange-100 dark:bg-orange-900/40 text-orange-800 dark:text-orange-300'
              : group.type === 'pnl'
              ? 'bg-emerald-100 dark:bg-emerald-900/40 text-emerald-800 dark:text-emerald-300'
              : group.type === 'diversity'
              ? 'bg-indigo-100 dark:bg-indigo-900/40 text-indigo-800 dark:text-indigo-300'
              : group.type === 'instrument'
              ? 'bg-teal-100 dark:bg-teal-900/40 text-teal-800 dark:text-teal-300'
              : 'bg-blue-100 dark:bg-blue-900/40 text-blue-800 dark:text-blue-300'
          }`}>
            {group.type === 'trade' ? 'Count of Trades' :
             group.type === 'volume' ? 'Sum of Volume' :
             group.type === 'streak' ? 'Streak: Trading Days' :
             group.type === 'pnl' ? 'Cumulative P&L' :
             group.type === 'instrument' ? 'Instrument' :
             'Instrument Diversity'}
          </span>
          {group.type === 'instrument' && group.tiers.some(t => t.symbol) && (
            <span className="ml-1 text-xs text-gray-500 dark:text-gray-400">
              {group.tiers.map(t => t.symbol).filter(Boolean).join(' / ')}
            </span>
          )}
          {group.valid_until && (
            <FlashBadge validUntil={group.valid_until} />
          )}
        </td>
        <td className="px-6 py-4 text-sm text-center text-gray-600 dark:text-gray-300">
          {group.tiers.length}
        </td>
        <td className="px-6 py-4 text-center">
          <button
            onClick={(e) => {
              e.stopPropagation();
              onToggleActive();
            }}
            className={`inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium transition-colors ${
              isActive
                ? 'bg-green-100 dark:bg-green-900/40 text-green-800 dark:text-green-300 hover:bg-green-200 dark:hover:bg-green-900/60'
                : 'bg-gray-100 dark:bg-gray-600 text-gray-600 dark:text-gray-400 hover:bg-gray-200 dark:hover:bg-gray-500'
            }`}
          >
            {isActive ? 'Active' : 'Inactive'}
          </button>
        </td>
        <td className="px-6 py-4 text-sm text-center text-gray-500 dark:text-gray-400">
          <div>{dateStr}</div>
          {group.expires_on && <ExpiryBadge expiresOn={group.expires_on} />}
        </td>
        <td className="px-6 py-4 text-center">
          <button
            onClick={(e) => {
              e.stopPropagation();
              onDelete();
            }}
            className="text-red-500 hover:text-red-700 dark:text-red-400 dark:hover:text-red-300 text-sm font-medium transition-colors"
          >
            Delete
          </button>
        </td>
      </tr>

      {/* Expanded tier details */}
      {isExpanded && (
        <tr>
          <td colSpan={6} className="px-6 py-3 bg-gray-50 dark:bg-gray-700/20">
            <div className="space-y-1">
              <div className="text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider mb-2">
                Tier Details
              </div>
              <table className="w-full text-sm">
                <thead>
                  <tr className="text-xs text-gray-500 dark:text-gray-400 uppercase">
                    <th className="text-left py-1 pr-4">#</th>
                    <th className="text-left py-1 pr-4">Tier Name</th>
                    <th className="text-right py-1 pr-4">Target</th>
                    <th className="text-right py-1">Reward ($)</th>
                  </tr>
                </thead>
                <tbody>
                  {group.tiers.map((tier, idx) => (
                    <tr key={tier.challengeId ?? idx} className="text-gray-700 dark:text-gray-300">
                      <td className="py-1 pr-4 text-gray-400 dark:text-gray-500">{idx + 1}</td>
                      <td className="py-1 pr-4 font-medium">
                        {tier.name}
                        {group.type === 'instrument' && tier.symbol && (
                          <span className="ml-1 text-xs font-mono bg-gray-100 dark:bg-gray-700 px-1.5 py-0.5 rounded">
                            {tier.symbol}
                          </span>
                        )}
                      </td>
                      <td className="py-1 pr-4 text-right tabular-nums">
                        {group.type === 'instrument' ? tier.symbol || '-' : tier.targetvalue.toLocaleString()}
                      </td>
                      <td className="py-1 text-right tabular-nums font-medium text-green-700 dark:text-green-400">
                        ${tier.rewardamount.toLocaleString()}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>

              {group.reward_multiplier && group.reward_multiplier > 1 && (
                <div className="mt-2 text-xs text-amber-600 dark:text-amber-400 font-medium">
                  ⚡ {group.reward_multiplier}× reward multiplier applied
                </div>
              )}

              {/* Audience info */}
              {group.audience_criteria && (
                <div className="mt-3 pt-2 border-t border-gray-200 dark:border-gray-600">
                  <div className="text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider mb-1">
                    Audience Criteria
                  </div>
                  <div className="text-xs text-gray-600 dark:text-gray-400">
                    {group.audience_criteria.all_clients ? (
                      <span className="text-green-600 dark:text-green-400">All Clients</span>
                    ) : group.audience_criteria.account_ids ? (
                      <span className="text-purple-600 dark:text-purple-400">
                        Specific Accounts: {group.audience_criteria.account_ids.length.toLocaleString()} IDs
                      </span>
                    ) : (
                      <span>
                        {group.audience_criteria.countries?.length
                          ? `Countries: ${group.audience_criteria.countries.join(', ')} `
                          : ''}
                        {group.audience_criteria.languages?.length
                          ? `| Languages: ${group.audience_criteria.languages.join(', ')} `
                          : ''}
                        {group.audience_criteria.balance_min != null ? `| Min Balance: $${group.audience_criteria.balance_min} ` : ''}
                        {group.audience_criteria.balance_max != null ? `| Max Balance: $${group.audience_criteria.balance_max} ` : ''}
                        {group.audience_criteria.last_trade_before ? `| Last Trade Before: ${group.audience_criteria.last_trade_before} ` : ''}
                        {group.audience_criteria.qualified_before ? `| Qualified Before: ${group.audience_criteria.qualified_before} ` : ''}
                      </span>
                    )}
                  </div>
                </div>
              )}

              {group.expires_on && (
                <div className="mt-2 pt-2 border-t border-gray-200 dark:border-gray-600">
                  <ExpiryBadge expiresOn={group.expires_on} />
                </div>
              )}
            </div>
          </td>
        </tr>
      )}
    </>
  );
}

// ---------------------------------------------------------------------------
// Helper: FlashBadge — shows countdown or "Expired" for flash challenges
// ---------------------------------------------------------------------------

function FlashBadge({ validUntil }: { validUntil: string }) {
  const [countdown, setCountdown] = useState('');

  useEffect(() => {
    const update = () => {
      const diff = new Date(validUntil).getTime() - Date.now();
      if (diff <= 0) {
        setCountdown('Expired');
        return;
      }
      const h = Math.floor(diff / 3600000);
      const m = Math.floor((diff % 3600000) / 60000);
      const s = Math.floor((diff % 60000) / 1000);
      setCountdown(h > 0 ? `${h}h ${m}m` : `${m}m ${s}s`);
    };
    update();
    const interval = setInterval(update, 1000);
    return () => clearInterval(interval);
  }, [validUntil]);

  const isExpired = new Date(validUntil).getTime() <= Date.now();

  return (
    <span className={`ml-1.5 inline-flex items-center px-2 py-0.5 rounded-full text-xs font-medium ${
      isExpired
        ? 'bg-gray-100 dark:bg-gray-600 text-gray-500 dark:text-gray-400'
        : 'bg-amber-100 dark:bg-amber-900/40 text-amber-800 dark:text-amber-300'
    }`}>
      {isExpired ? 'Expired' : `⚡ ${countdown}`}
    </span>
  );
}

// ---------------------------------------------------------------------------
// Helper: AssetClassBadge — colored badge per asset class
// ---------------------------------------------------------------------------

function AssetClassBadge({ assetClass }: { assetClass: string }) {
  const colors: Record<string, string> = {
    forex: 'bg-blue-100 dark:bg-blue-900/40 text-blue-800 dark:text-blue-300',
    commodity: 'bg-yellow-100 dark:bg-yellow-900/40 text-yellow-800 dark:text-yellow-300',
    index: 'bg-purple-100 dark:bg-purple-900/40 text-purple-800 dark:text-purple-300',
    crypto: 'bg-orange-100 dark:bg-orange-900/40 text-orange-800 dark:text-orange-300',
    stock: 'bg-green-100 dark:bg-green-900/40 text-green-800 dark:text-green-300',
  };
  return (
    <span className={`inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium capitalize ${colors[assetClass] || 'bg-gray-100 text-gray-600'}`}>
      {assetClass}
    </span>
  );
}

// ---------------------------------------------------------------------------
// Helper: ExpiryBadge — color-coded expiration date badge
// ---------------------------------------------------------------------------

function ExpiryBadge({ expiresOn }: { expiresOn: string }) {
  const expiry = new Date(expiresOn);
  const today = new Date();
  today.setHours(0, 0, 0, 0);
  const daysLeft = Math.ceil((expiry.getTime() - today.getTime()) / 86400000);

  if (daysLeft < 0) {
    return (
      <span className="inline-flex items-center px-2 py-0.5 rounded-full text-xs font-medium bg-red-100 dark:bg-red-900/40 text-red-700 dark:text-red-300">
        Expired
      </span>
    );
  }
  if (daysLeft === 0) {
    return (
      <span className="inline-flex items-center px-2 py-0.5 rounded-full text-xs font-medium bg-red-100 dark:bg-red-900/40 text-red-700 dark:text-red-300">
        Expires today
      </span>
    );
  }
  if (daysLeft <= 3) {
    return (
      <span className="inline-flex items-center px-2 py-0.5 rounded-full text-xs font-medium bg-amber-100 dark:bg-amber-900/40 text-amber-700 dark:text-amber-300">
        Expires in {daysLeft}d
      </span>
    );
  }
  return (
    <span className="inline-flex items-center px-2 py-0.5 rounded-full text-xs font-medium bg-gray-100 dark:bg-gray-600 text-gray-500 dark:text-gray-400">
      Expires {expiresOn}
    </span>
  );
}
