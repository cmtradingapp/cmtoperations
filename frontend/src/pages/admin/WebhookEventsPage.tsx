import { useEffect, useState } from 'react';
import {
  ACTION_TYPES,
  KNOWN_EVENTS,
  type ActionRule,
  type ActionType,
  type EventLogRow,
  type EventStat,
  createActionRule,
  deleteActionRule,
  fetchActionRules,
  fetchEventLog,
  fetchEventStats,
  updateActionRule,
} from '../../api/webhookEvents';

const TABS = ['Event Log', 'Action Rules', 'Stats'] as const;
type Tab = (typeof TABS)[number];

const ACTION_LABELS: Record<ActionType, string> = {
  log_only: 'Log Only',
  optimove: 'Forward to Optimove',
  challenge: 'Challenge Engine',
  bonus: 'Award Bonus',
};

const ACTION_COLORS: Record<ActionType, string> = {
  log_only: 'bg-gray-100 text-gray-700 dark:bg-gray-700 dark:text-gray-300',
  optimove: 'bg-blue-100 text-blue-700 dark:bg-blue-900/30 dark:text-blue-300',
  challenge: 'bg-purple-100 text-purple-700 dark:bg-purple-900/30 dark:text-purple-300',
  bonus: 'bg-green-100 text-green-700 dark:bg-green-900/30 dark:text-green-300',
};

// ── Event Log Tab ─────────────────────────────────────────────────────────────

function EventLogTab() {
  const [rows, setRows] = useState<EventLogRow[]>([]);
  const [total, setTotal] = useState(0);
  const [page, setPage] = useState(1);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState('');
  const [filterEvent, setFilterEvent] = useState('');
  const [filterCustomer, setFilterCustomer] = useState('');
  const [expanded, setExpanded] = useState<number | null>(null);
  const PAGE_SIZE = 50;

  const load = (p: number, ev: string, cust: string) => {
    setLoading(true);
    setError('');
    fetchEventLog({ event_name: ev || undefined, customer: cust || undefined, page: p, page_size: PAGE_SIZE })
      .then((res) => { setRows(res.rows); setTotal(res.total); })
      .catch((e) => setError(e.message))
      .finally(() => setLoading(false));
  };

  useEffect(() => { load(1, '', ''); }, []);

  const handleSearch = () => { setPage(1); load(1, filterEvent, filterCustomer); };

  return (
    <div className="space-y-3">
      {/* Filters */}
      <div className="flex flex-wrap gap-2 items-end">
        <div>
          <label className="block text-xs font-medium text-gray-600 dark:text-gray-400 mb-1">Event</label>
          <select
            value={filterEvent}
            onChange={(e) => setFilterEvent(e.target.value)}
            className="px-2 py-1.5 rounded border border-gray-300 dark:border-gray-600 bg-white dark:bg-gray-700 text-gray-900 dark:text-gray-100 text-xs focus:outline-none focus:ring-2 focus:ring-blue-500"
          >
            <option value="">All events</option>
            {KNOWN_EVENTS.map((ev) => <option key={ev} value={ev}>{ev}</option>)}
          </select>
        </div>
        <div>
          <label className="block text-xs font-medium text-gray-600 dark:text-gray-400 mb-1">Customer ID</label>
          <input
            type="text"
            value={filterCustomer}
            onChange={(e) => setFilterCustomer(e.target.value)}
            onKeyDown={(e) => e.key === 'Enter' && handleSearch()}
            placeholder="e.g. 123456"
            className="px-2 py-1.5 rounded border border-gray-300 dark:border-gray-600 bg-white dark:bg-gray-700 text-gray-900 dark:text-gray-100 text-xs focus:outline-none focus:ring-2 focus:ring-blue-500 w-36"
          />
        </div>
        <button
          onClick={handleSearch}
          className="px-3 py-1.5 bg-blue-600 text-white text-xs rounded hover:bg-blue-700"
        >
          Search
        </button>
        <button
          onClick={() => { setFilterEvent(''); setFilterCustomer(''); setPage(1); load(1, '', ''); }}
          className="px-3 py-1.5 bg-gray-100 dark:bg-gray-700 text-gray-700 dark:text-gray-300 text-xs rounded hover:bg-gray-200 dark:hover:bg-gray-600"
        >
          Clear
        </button>
        <span className="ml-auto text-xs text-gray-500 dark:text-gray-400">{total.toLocaleString()} events</span>
      </div>

      {error && <p className="text-sm text-red-500">{error}</p>}

      {/* Table */}
      <div className="overflow-x-auto rounded-lg border border-gray-200 dark:border-gray-700">
        <table className="min-w-full text-xs">
          <thead className="bg-gray-50 dark:bg-gray-800 border-b border-gray-200 dark:border-gray-700">
            <tr>
              {['ID', 'Event', 'Customer', 'Actions Applied', 'Received At', ''].map((h) => (
                <th key={h} className="px-3 py-2 text-left font-semibold text-gray-600 dark:text-gray-300 whitespace-nowrap">{h}</th>
              ))}
            </tr>
          </thead>
          <tbody className="divide-y divide-gray-100 dark:divide-gray-800">
            {loading ? (
              <tr><td colSpan={6} className="px-3 py-6 text-center text-gray-500">Loading…</td></tr>
            ) : rows.length === 0 ? (
              <tr><td colSpan={6} className="px-3 py-6 text-center text-gray-500">No events found.</td></tr>
            ) : rows.map((row) => (
              <>
                <tr key={row.id} className="bg-white dark:bg-gray-900 hover:bg-gray-50 dark:hover:bg-gray-800 transition-colors">
                  <td className="px-3 py-2 text-gray-500 dark:text-gray-400">{row.id}</td>
                  <td className="px-3 py-2 font-mono font-medium text-gray-900 dark:text-gray-100">{row.event_name}</td>
                  <td className="px-3 py-2 text-gray-700 dark:text-gray-300">{row.customer ?? <span className="text-gray-400">—</span>}</td>
                  <td className="px-3 py-2">
                    {row.actions_applied?.length > 0 ? (
                      <div className="flex flex-wrap gap-1">
                        {row.actions_applied.map((a, i) => (
                          <span
                            key={i}
                            className={`inline-flex items-center gap-1 px-1.5 py-0.5 rounded text-xs ${
                              a.result === 'ok' || a.result === 'logged' ? 'bg-green-100 text-green-700 dark:bg-green-900/20 dark:text-green-300' : 'bg-yellow-100 text-yellow-700 dark:bg-yellow-900/20 dark:text-yellow-300'
                            }`}
                          >
                            {a.action}
                          </span>
                        ))}
                      </div>
                    ) : <span className="text-gray-400">none</span>}
                  </td>
                  <td className="px-3 py-2 text-gray-500 whitespace-nowrap">
                    {row.created_at ? new Date(row.created_at).toLocaleString() : '—'}
                  </td>
                  <td className="px-3 py-2">
                    <button
                      onClick={() => setExpanded(expanded === row.id ? null : row.id)}
                      className="text-blue-600 dark:text-blue-400 hover:underline"
                    >
                      {expanded === row.id ? 'hide' : 'payload'}
                    </button>
                  </td>
                </tr>
                {expanded === row.id && (
                  <tr key={`${row.id}-expand`} className="bg-gray-50 dark:bg-gray-800">
                    <td colSpan={6} className="px-4 py-3">
                      <pre className="text-xs text-gray-700 dark:text-gray-300 overflow-x-auto whitespace-pre-wrap">
                        {JSON.stringify(row.payload, null, 2)}
                      </pre>
                    </td>
                  </tr>
                )}
              </>
            ))}
          </tbody>
        </table>
      </div>

      {/* Pagination */}
      {total > PAGE_SIZE && (
        <div className="flex items-center gap-2 text-xs">
          <button
            disabled={page === 1}
            onClick={() => { const p = page - 1; setPage(p); load(p, filterEvent, filterCustomer); }}
            className="px-2 py-1 rounded border border-gray-300 dark:border-gray-600 disabled:opacity-40 hover:bg-gray-100 dark:hover:bg-gray-700"
          >
            Prev
          </button>
          <span className="text-gray-500">Page {page} of {Math.ceil(total / PAGE_SIZE)}</span>
          <button
            disabled={page >= Math.ceil(total / PAGE_SIZE)}
            onClick={() => { const p = page + 1; setPage(p); load(p, filterEvent, filterCustomer); }}
            className="px-2 py-1 rounded border border-gray-300 dark:border-gray-600 disabled:opacity-40 hover:bg-gray-100 dark:hover:bg-gray-700"
          >
            Next
          </button>
        </div>
      )}
    </div>
  );
}

// ── Action Rules Tab ──────────────────────────────────────────────────────────

function ActionRulesTab() {
  const [rules, setRules] = useState<ActionRule[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState('');

  // New rule form
  const [showForm, setShowForm] = useState(false);
  const [formEvent, setFormEvent] = useState(KNOWN_EVENTS[0] as string);
  const [formCustomEvent, setFormCustomEvent] = useState('');
  const [formAction, setFormAction] = useState<ActionType>('log_only');
  const [formLabel, setFormLabel] = useState('');
  const [formConfig, setFormConfig] = useState('{}');
  const [formConfigError, setFormConfigError] = useState('');
  const [saving, setSaving] = useState(false);

  const load = () => {
    setLoading(true);
    fetchActionRules()
      .then(setRules)
      .catch((e) => setError(e.message))
      .finally(() => setLoading(false));
  };

  useEffect(() => { load(); }, []);

  const handleSave = async () => {
    let parsedConfig: Record<string, unknown> = {};
    try { parsedConfig = JSON.parse(formConfig); } catch { setFormConfigError('Invalid JSON'); return; }
    setFormConfigError('');
    const eventName = formEvent === '__custom__' ? formCustomEvent.trim() : formEvent;
    if (!eventName) return;
    setSaving(true);
    try {
      await createActionRule({ event_name: eventName, action_type: formAction, label: formLabel || undefined, config: parsedConfig });
      setShowForm(false);
      setFormLabel('');
      setFormConfig('{}');
      load();
    } catch (e: unknown) {
      setError(e instanceof Error ? e.message : 'Failed');
    } finally {
      setSaving(false);
    }
  };

  const handleToggle = async (rule: ActionRule) => {
    try {
      await updateActionRule(rule.id, { is_active: !rule.is_active });
      load();
    } catch (e: unknown) {
      setError(e instanceof Error ? e.message : 'Failed');
    }
  };

  const handleDelete = async (id: number) => {
    try {
      await deleteActionRule(id);
      load();
    } catch (e: unknown) {
      setError(e instanceof Error ? e.message : 'Failed');
    }
  };

  // Group rules by event_name
  const grouped = rules.reduce<Record<string, ActionRule[]>>((acc, r) => {
    (acc[r.event_name] ??= []).push(r);
    return acc;
  }, {});

  return (
    <div className="space-y-4">
      <div className="flex items-center justify-between">
        <p className="text-sm text-gray-500 dark:text-gray-400">
          Configure what happens when each event is received.
        </p>
        <button
          onClick={() => setShowForm(!showForm)}
          className="px-3 py-1.5 bg-blue-600 text-white text-xs rounded hover:bg-blue-700"
        >
          + Add Rule
        </button>
      </div>

      {error && <p className="text-sm text-red-500">{error}</p>}

      {/* New rule form */}
      {showForm && (
        <div className="bg-gray-50 dark:bg-gray-800 border border-gray-200 dark:border-gray-700 rounded-lg p-4 space-y-3">
          <h3 className="text-sm font-semibold text-gray-900 dark:text-gray-100">New Action Rule</h3>
          <div className="grid grid-cols-1 sm:grid-cols-2 gap-3">
            <div>
              <label className="block text-xs font-medium text-gray-600 dark:text-gray-400 mb-1">Event Name</label>
              <select
                value={formEvent}
                onChange={(e) => setFormEvent(e.target.value)}
                className="w-full px-2 py-1.5 rounded border border-gray-300 dark:border-gray-600 bg-white dark:bg-gray-700 text-gray-900 dark:text-gray-100 text-xs focus:outline-none focus:ring-2 focus:ring-blue-500"
              >
                {KNOWN_EVENTS.map((ev) => <option key={ev} value={ev}>{ev}</option>)}
                <option value="__custom__">Custom…</option>
              </select>
              {formEvent === '__custom__' && (
                <input
                  type="text"
                  value={formCustomEvent}
                  onChange={(e) => setFormCustomEvent(e.target.value)}
                  placeholder="my_custom_event"
                  className="mt-1 w-full px-2 py-1.5 rounded border border-gray-300 dark:border-gray-600 bg-white dark:bg-gray-700 text-gray-900 dark:text-gray-100 text-xs focus:outline-none focus:ring-2 focus:ring-blue-500"
                />
              )}
            </div>
            <div>
              <label className="block text-xs font-medium text-gray-600 dark:text-gray-400 mb-1">Action Type</label>
              <select
                value={formAction}
                onChange={(e) => setFormAction(e.target.value as ActionType)}
                className="w-full px-2 py-1.5 rounded border border-gray-300 dark:border-gray-600 bg-white dark:bg-gray-700 text-gray-900 dark:text-gray-100 text-xs focus:outline-none focus:ring-2 focus:ring-blue-500"
              >
                {ACTION_TYPES.map((at) => <option key={at} value={at}>{ACTION_LABELS[at]}</option>)}
              </select>
            </div>
            <div>
              <label className="block text-xs font-medium text-gray-600 dark:text-gray-400 mb-1">Label (optional)</label>
              <input
                type="text"
                value={formLabel}
                onChange={(e) => setFormLabel(e.target.value)}
                placeholder="e.g. Forward withdrawal to Optimove"
                className="w-full px-2 py-1.5 rounded border border-gray-300 dark:border-gray-600 bg-white dark:bg-gray-700 text-gray-900 dark:text-gray-100 text-xs focus:outline-none focus:ring-2 focus:ring-blue-500"
              />
            </div>
            <div>
              <label className="block text-xs font-medium text-gray-600 dark:text-gray-400 mb-1">
                Config (JSON)
                {formAction === 'optimove' && (
                  <span className="ml-2 font-normal text-gray-400">— e.g. {`{"optimove_event_name":"withdrawal_request"}`}</span>
                )}
              </label>
              <input
                type="text"
                value={formConfig}
                onChange={(e) => { setFormConfig(e.target.value); setFormConfigError(''); }}
                className="w-full px-2 py-1.5 rounded border border-gray-300 dark:border-gray-600 bg-white dark:bg-gray-700 text-gray-900 dark:text-gray-100 text-xs font-mono focus:outline-none focus:ring-2 focus:ring-blue-500"
              />
              {formConfigError && <p className="text-xs text-red-500 mt-0.5">{formConfigError}</p>}
            </div>
          </div>
          <div className="flex gap-2">
            <button
              onClick={handleSave}
              disabled={saving}
              className="px-3 py-1.5 bg-blue-600 text-white text-xs rounded hover:bg-blue-700 disabled:opacity-50"
            >
              {saving ? 'Saving…' : 'Save Rule'}
            </button>
            <button
              onClick={() => setShowForm(false)}
              className="px-3 py-1.5 bg-gray-100 dark:bg-gray-700 text-gray-700 dark:text-gray-300 text-xs rounded hover:bg-gray-200 dark:hover:bg-gray-600"
            >
              Cancel
            </button>
          </div>
        </div>
      )}

      {/* Rules grouped by event */}
      {loading ? (
        <p className="text-sm text-gray-500 py-4 text-center">Loading…</p>
      ) : Object.keys(grouped).length === 0 ? (
        <div className="rounded-lg border border-dashed border-gray-300 dark:border-gray-600 py-10 text-center">
          <p className="text-sm text-gray-500 dark:text-gray-400">No action rules yet.</p>
          <p className="text-xs text-gray-400 dark:text-gray-500 mt-1">
            By default all events are just stored. Add rules to trigger actions.
          </p>
        </div>
      ) : (
        <div className="space-y-3">
          {Object.entries(grouped).map(([eventName, eventRules]) => (
            <div key={eventName} className="rounded-lg border border-gray-200 dark:border-gray-700 overflow-hidden">
              <div className="bg-gray-50 dark:bg-gray-800 px-4 py-2 border-b border-gray-200 dark:border-gray-700">
                <span className="font-mono text-sm font-semibold text-gray-900 dark:text-gray-100">{eventName}</span>
                <span className="ml-2 text-xs text-gray-500">{eventRules.length} rule{eventRules.length !== 1 ? 's' : ''}</span>
              </div>
              <table className="min-w-full text-xs">
                <thead className="border-b border-gray-100 dark:border-gray-700">
                  <tr>
                    {['Action', 'Label', 'Config', 'Active', ''].map((h) => (
                      <th key={h} className="px-3 py-2 text-left font-medium text-gray-500 dark:text-gray-400">{h}</th>
                    ))}
                  </tr>
                </thead>
                <tbody className="divide-y divide-gray-100 dark:divide-gray-800">
                  {eventRules.map((rule) => (
                    <tr key={rule.id} className="bg-white dark:bg-gray-900 hover:bg-gray-50 dark:hover:bg-gray-800 transition-colors">
                      <td className="px-3 py-2">
                        <span className={`inline-flex px-2 py-0.5 rounded-full text-xs font-medium ${ACTION_COLORS[rule.action_type]}`}>
                          {ACTION_LABELS[rule.action_type]}
                        </span>
                      </td>
                      <td className="px-3 py-2 text-gray-700 dark:text-gray-300">{rule.label ?? <span className="text-gray-400">—</span>}</td>
                      <td className="px-3 py-2 font-mono text-gray-500 dark:text-gray-400 max-w-xs truncate">
                        {Object.keys(rule.config).length ? JSON.stringify(rule.config) : <span className="text-gray-400">—</span>}
                      </td>
                      <td className="px-3 py-2">
                        <button
                          onClick={() => handleToggle(rule)}
                          className={`relative inline-flex h-4 w-8 rounded-full transition-colors ${rule.is_active ? 'bg-blue-500' : 'bg-gray-300 dark:bg-gray-600'}`}
                        >
                          <span className={`inline-block h-3 w-3 mt-0.5 rounded-full bg-white shadow transition-transform ${rule.is_active ? 'translate-x-4' : 'translate-x-0.5'}`} />
                        </button>
                      </td>
                      <td className="px-3 py-2">
                        <button
                          onClick={() => handleDelete(rule.id)}
                          className="text-red-500 hover:text-red-700 dark:hover:text-red-400 text-xs"
                        >
                          Delete
                        </button>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          ))}
        </div>
      )}
    </div>
  );
}

// ── Stats Tab ─────────────────────────────────────────────────────────────────

function StatsTab() {
  const [stats, setStats] = useState<EventStat[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState('');

  useEffect(() => {
    fetchEventStats()
      .then(setStats)
      .catch((e) => setError(e.message))
      .finally(() => setLoading(false));
  }, []);

  if (loading) return <p className="text-sm text-gray-500 py-6 text-center">Loading…</p>;
  if (error) return <p className="text-sm text-red-500 py-6 text-center">{error}</p>;
  if (!stats.length) return <p className="text-sm text-gray-500 py-6 text-center">No events received yet.</p>;

  return (
    <div className="overflow-x-auto rounded-lg border border-gray-200 dark:border-gray-700">
      <table className="min-w-full text-xs">
        <thead className="bg-gray-50 dark:bg-gray-800 border-b border-gray-200 dark:border-gray-700">
          <tr>
            {['Event Name', 'Total Received', 'Last Received'].map((h) => (
              <th key={h} className="px-4 py-2 text-left font-semibold text-gray-600 dark:text-gray-300">{h}</th>
            ))}
          </tr>
        </thead>
        <tbody className="divide-y divide-gray-100 dark:divide-gray-800">
          {stats.map((s) => (
            <tr key={s.event_name} className="bg-white dark:bg-gray-900 hover:bg-gray-50 dark:hover:bg-gray-800">
              <td className="px-4 py-2 font-mono font-medium text-gray-900 dark:text-gray-100">{s.event_name}</td>
              <td className="px-4 py-2 text-gray-700 dark:text-gray-300">{s.total.toLocaleString()}</td>
              <td className="px-4 py-2 text-gray-500">
                {s.last_received ? new Date(s.last_received).toLocaleString() : '—'}
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

// ── Page ──────────────────────────────────────────────────────────────────────

export function WebhookEventsPage() {
  const [activeTab, setActiveTab] = useState<Tab>(TABS[0]);

  return (
    <div className="space-y-6">
      {/* Header */}
      <div>
        <h1 className="text-lg font-semibold text-gray-900 dark:text-gray-100">Webhook Events</h1>
        <p className="mt-1 text-sm text-gray-500 dark:text-gray-400">
          Endpoint: <code className="font-mono text-xs bg-gray-100 dark:bg-gray-700 px-1.5 py-0.5 rounded">POST /api/webhooks/event</code>
          {' '}— receives and stores all events, then applies configured action rules.
        </p>
      </div>

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

      {activeTab === 'Event Log' && <EventLogTab />}
      {activeTab === 'Action Rules' && <ActionRulesTab />}
      {activeTab === 'Stats' && <StatsTab />}
    </div>
  );
}
