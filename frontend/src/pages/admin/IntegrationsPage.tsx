import { useEffect, useState } from 'react';
import axios from 'axios';

const api = axios.create({ baseURL: import.meta.env.VITE_API_BASE_URL || '/api' });
api.interceptors.request.use((config) => {
  const auth = JSON.parse(localStorage.getItem('auth') || '{}');
  const token = auth?.state?.token;
  if (token) config.headers.Authorization = `Bearer ${token}`;
  return config;
});

interface Integration {
  id: number;
  name: string;
  base_url: string;
  auth_key: string | null;
  description: string | null;
  is_active: boolean;
  created_at: string;
}

interface DbInfo {
  host: string;
  port?: number;
  database: string;
  user: string;
  status: string;
}

interface IntegrationsResponse {
  integrations: Integration[];
  databases: Record<string, DbInfo>;
}

const EMPTY_FORM = { name: '', base_url: '', auth_key: '', description: '', is_active: true };

// ---------------------------------------------------------------------------
// SendGrid section
// ---------------------------------------------------------------------------

interface SendGridConfig {
  api_key: string | null;
  from_email: string;
  configured: boolean;
}

function SendGridSection() {
  const [config, setConfig] = useState<SendGridConfig | null>(null);
  const [loading, setLoading] = useState(true);
  const [editing, setEditing] = useState(false);
  const [form, setForm] = useState({ api_key: '', from_email: '' });
  const [saving, setSaving] = useState(false);
  const [testing, setTesting] = useState(false);
  const [testEmail, setTestEmail] = useState('');
  const [showTestInput, setShowTestInput] = useState(false);
  const [message, setMessage] = useState<{ text: string; type: 'success' | 'error' } | null>(null);

  const load = async () => {
    try {
      const res = await api.get<SendGridConfig>('/admin/sendgrid');
      setConfig(res.data);
      setForm({ api_key: '', from_email: res.data.from_email });
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => { load(); }, []);

  const showMsg = (text: string, type: 'success' | 'error') => {
    setMessage({ text, type });
    setTimeout(() => setMessage(null), 4000);
  };

  const handleSave = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!form.api_key) { showMsg('API key is required', 'error'); return; }
    setSaving(true);
    try {
      await api.put('/admin/sendgrid', { api_key: form.api_key, from_email: form.from_email });
      showMsg('SendGrid configuration saved.', 'success');
      setEditing(false);
      setForm((f) => ({ ...f, api_key: '' }));
      load();
    } catch (err: any) {
      showMsg(err.response?.data?.detail || 'Failed to save', 'error');
    } finally {
      setSaving(false);
    }
  };

  const handleTest = async () => {
    if (!testEmail) return;
    setTesting(true);
    try {
      await api.post('/admin/sendgrid/test', { to_email: testEmail });
      showMsg(`Test email sent to ${testEmail}.`, 'success');
      setShowTestInput(false);
      setTestEmail('');
    } catch (err: any) {
      showMsg(err.response?.data?.detail || 'Test email failed', 'error');
    } finally {
      setTesting(false);
    }
  };

  return (
    <div>
      <h2 className="text-base font-semibold text-gray-700 dark:text-gray-300 mb-4">SendGrid</h2>
      <div className="bg-white dark:bg-gray-800 rounded-lg shadow dark:shadow-gray-900 p-5">
        {loading ? (
          <p className="text-sm text-gray-400">Loading...</p>
        ) : (
          <div className="space-y-4">
            {/* Status row */}
            <div className="flex items-center justify-between">
              <div className="flex items-center gap-3">
                <svg className="w-8 h-8" viewBox="0 0 48 48" fill="none" xmlns="http://www.w3.org/2000/svg">
                  <rect width="48" height="48" rx="8" fill="#1A82E2"/>
                  <path d="M24 12C17.373 12 12 17.373 12 24C12 30.627 17.373 36 24 36C30.627 36 36 30.627 36 24" stroke="white" strokeWidth="2.5" strokeLinecap="round"/>
                  <path d="M36 12L24 24" stroke="white" strokeWidth="2.5" strokeLinecap="round"/>
                  <circle cx="36" cy="12" r="3" fill="white"/>
                </svg>
                <div>
                  <p className="text-sm font-semibold text-gray-800 dark:text-gray-100">SendGrid Email API</p>
                  <p className="text-xs text-gray-500 dark:text-gray-400">Used for password reset emails</p>
                </div>
              </div>
              <span className={`px-2 py-0.5 rounded-full text-xs font-medium ${config?.configured ? 'bg-green-100 text-green-800 dark:bg-green-900 dark:text-green-200' : 'bg-yellow-100 text-yellow-800 dark:bg-yellow-900 dark:text-yellow-200'}`}>
                {config?.configured ? 'Configured' : 'Not configured'}
              </span>
            </div>

            {/* Current config display */}
            {!editing && (
              <div className="space-y-1.5 text-sm border-t border-gray-100 dark:border-gray-700 pt-3">
                <div className="flex items-center gap-2">
                  <span className="text-xs font-medium text-gray-500 dark:text-gray-400 w-24 flex-shrink-0">API Key</span>
                  <code className="text-xs bg-gray-50 dark:bg-gray-900 px-2 py-0.5 rounded text-gray-600 dark:text-gray-400">
                    {config?.api_key || '(not set)'}
                  </code>
                </div>
                <div className="flex items-center gap-2">
                  <span className="text-xs font-medium text-gray-500 dark:text-gray-400 w-24 flex-shrink-0">From Email</span>
                  <span className="text-gray-700 dark:text-gray-300 text-xs">{config?.from_email}</span>
                </div>
              </div>
            )}

            {/* Edit form */}
            {editing && (
              <form onSubmit={handleSave} className="space-y-3 border-t border-gray-100 dark:border-gray-700 pt-3">
                <div>
                  <label className="block text-xs font-medium text-gray-600 dark:text-gray-400 mb-1">API Key *</label>
                  <input
                    required
                    value={form.api_key}
                    onChange={(e) => setForm({ ...form, api_key: e.target.value })}
                    placeholder="SG.xxxxxxxxxxxxxxxxxxxxxxxx"
                    className="w-full border border-gray-300 dark:border-gray-600 rounded-md px-3 py-1.5 text-sm bg-white dark:bg-gray-800 text-gray-900 dark:text-gray-100 placeholder-gray-400 dark:placeholder-gray-500 focus:outline-none focus:ring-2 focus:ring-blue-500 font-mono"
                  />
                </div>
                <div>
                  <label className="block text-xs font-medium text-gray-600 dark:text-gray-400 mb-1">From Email *</label>
                  <input
                    required
                    type="email"
                    value={form.from_email}
                    onChange={(e) => setForm({ ...form, from_email: e.target.value })}
                    placeholder="no-reply@example.com"
                    className="w-full border border-gray-300 dark:border-gray-600 rounded-md px-3 py-1.5 text-sm bg-white dark:bg-gray-800 text-gray-900 dark:text-gray-100 placeholder-gray-400 dark:placeholder-gray-500 focus:outline-none focus:ring-2 focus:ring-blue-500"
                  />
                </div>
                <div className="flex gap-2">
                  <button
                    type="submit"
                    disabled={saving}
                    className="px-4 py-1.5 bg-blue-600 text-white rounded-md text-sm font-medium hover:bg-blue-700 disabled:opacity-50 transition-colors"
                  >
                    {saving ? 'Saving...' : 'Save'}
                  </button>
                  <button
                    type="button"
                    onClick={() => { setEditing(false); setForm((f) => ({ ...f, api_key: '' })); }}
                    className="px-4 py-1.5 bg-gray-100 dark:bg-gray-700 text-gray-700 dark:text-gray-300 rounded-md text-sm font-medium hover:bg-gray-200 dark:hover:bg-gray-600 transition-colors"
                  >
                    Cancel
                  </button>
                </div>
              </form>
            )}

            {/* Test email input */}
            {showTestInput && !editing && (
              <div className="flex gap-2 items-center border-t border-gray-100 dark:border-gray-700 pt-3">
                <input
                  type="email"
                  value={testEmail}
                  onChange={(e) => setTestEmail(e.target.value)}
                  placeholder="recipient@example.com"
                  className="flex-1 border border-gray-300 dark:border-gray-600 rounded-md px-3 py-1.5 text-sm bg-white dark:bg-gray-800 text-gray-900 dark:text-gray-100 placeholder-gray-400 focus:outline-none focus:ring-2 focus:ring-blue-500"
                />
                <button
                  onClick={handleTest}
                  disabled={testing || !testEmail}
                  className="px-4 py-1.5 bg-blue-600 text-white rounded-md text-sm font-medium hover:bg-blue-700 disabled:opacity-50 transition-colors"
                >
                  {testing ? 'Sending...' : 'Send'}
                </button>
                <button
                  onClick={() => { setShowTestInput(false); setTestEmail(''); }}
                  className="px-3 py-1.5 bg-gray-100 dark:bg-gray-700 text-gray-700 dark:text-gray-300 rounded-md text-sm font-medium hover:bg-gray-200 dark:hover:bg-gray-600 transition-colors"
                >
                  Cancel
                </button>
              </div>
            )}

            {/* Action buttons */}
            {!editing && (
              <div className="flex gap-2 pt-1 border-t border-gray-100 dark:border-gray-700">
                <button
                  onClick={() => setEditing(true)}
                  className="text-xs text-blue-600 hover:underline"
                >
                  {config?.configured ? 'Update API Key' : 'Configure'}
                </button>
                {config?.configured && !showTestInput && (
                  <button
                    onClick={() => setShowTestInput(true)}
                    className="text-xs text-gray-500 hover:underline"
                  >
                    Send Test Email
                  </button>
                )}
              </div>
            )}

            {/* Feedback message */}
            {message && (
              <p className={`text-sm ${message.type === 'success' ? 'text-green-600 dark:text-green-400' : 'text-red-600 dark:text-red-400'}`}>
                {message.text}
              </p>
            )}
          </div>
        )}
      </div>
    </div>
  );
}

// ---------------------------------------------------------------------------
// Webhooks reference section
// ---------------------------------------------------------------------------

interface WebhookEvent {
  event: string;
  description: string;
  payload: string;
}

interface WebhookDef {
  name: string;
  method: string;
  path: string;
  description: string;
  auth: string;
  events: WebhookEvent[];
}

const WEBHOOKS: WebhookDef[] = [
  {
    name: 'Trade Event Webhook',
    method: 'POST',
    path: '/api/webhooks/trade-event',
    description:
      'Unified webhook endpoint that handles all trading lifecycle events. Drives the Challenges module (trade, volume, streak, pnl, diversity, instrument challenge types) and the Action Bonuses module (live_details, submit_documents). No authentication required — designed to be called by the trading platform.',
    auth: 'None (public endpoint)',
    events: [
      {
        event: 'open_trade',
        description: 'Fired when a client opens a trade. Used by trade, volume, streak, diversity, and instrument challenges.',
        payload: JSON.stringify(
          {
            tenant: 991,
            event: 'open_trade',
            customer: 26708487,
            payload: {
              customer: '26708487',
              ticket: 'T123456',
              symbol: 'EURUSD',
              volume: 1.0,
              type: 'buy',
            },
            context: {
              account_number: 141738783,
              language: 'en',
            },
          },
          null,
          2,
        ),
      },
      {
        event: 'close_trade',
        description: 'Fired when a client closes a trade. Used by PnL challenges.',
        payload: JSON.stringify(
          {
            tenant: 991,
            event: 'close_trade',
            customer: 26708487,
            payload: {
              customer: '26708487',
              ticket: 'T123456',
              symbol: 'XAUUSD',
              volume: 0.5,
              type: 'sell',
            },
            context: {
              account_number: 141738783,
              profit: 42.5,
            },
          },
          null,
          2,
        ),
      },
      {
        event: 'live_details',
        description: 'Fired when a client completes their live account details. Triggers Action Bonus rules for this event type.',
        payload: JSON.stringify(
          {
            tenant: 991,
            event: 'live_details',
            customer: 26708487,
            context: {
              email: 'client@example.com',
              country: 'Mexico',
              affiliate: 'campaign_xyz',
              account_number: 141738783,
            },
          },
          null,
          2,
        ),
      },
      {
        event: 'submit_documents',
        description: 'Fired when a client submits KYC documents. Triggers Action Bonus rules for this event type.',
        payload: JSON.stringify(
          {
            tenant: 991,
            event: 'submit_documents',
            customer: 26708487,
            context: {
              email: 'client@example.com',
              country: 'Mexico',
              affiliate: 'campaign_xyz',
              account_number: 141738783,
            },
          },
          null,
          2,
        ),
      },
    ],
  },
];

function WebhooksSection() {
  const [expandedWebhook, setExpandedWebhook] = useState<string | null>('Trade Event Webhook');
  const [expandedEvent, setExpandedEvent] = useState<string | null>(null);
  const [copied, setCopied] = useState<string | null>(null);

  const copyToClipboard = (text: string, key: string) => {
    navigator.clipboard.writeText(text).then(() => {
      setCopied(key);
      setTimeout(() => setCopied(null), 2000);
    });
  };

  const eventBadgeColor: Record<string, string> = {
    open_trade: 'bg-green-100 text-green-700 dark:bg-green-900/30 dark:text-green-400',
    close_trade: 'bg-red-100 text-red-700 dark:bg-red-900/30 dark:text-red-400',
    live_details: 'bg-blue-100 text-blue-700 dark:bg-blue-900/30 dark:text-blue-400',
    submit_documents: 'bg-violet-100 text-violet-700 dark:bg-violet-900/30 dark:text-violet-400',
  };

  return (
    <div>
      <h2 className="text-base font-semibold text-gray-700 dark:text-gray-300 mb-4">Webhooks</h2>
      <div className="space-y-3">
        {WEBHOOKS.map((wh) => (
          <div key={wh.name} className="bg-white dark:bg-gray-800 rounded-lg shadow dark:shadow-gray-900 overflow-hidden">
            {/* Header */}
            <button
              className="w-full flex items-center justify-between px-5 py-4 text-left hover:bg-gray-50 dark:hover:bg-gray-700/50 transition-colors"
              onClick={() => setExpandedWebhook(expandedWebhook === wh.name ? null : wh.name)}
            >
              <div className="flex items-center gap-3">
                <span className="text-xs font-bold text-white bg-blue-600 px-2 py-0.5 rounded font-mono">
                  {wh.method}
                </span>
                <code className="text-sm text-gray-800 dark:text-gray-100 font-mono">{wh.path}</code>
                <span className="text-sm font-medium text-gray-700 dark:text-gray-300 hidden sm:block">— {wh.name}</span>
              </div>
              <span className="text-gray-400 text-sm">{expandedWebhook === wh.name ? '▲' : '▼'}</span>
            </button>

            {expandedWebhook === wh.name && (
              <div className="border-t border-gray-100 dark:border-gray-700 px-5 py-4 space-y-4">
                {/* Description */}
                <p className="text-sm text-gray-600 dark:text-gray-400">{wh.description}</p>

                {/* Meta row */}
                <div className="flex flex-wrap gap-4 text-xs">
                  <div>
                    <span className="text-gray-500 dark:text-gray-400">Endpoint: </span>
                    <code className="bg-gray-50 dark:bg-gray-900 px-2 py-0.5 rounded text-gray-700 dark:text-gray-300 font-mono">
                      {wh.path}
                    </code>
                    <button
                      onClick={() => copyToClipboard(wh.path, wh.path)}
                      className="ml-2 text-blue-600 hover:text-blue-700 dark:text-blue-400"
                    >
                      {copied === wh.path ? 'Copied!' : 'Copy'}
                    </button>
                  </div>
                  <div>
                    <span className="text-gray-500 dark:text-gray-400">Auth: </span>
                    <span className="text-gray-700 dark:text-gray-300">{wh.auth}</span>
                  </div>
                </div>

                {/* Events */}
                <div>
                  <h4 className="text-xs font-semibold text-gray-500 dark:text-gray-400 uppercase tracking-wide mb-2">
                    Supported Events ({wh.events.length})
                  </h4>
                  <div className="space-y-2">
                    {wh.events.map((ev) => (
                      <div key={ev.event} className="border border-gray-100 dark:border-gray-700 rounded-lg overflow-hidden">
                        <button
                          className="w-full flex items-center justify-between px-4 py-2.5 text-left hover:bg-gray-50 dark:hover:bg-gray-700/40 transition-colors"
                          onClick={() => setExpandedEvent(expandedEvent === ev.event ? null : ev.event)}
                        >
                          <div className="flex items-center gap-3">
                            <span className={`text-xs font-mono font-semibold px-2 py-0.5 rounded ${eventBadgeColor[ev.event] || 'bg-gray-100 text-gray-700'}`}>
                              {ev.event}
                            </span>
                            <span className="text-sm text-gray-600 dark:text-gray-400">{ev.description}</span>
                          </div>
                          <span className="text-gray-400 text-xs flex-shrink-0 ml-2">{expandedEvent === ev.event ? '▲' : 'Show payload'}</span>
                        </button>

                        {expandedEvent === ev.event && (
                          <div className="border-t border-gray-100 dark:border-gray-700 bg-gray-50 dark:bg-gray-900/60 px-4 py-3">
                            <div className="flex justify-between items-center mb-2">
                              <span className="text-xs text-gray-500 font-medium">Example payload</span>
                              <button
                                onClick={() => copyToClipboard(ev.payload, ev.event)}
                                className="text-xs text-blue-600 hover:text-blue-700 dark:text-blue-400"
                              >
                                {copied === ev.event ? 'Copied!' : 'Copy'}
                              </button>
                            </div>
                            <pre className="text-xs font-mono text-gray-700 dark:text-gray-300 overflow-x-auto whitespace-pre-wrap">
                              {ev.payload}
                            </pre>
                          </div>
                        )}
                      </div>
                    ))}
                  </div>
                </div>
              </div>
            )}
          </div>
        ))}
      </div>
    </div>
  );
}

export function IntegrationsPage() {
  const [data, setData] = useState<IntegrationsResponse | null>(null);
  const [loading, setLoading] = useState(true);
  const [showForm, setShowForm] = useState(false);
  const [editId, setEditId] = useState<number | null>(null);
  const [form, setForm] = useState({ ...EMPTY_FORM });
  const [saving, setSaving] = useState(false);
  const [formError, setFormError] = useState('');
  const [revealedKeys, setRevealedKeys] = useState<Set<number>>(new Set());
  const [revealedFullKeys, setRevealedFullKeys] = useState<Record<number, string>>({});

  const load = async () => {
    try {
      const res = await api.get<IntegrationsResponse>('/admin/integrations');
      setData(res.data);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => { load(); }, []);

  const toggleRevealKey = async (id: number) => {
    if (revealedKeys.has(id)) {
      setRevealedKeys((prev) => { const n = new Set(prev); n.delete(id); return n; });
      return;
    }
    // Fetch the unmasked key
    try {
      const res = await api.get<Integration>(`/admin/integrations/${id}?reveal_key=true`);
      setRevealedFullKeys((prev) => ({ ...prev, [id]: res.data.auth_key || '' }));
      setRevealedKeys((prev) => new Set(prev).add(id));
    } catch {
      // fallback: just toggle with masked key
      setRevealedKeys((prev) => new Set(prev).add(id));
    }
  };

  const startEdit = (integration: Integration) => {
    setEditId(integration.id);
    setForm({
      name: integration.name,
      base_url: integration.base_url,
      auth_key: '', // don't pre-fill masked key
      description: integration.description || '',
      is_active: integration.is_active,
    });
    setShowForm(true);
  };

  const resetForm = () => {
    setShowForm(false);
    setEditId(null);
    setForm({ ...EMPTY_FORM });
    setFormError('');
  };

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    setSaving(true);
    setFormError('');
    try {
      const payload = {
        name: form.name,
        base_url: form.base_url,
        auth_key: form.auth_key || null,
        description: form.description || null,
        is_active: form.is_active,
      };
      if (editId) {
        await api.put(`/admin/integrations/${editId}`, payload);
      } else {
        await api.post('/admin/integrations', payload);
      }
      resetForm();
      load();
    } catch (err: any) {
      setFormError(err.response?.data?.detail || 'Failed to save integration');
    } finally {
      setSaving(false);
    }
  };

  const deleteIntegration = async (id: number) => {
    if (!confirm('Delete this integration?')) return;
    try {
      await api.delete(`/admin/integrations/${id}`);
      load();
    } catch (err: any) {
      alert(err.response?.data?.detail || 'Failed to delete integration');
    }
  };

  const statusBadge = (status: string) => {
    const colors: Record<string, string> = {
      connected: 'bg-green-100 text-green-800 dark:bg-green-900 dark:text-green-200',
      configured: 'bg-blue-100 text-blue-800 dark:bg-blue-900 dark:text-blue-200',
      local: 'bg-yellow-100 text-yellow-800 dark:bg-yellow-900 dark:text-yellow-200',
    };
    return (
      <span className={`px-2 py-0.5 rounded-full text-xs font-medium ${colors[status] || 'bg-gray-100 dark:bg-gray-700 text-gray-700 dark:text-gray-300'}`}>
        {status}
      </span>
    );
  };

  if (loading) {
    return <div className="text-center text-gray-400 dark:text-gray-500 py-12">Loading...</div>;
  }

  const integrations = data?.integrations || [];
  const databases = data?.databases || {};

  return (
    <div className="space-y-6">
      {/* Integrations Section */}
      <div>
        <div className="flex justify-between items-center mb-4">
          <h2 className="text-base font-semibold text-gray-700 dark:text-gray-300">API Integrations</h2>
          <button
            onClick={() => showForm && !editId ? resetForm() : setShowForm(true)}
            className="px-4 py-2 bg-blue-600 text-white rounded-md text-sm font-medium hover:bg-blue-700 transition-colors"
          >
            {showForm && !editId ? 'Cancel' : '+ Add Integration'}
          </button>
        </div>

        {/* Add/Edit Form */}
        {showForm && (
          <form onSubmit={handleSubmit} className="bg-white dark:bg-gray-800 rounded-lg shadow dark:shadow-gray-900 p-5 space-y-4 mb-4">
            <h3 className="text-sm font-semibold text-gray-700 dark:text-gray-300">
              {editId ? 'Edit Integration' : 'New Integration'}
            </h3>
            <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
              <div>
                <label className="block text-xs font-medium text-gray-600 dark:text-gray-400 mb-1">Name *</label>
                <input
                  required
                  value={form.name}
                  onChange={(e) => setForm({ ...form, name: e.target.value })}
                  placeholder="e.g. SendGrid, Optimove"
                  className="w-full border border-gray-300 dark:border-gray-600 rounded-md px-3 py-1.5 text-sm bg-white dark:bg-gray-800 text-gray-900 dark:text-gray-100 placeholder-gray-400 dark:placeholder-gray-500 focus:outline-none focus:ring-2 focus:ring-blue-500"
                />
              </div>
              <div>
                <label className="block text-xs font-medium text-gray-600 dark:text-gray-400 mb-1">Base URL *</label>
                <input
                  required
                  value={form.base_url}
                  onChange={(e) => setForm({ ...form, base_url: e.target.value })}
                  placeholder="https://api.example.com/v1/"
                  className="w-full border border-gray-300 dark:border-gray-600 rounded-md px-3 py-1.5 text-sm bg-white dark:bg-gray-800 text-gray-900 dark:text-gray-100 placeholder-gray-400 dark:placeholder-gray-500 focus:outline-none focus:ring-2 focus:ring-blue-500"
                />
              </div>
              <div>
                <label className="block text-xs font-medium text-gray-600 dark:text-gray-400 mb-1">Auth Key</label>
                <input
                  value={form.auth_key}
                  onChange={(e) => setForm({ ...form, auth_key: e.target.value })}
                  placeholder={editId ? '(leave blank to keep current)' : 'Optional'}
                  className="w-full border border-gray-300 dark:border-gray-600 rounded-md px-3 py-1.5 text-sm bg-white dark:bg-gray-800 text-gray-900 dark:text-gray-100 placeholder-gray-400 dark:placeholder-gray-500 focus:outline-none focus:ring-2 focus:ring-blue-500"
                />
              </div>
              <div>
                <label className="block text-xs font-medium text-gray-600 dark:text-gray-400 mb-1">Description</label>
                <input
                  value={form.description}
                  onChange={(e) => setForm({ ...form, description: e.target.value })}
                  placeholder="Optional description"
                  className="w-full border border-gray-300 dark:border-gray-600 rounded-md px-3 py-1.5 text-sm bg-white dark:bg-gray-800 text-gray-900 dark:text-gray-100 placeholder-gray-400 dark:placeholder-gray-500 focus:outline-none focus:ring-2 focus:ring-blue-500"
                />
              </div>
            </div>
            <div>
              <label className="flex items-center gap-2 text-sm text-gray-700 dark:text-gray-300 cursor-pointer">
                <input
                  type="checkbox"
                  checked={form.is_active}
                  onChange={(e) => setForm({ ...form, is_active: e.target.checked })}
                  className="rounded border-gray-300 dark:border-gray-600 text-blue-600 focus:ring-blue-500"
                />
                Active
              </label>
            </div>
            {formError && <p className="text-sm text-red-600">{formError}</p>}
            <div className="flex gap-2">
              <button
                type="submit"
                disabled={saving}
                className="px-4 py-1.5 bg-blue-600 text-white rounded-md text-sm font-medium hover:bg-blue-700 disabled:opacity-50 transition-colors"
              >
                {saving ? 'Saving...' : editId ? 'Update' : 'Create'}
              </button>
              <button
                type="button"
                onClick={resetForm}
                className="px-4 py-1.5 bg-gray-100 dark:bg-gray-700 text-gray-700 dark:text-gray-300 rounded-md text-sm font-medium hover:bg-gray-200 dark:hover:bg-gray-600 transition-colors"
              >
                Cancel
              </button>
            </div>
          </form>
        )}

        {/* Integration Cards */}
        {integrations.length === 0 ? (
          <div className="bg-white dark:bg-gray-800 rounded-lg shadow dark:shadow-gray-900 p-8 text-center text-gray-400 dark:text-gray-500 text-sm">
            No integrations configured yet. Click "+ Add Integration" to get started.
          </div>
        ) : (
          <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
            {integrations.map((integ) => (
              <div key={integ.id} className="bg-white dark:bg-gray-800 rounded-lg shadow dark:shadow-gray-900 p-5 space-y-3">
                <div className="flex justify-between items-start">
                  <div>
                    <h3 className="text-sm font-semibold text-gray-800 dark:text-gray-100">{integ.name}</h3>
                    {integ.description && (
                      <p className="text-xs text-gray-500 dark:text-gray-400 mt-0.5">{integ.description}</p>
                    )}
                  </div>
                  <span
                    className={`px-2 py-0.5 rounded-full text-xs font-medium ${
                      integ.is_active
                        ? 'bg-green-100 text-green-800 dark:bg-green-900 dark:text-green-200'
                        : 'bg-red-100 text-red-800 dark:bg-red-900 dark:text-red-200'
                    }`}
                  >
                    {integ.is_active ? 'Active' : 'Inactive'}
                  </span>
                </div>

                <div className="space-y-1.5 text-sm">
                  <div className="flex items-center gap-2">
                    <span className="text-xs font-medium text-gray-500 dark:text-gray-400 w-20 flex-shrink-0">Base URL</span>
                    <span className="text-gray-700 dark:text-gray-300 break-all">{integ.base_url}</span>
                  </div>
                  {integ.auth_key && (
                    <div className="flex items-center gap-2">
                      <span className="text-xs font-medium text-gray-500 dark:text-gray-400 w-20 flex-shrink-0">Auth Key</span>
                      <code className="text-xs bg-gray-50 dark:bg-gray-900 px-2 py-0.5 rounded text-gray-600 dark:text-gray-400 break-all">
                        {revealedKeys.has(integ.id)
                          ? revealedFullKeys[integ.id] || integ.auth_key
                          : integ.auth_key}
                      </code>
                      <button
                        onClick={() => toggleRevealKey(integ.id)}
                        className="text-xs text-blue-600 hover:underline flex-shrink-0"
                      >
                        {revealedKeys.has(integ.id) ? 'Hide' : 'Reveal'}
                      </button>
                    </div>
                  )}
                  <div className="flex items-center gap-2">
                    <span className="text-xs font-medium text-gray-500 dark:text-gray-400 w-20 flex-shrink-0">Added</span>
                    <span className="text-gray-500 dark:text-gray-400 text-xs">
                      {new Date(integ.created_at).toLocaleDateString()}
                    </span>
                  </div>
                </div>

                <div className="flex gap-2 pt-1 border-t border-gray-100 dark:border-gray-700">
                  <button
                    onClick={() => startEdit(integ)}
                    className="text-xs text-blue-600 hover:underline"
                  >
                    Edit
                  </button>
                  <button
                    onClick={() => deleteIntegration(integ.id)}
                    className="text-xs text-red-500 hover:underline"
                  >
                    Delete
                  </button>
                </div>
              </div>
            ))}
          </div>
        )}
      </div>

      {/* SendGrid Section */}
      <SendGridSection />

      {/* Webhooks Section */}
      <WebhooksSection />

      {/* Database Connections Section */}
      <div>
        <h2 className="text-base font-semibold text-gray-700 dark:text-gray-300 mb-4">Database Connections</h2>
        <div className="grid grid-cols-1 lg:grid-cols-2 xl:grid-cols-3 gap-4">
          {Object.entries(databases).map(([key, db]) => (
            <div key={key} className="bg-white dark:bg-gray-800 rounded-lg shadow dark:shadow-gray-900 p-5 space-y-3">
              <div className="flex justify-between items-center">
                <h3 className="text-sm font-semibold text-gray-800 dark:text-gray-100 uppercase">{key}</h3>
                {statusBadge(db.status)}
              </div>
              <div className="space-y-1.5 text-sm">
                <div className="flex items-center gap-2">
                  <span className="text-xs font-medium text-gray-500 dark:text-gray-400 w-16 flex-shrink-0">Host</span>
                  <span className="text-gray-700 dark:text-gray-300">{db.host}</span>
                </div>
                {db.port && (
                  <div className="flex items-center gap-2">
                    <span className="text-xs font-medium text-gray-500 dark:text-gray-400 w-16 flex-shrink-0">Port</span>
                    <span className="text-gray-700 dark:text-gray-300">{db.port}</span>
                  </div>
                )}
                <div className="flex items-center gap-2">
                  <span className="text-xs font-medium text-gray-500 dark:text-gray-400 w-16 flex-shrink-0">Database</span>
                  <span className="text-gray-700 dark:text-gray-300">{db.database}</span>
                </div>
                <div className="flex items-center gap-2">
                  <span className="text-xs font-medium text-gray-500 dark:text-gray-400 w-16 flex-shrink-0">User</span>
                  <span className="text-gray-700 dark:text-gray-300">{db.user}</span>
                </div>
              </div>
            </div>
          ))}
        </div>
      </div>
    </div>
  );
}
