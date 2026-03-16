import { useEffect, useState, useRef, useCallback } from 'react';
import axios from 'axios';

const api = axios.create({ baseURL: import.meta.env.VITE_API_BASE_URL || '/api' });
api.interceptors.request.use((config) => {
  const auth = JSON.parse(localStorage.getItem('auth') || '{}');
  const token = auth?.state?.token;
  if (token) config.headers.Authorization = `Bearer ${token}`;
  return config;
});

// Exactly the pages that exist in the current nav — grouped to match sidebar sections
const NAV_PAGES: { section: string; key: string; label: string }[] = [
  { section: 'Marketing', key: 'challenges',         label: 'Challenges' },
  { section: 'Marketing', key: 'action-bonuses',     label: 'Automatic Bonus' },
  { section: 'Marketing', key: 'elena-ai-results',   label: 'Elena AI Results' },
  { section: 'AI Calls',  key: 'call-manager',       label: 'Call Manager' },
  { section: 'AI Calls',  key: 'call-history',       label: 'Call History' },
  { section: 'AI Calls',  key: 'call-dashboard',     label: 'AI Call Dashboard' },
  { section: 'AI Calls',  key: 'batch-call',         label: 'Batch Call from File' },
  { section: 'AI Calls',  key: 'calling-agents',     label: 'Calling Agents' },
  { section: 'AI Calls',  key: 'elena-ai-upload',    label: 'Upload to Campaign' },
  { section: 'System',    key: 'protected-clients',  label: 'Protected Clients' },
  { section: 'System',    key: 'webhook-events',     label: 'Webhook Events' },
  { section: 'Admin',     key: 'users',              label: 'Users' },
  { section: 'Admin',     key: 'roles',              label: 'Roles' },
  { section: 'Admin',     key: 'permissions',        label: 'Permissions' },
  { section: 'Admin',     key: 'integrations',       label: 'Integrations & Config' },
];

const PAGE_LABELS: Record<string, string> = Object.fromEntries(NAV_PAGES.map((p) => [p.key, p.label]));

interface Role {
  id: number;
  name: string;
  permissions: string[];
  created_at: string;
}

// ── CLAUD-85: Column visibility configuration ────────────────────────────

// CLAUD-115: Full column list with all groups
const COLUMN_GROUPS = [
  {
    label: 'Identity',
    columns: [
      { key: 'client_name', label: 'Client Name' },
      { key: 'client_id', label: 'Client ID' },
      { key: 'score', label: 'Score' },
      { key: 'age', label: 'Age' },
      { key: 'client_potential', label: 'Client Potential' },   // CLAUD-116
      { key: 'client_segment', label: 'Segment' },              // CLAUD-116
    ],
  },
  {
    label: 'Financials',
    columns: [
      { key: 'balance', label: 'Balance' },
      { key: 'equity', label: 'Equity' },
      { key: 'live_equity', label: 'Live Equity' },
      { key: 'credit', label: 'Credit' },
      { key: 'exposure_usd', label: 'Exposure (USD)' },
      { key: 'exposure_pct', label: 'Exposure %' },
      { key: 'open_pnl', label: 'Open PNL' },
      { key: 'closed_pnl', label: 'Closed P&L' },         // CLAUD-121
      { key: 'net_deposit_ever', label: 'Net Deposit Ever' }, // CLAUD-121
      { key: 'total_deposit', label: 'Total Deposits' },
      { key: 'deposit_count', label: 'Deposit Count' },
    ],
  },
  {
    label: 'Trading',
    columns: [
      { key: 'trade_count', label: 'Total Trades' },
      { key: 'turnover', label: 'Turnover' },
      { key: 'max_volume', label: 'Max Volume' },
      { key: 'max_open_trade', label: 'Max Open Trade' },
      { key: 'avg_trade_size', label: 'Avg Trade Size' },
      { key: 'last_trade_date', label: 'Last Trade Date' },
      { key: 'days_in_retention', label: 'Days in Retention' },
      { key: 'days_from_last_trade', label: 'Days from Last Trade' },
    ],
  },
  {
    label: 'Activity',
    columns: [
      { key: 'retention_status', label: 'Retention Status' },
      { key: 'last_contact', label: 'Last Contact' },
      { key: 'assigned_to', label: 'Assigned To' },
      { key: 'task_type', label: 'Tasks' },
      { key: 'registration_date', label: 'Registration Date' },
      { key: 'card_type', label: 'Card Type' },
    ],
  },
];

type ColumnConfig = Record<string, boolean>;

function ColumnVisibilitySection({ roleName }: { roleName: string }) {
  const [columns, setColumns] = useState<ColumnConfig>({});
  const [loading, setLoading] = useState(true);
  const [toast, setToast] = useState<{ type: 'success' | 'error'; message: string } | null>(null);
  const [resetting, setResetting] = useState(false);
  const debounceTimers = useRef<Record<string, ReturnType<typeof setTimeout>>>({});

  // Fetch config on mount
  useEffect(() => {
    setLoading(true);
    api.get(`/admin/column-visibility/${roleName}`)
      .then((res) => setColumns(res.data.columns))
      .catch(() => setToast({ type: 'error', message: 'Failed to load column config' }))
      .finally(() => setLoading(false));
  }, [roleName]);

  // Auto-clear toast after 3s
  useEffect(() => {
    if (!toast) return;
    const t = setTimeout(() => setToast(null), 3000);
    return () => clearTimeout(t);
  }, [toast]);

  const saveColumn = useCallback((key: string, value: boolean) => {
    // Debounce per-column to avoid rapid fire
    if (debounceTimers.current[key]) clearTimeout(debounceTimers.current[key]);
    debounceTimers.current[key] = setTimeout(() => {
      api.put(`/admin/column-visibility/${roleName}`, { columns: { [key]: value } })
        .then(() => setToast({ type: 'success', message: `Saved` }))
        .catch(() => setToast({ type: 'error', message: `Failed to save "${key}"` }));
    }, 500);
  }, [roleName]);

  const toggleColumn = useCallback((key: string) => {
    setColumns((prev) => {
      const newVal = !prev[key];
      saveColumn(key, newVal);
      return { ...prev, [key]: newVal };
    });
  }, [saveColumn]);

  const handleReset = async () => {
    setResetting(true);
    try {
      const res = await api.post(`/admin/column-visibility/${roleName}/reset`);
      setColumns(res.data.columns);
      setToast({ type: 'success', message: `Reset to defaults` });
    } catch {
      setToast({ type: 'error', message: 'Failed to reset' });
    } finally {
      setResetting(false);
    }
  };

  if (loading) {
    return <div className="text-xs text-gray-400 dark:text-gray-500 py-2">Loading columns...</div>;
  }

  return (
    <div className="space-y-3">
      {COLUMN_GROUPS.map((group) => {
        const visibleCount = group.columns.filter((col) => columns[col.key] ?? true).length;
        const totalCount = group.columns.length;
        return (
        <div key={group.label}>
          <h5 className="text-xs font-semibold text-gray-500 dark:text-gray-400 uppercase tracking-wider mb-1.5">
            {group.label} <span className="font-normal normal-case text-gray-400 dark:text-gray-500">({visibleCount}/{totalCount} visible)</span>
          </h5>
          <div className="grid grid-cols-2 sm:grid-cols-3 md:grid-cols-4 gap-2">
            {group.columns.map((col) => (
              <label
                key={col.key}
                className="flex items-center gap-2 text-sm text-gray-700 dark:text-gray-300 cursor-pointer select-none"
              >
                <button
                  type="button"
                  role="switch"
                  aria-checked={columns[col.key] ?? true}
                  onClick={() => toggleColumn(col.key)}
                  className={`relative inline-flex h-5 w-9 flex-shrink-0 rounded-full border-2 border-transparent transition-colors duration-200 ease-in-out focus:outline-none focus:ring-2 focus:ring-blue-500 focus:ring-offset-2 ${
                    columns[col.key] ?? true
                      ? 'bg-blue-600'
                      : 'bg-gray-300 dark:bg-gray-600'
                  }`}
                >
                  <span
                    className={`pointer-events-none inline-block h-4 w-4 transform rounded-full bg-white shadow ring-0 transition duration-200 ease-in-out ${
                      columns[col.key] ?? true ? 'translate-x-4' : 'translate-x-0'
                    }`}
                  />
                </button>
                <span className={columns[col.key] ?? true ? '' : 'text-gray-400 dark:text-gray-500 line-through'}>
                  {col.label}
                </span>
              </label>
            ))}
          </div>
        </div>
      );
    })}
      <div className="flex items-center gap-3 pt-1">
        <button
          onClick={handleReset}
          disabled={resetting}
          className="px-3 py-1 text-xs font-medium rounded-md border border-gray-300 dark:border-gray-600 text-gray-600 dark:text-gray-400 bg-white dark:bg-gray-800 hover:bg-gray-50 dark:hover:bg-gray-700 disabled:opacity-50 transition-colors"
        >
          {resetting ? 'Resetting...' : 'Reset to Default'}
        </button>
        {toast && (
          <span className={`text-xs font-medium ${toast.type === 'success' ? 'text-green-600 dark:text-green-400' : 'text-red-600 dark:text-red-400'}`}>
            {toast.message}
          </span>
        )}
      </div>
    </div>
  );
}

// ── Main RolesPage component ─────────────────────────────────────────────

export function RolesPage() {
  const [roles, setRoles] = useState<Role[]>([]);
  const [loading, setLoading] = useState(true);
  const [showForm, setShowForm] = useState(false);
  const [editId, setEditId] = useState<number | null>(null);
  const [form, setForm] = useState({ name: '', permissions: [] as string[] });
  const [saving, setSaving] = useState(false);
  const [formError, setFormError] = useState('');
  const [colVisOpen, setColVisOpen] = useState(false);
  const [colVisRole, setColVisRole] = useState<string | null>(null);

  const load = async () => {
    try {
      const rolesRes = await api.get('/admin/roles');
      setRoles(rolesRes.data);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => { load(); }, []);

  const togglePermission = (page: string) => {
    setForm((f) => ({
      ...f,
      permissions: f.permissions.includes(page)
        ? f.permissions.filter((p) => p !== page)
        : [...f.permissions, page],
    }));
  };

  const startEdit = (role: Role) => {
    setEditId(role.id);
    setForm({ name: role.name, permissions: [...role.permissions] });
    setShowForm(true);
  };

  const resetForm = () => {
    setShowForm(false);
    setEditId(null);
    setForm({ name: '', permissions: [] });
    setFormError('');
  };

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    setSaving(true);
    setFormError('');
    try {
      if (editId) {
        await api.patch(`/admin/roles/${editId}`, form);
      } else {
        await api.post('/admin/roles', form);
      }
      resetForm();
      load();
    } catch (err: any) {
      setFormError(err.response?.data?.detail || 'Failed to save role');
    } finally {
      setSaving(false);
    }
  };

  const deleteRole = async (id: number) => {
    if (!confirm('Delete this role?')) return;
    try {
      await api.delete(`/admin/roles/${id}`);
      load();
    } catch (err: any) {
      alert(err.response?.data?.detail || 'Failed to delete role');
    }
  };

  return (
    <div className="space-y-4">
      <div className="flex justify-between items-center">
        <p className="text-sm text-gray-500 dark:text-gray-400">{roles.length} role{roles.length !== 1 ? 's' : ''}</p>
        <button
          onClick={() => showForm && !editId ? resetForm() : setShowForm(true)}
          className="px-4 py-2 bg-blue-600 text-white rounded-md text-sm font-medium hover:bg-blue-700 transition-colors"
        >
          {showForm && !editId ? 'Cancel' : '+ Add Role'}
        </button>
      </div>

      {showForm && (
        <form onSubmit={handleSubmit} className="bg-white dark:bg-gray-800 rounded-lg shadow dark:shadow-gray-900 p-5 space-y-4">
          <h3 className="text-sm font-semibold text-gray-700 dark:text-gray-300">{editId ? 'Edit Role' : 'New Role'}</h3>
          <div>
            <label className="block text-xs font-medium text-gray-600 dark:text-gray-400 mb-1">Role Name *</label>
            <input
              required
              value={form.name}
              onChange={(e) => setForm({ ...form, name: e.target.value })}
              disabled={editId !== null && form.name === 'admin'}
              className="w-full max-w-xs border border-gray-300 dark:border-gray-600 rounded-md px-3 py-1.5 text-sm bg-white dark:bg-gray-800 text-gray-900 dark:text-gray-100 placeholder-gray-400 dark:placeholder-gray-500 focus:outline-none focus:ring-2 focus:ring-blue-500 disabled:bg-gray-50 dark:disabled:bg-gray-900"
            />
          </div>
          <div>
            <label className="block text-xs font-medium text-gray-600 dark:text-gray-400 mb-2">Page Access</label>
            <div className="space-y-3">
              {['Marketing', 'AI Calls', 'System', 'Admin'].map((section) => (
                <div key={section}>
                  <p className="text-xs text-gray-400 dark:text-gray-500 uppercase tracking-wider mb-1.5">{section}</p>
                  <div className="flex flex-wrap gap-3">
                    {NAV_PAGES.filter((p) => p.section === section).map((page) => (
                      <label key={page.key} className="flex items-center gap-2 text-sm text-gray-700 dark:text-gray-300 cursor-pointer">
                        <input
                          type="checkbox"
                          checked={form.permissions.includes(page.key)}
                          onChange={() => togglePermission(page.key)}
                          className="rounded border-gray-300 dark:border-gray-600 text-blue-600 focus:ring-blue-500"
                        />
                        {page.label}
                      </label>
                    ))}
                  </div>
                </div>
              ))}
            </div>
          </div>
          {formError && <p className="text-sm text-red-600">{formError}</p>}
          <div className="flex gap-2">
            <button
              type="submit"
              disabled={saving}
              className="px-4 py-1.5 bg-blue-600 text-white rounded-md text-sm font-medium hover:bg-blue-700 disabled:opacity-50 transition-colors"
            >
              {saving ? 'Saving...' : editId ? 'Update Role' : 'Create Role'}
            </button>
            <button type="button" onClick={resetForm} className="px-4 py-1.5 bg-gray-100 dark:bg-gray-700 text-gray-700 dark:text-gray-300 rounded-md text-sm font-medium hover:bg-gray-200 dark:hover:bg-gray-600 transition-colors">
              Cancel
            </button>
          </div>
        </form>
      )}

      <div className="bg-white dark:bg-gray-800 rounded-lg shadow dark:shadow-gray-900 overflow-hidden">
        {loading ? (
          <div className="p-12 text-center text-gray-400 dark:text-gray-500 text-sm">Loading...</div>
        ) : (
          <div className="overflow-x-auto">
            <table className="w-full">
              <thead className="bg-gray-50 dark:bg-gray-900">
                <tr>
                  {['Role Name', 'Page Access', 'Created', 'Actions'].map((h) => (
                    <th key={h} className="px-4 py-3 text-left text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider">
                      {h}
                    </th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {roles.map((r) => (
                  <tr key={r.id} className="border-b border-gray-100 dark:border-gray-700 hover:bg-gray-50 dark:hover:bg-gray-700">
                    <td className="px-4 py-3 text-sm font-medium text-gray-900 dark:text-gray-100">{r.name}</td>
                    <td className="px-4 py-3 text-sm text-gray-600 dark:text-gray-400">
                      <div className="flex flex-wrap gap-1">
                        {r.permissions.length === 0 ? (
                          <span className="text-gray-400 dark:text-gray-500">No access</span>
                        ) : (
                          r.permissions.map((p) => (
                            <span key={p} className="px-2 py-0.5 rounded-full text-xs bg-gray-100 dark:bg-gray-700 text-gray-700 dark:text-gray-300">
                              {PAGE_LABELS[p] ?? p}
                            </span>
                          ))
                        )}
                      </div>
                    </td>
                    <td className="px-4 py-3 text-sm text-gray-500 dark:text-gray-400 whitespace-nowrap">
                      {new Date(r.created_at).toLocaleDateString()}
                    </td>
                    <td className="px-4 py-3 text-sm flex gap-2">
                      {r.name !== 'admin' && (
                        <>
                          <button onClick={() => startEdit(r)} className="text-xs text-blue-600 hover:underline">Edit</button>
                          <button onClick={() => deleteRole(r.id)} className="text-xs text-red-500 hover:underline">Delete</button>
                        </>
                      )}
                      {r.name === 'admin' && <span className="text-xs text-gray-400 dark:text-gray-500">Protected</span>}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </div>

      {/* CLAUD-85: Retention Grid Column Visibility */}
      <div className="bg-white dark:bg-gray-800 rounded-lg shadow dark:shadow-gray-900 overflow-hidden">
        <button
          onClick={() => setColVisOpen((v) => !v)}
          className="w-full flex items-center justify-between px-5 py-4 text-left hover:bg-gray-50 dark:hover:bg-gray-700 transition-colors"
        >
          <div>
            <h3 className="text-sm font-semibold text-gray-700 dark:text-gray-300">Retention Grid Columns</h3>
            <p className="text-xs text-gray-500 dark:text-gray-400 mt-0.5">Configure which columns each role can see in the Retention Manager grid</p>
          </div>
          <span className="text-gray-400 dark:text-gray-500 text-lg">{colVisOpen ? '\u25B2' : '\u25BC'}</span>
        </button>

        {colVisOpen && (
          <div className="px-5 pb-5 space-y-6 border-t border-gray-100 dark:border-gray-700 pt-4">
            {loading ? (
              <div className="text-sm text-gray-400 dark:text-gray-500">Loading roles...</div>
            ) : (
              <>
                {/* Role selector tabs */}
                <div className="flex flex-wrap gap-2">
                  {roles.map((r) => (
                    <button
                      key={r.id}
                      onClick={() => setColVisRole(colVisRole === r.name ? null : r.name)}
                      className={`px-3 py-1.5 text-xs font-medium rounded-md border transition-colors ${
                        colVisRole === r.name
                          ? 'bg-blue-600 text-white border-blue-600'
                          : 'bg-white dark:bg-gray-800 text-gray-700 dark:text-gray-300 border-gray-300 dark:border-gray-600 hover:bg-gray-50 dark:hover:bg-gray-700'
                      }`}
                    >
                      {r.name}
                    </button>
                  ))}
                </div>

                {/* Column toggles for selected role */}
                {colVisRole && (
                  <div className="bg-gray-50 dark:bg-gray-900 rounded-lg p-4">
                    <h4 className="text-sm font-medium text-gray-700 dark:text-gray-300 mb-3">
                      Column visibility for <span className="font-semibold text-blue-600">{colVisRole}</span>
                    </h4>
                    <ColumnVisibilitySection key={colVisRole} roleName={colVisRole} />
                  </div>
                )}

                {!colVisRole && (
                  <p className="text-xs text-gray-400 dark:text-gray-500">Select a role above to configure its column visibility.</p>
                )}
              </>
            )}
          </div>
        )}
      </div>
    </div>
  );
}
