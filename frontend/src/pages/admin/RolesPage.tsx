import { useEffect, useState } from 'react';
import axios from 'axios';

const api = axios.create({ baseURL: import.meta.env.VITE_API_BASE_URL || '/api' });
api.interceptors.request.use((config) => {
  const auth = JSON.parse(localStorage.getItem('auth') || '{}');
  const token = auth?.state?.token;
  if (token) config.headers.Authorization = `Bearer ${token}`;
  return config;
});

const PAGE_LABELS: Record<string, string> = {
  // Admin
  'users': 'Users',
  'roles': 'Roles',
  'permissions': 'Permissions',
  'integrations': 'Integrations & Config',
  'audit-log': 'Audit Log',
  // Marketing
  'challenges': 'Challenges',
  'action-bonuses': 'Automatic Bonus',
  // AI Calls
  'call-manager': 'Call Manager',
  'call-history': 'Call History',
  'call-dashboard': 'AI Call Dashboard',
  'batch-call': 'Batch Call from File',
  'elena-ai-upload': 'Upload to Campaign',
};

interface Role {
  id: number;
  name: string;
  permissions: string[];
  created_at: string;
}

export function RolesPage() {
  const [roles, setRoles] = useState<Role[]>([]);
  const [allPages, setAllPages] = useState<string[]>([]);
  const [loading, setLoading] = useState(true);
  const [showForm, setShowForm] = useState(false);
  const [editId, setEditId] = useState<number | null>(null);
  const [form, setForm] = useState({ name: '', permissions: [] as string[] });
  const [saving, setSaving] = useState(false);
  const [formError, setFormError] = useState('');

  const load = async () => {
    try {
      const [rolesRes, pagesRes] = await Promise.all([
        api.get('/admin/roles'),
        api.get('/admin/pages'),
      ]);
      setRoles(rolesRes.data);
      setAllPages(pagesRes.data);
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
    } catch (err: unknown) {
      const e = err as { response?: { data?: { detail?: string } } };
      setFormError(e.response?.data?.detail || 'Failed to save role');
    } finally {
      setSaving(false);
    }
  };

  const deleteRole = async (id: number) => {
    if (!confirm('Delete this role?')) return;
    try {
      await api.delete(`/admin/roles/${id}`);
      load();
    } catch (err: unknown) {
      const e = err as { response?: { data?: { detail?: string } } };
      alert(e.response?.data?.detail || 'Failed to delete role');
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
            <input required value={form.name} onChange={(e) => setForm({ ...form, name: e.target.value })}
              disabled={editId !== null && form.name === 'admin'}
              className="w-full max-w-xs border border-gray-300 dark:border-gray-600 rounded-md px-3 py-1.5 text-sm bg-white dark:bg-gray-800 text-gray-900 dark:text-gray-100 focus:outline-none focus:ring-2 focus:ring-blue-500 disabled:bg-gray-50 dark:disabled:bg-gray-900" />
          </div>
          <div>
            <label className="block text-xs font-medium text-gray-600 dark:text-gray-400 mb-2">Page Access</label>
            <div className="flex flex-wrap gap-3">
              {allPages.map((page) => (
                <label key={page} className="flex items-center gap-2 text-sm text-gray-700 dark:text-gray-300 cursor-pointer">
                  <input type="checkbox" checked={form.permissions.includes(page)} onChange={() => togglePermission(page)}
                    className="rounded border-gray-300 dark:border-gray-600 text-blue-600 focus:ring-blue-500" />
                  {PAGE_LABELS[page] ?? page}
                </label>
              ))}
            </div>
          </div>
          {formError && <p className="text-sm text-red-600">{formError}</p>}
          <div className="flex gap-2">
            <button type="submit" disabled={saving}
              className="px-4 py-1.5 bg-blue-600 text-white rounded-md text-sm font-medium hover:bg-blue-700 disabled:opacity-50 transition-colors">
              {saving ? 'Saving...' : editId ? 'Update Role' : 'Create Role'}
            </button>
            <button type="button" onClick={resetForm}
              className="px-4 py-1.5 bg-gray-100 dark:bg-gray-700 text-gray-700 dark:text-gray-300 rounded-md text-sm font-medium hover:bg-gray-200 dark:hover:bg-gray-600 transition-colors">
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
                    <th key={h} className="px-4 py-3 text-left text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider">{h}</th>
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
                        ) : r.permissions.map((p) => (
                          <span key={p} className="px-2 py-0.5 rounded-full text-xs bg-gray-100 dark:bg-gray-700 text-gray-700 dark:text-gray-300">
                            {PAGE_LABELS[p] ?? p}
                          </span>
                        ))}
                      </div>
                    </td>
                    <td className="px-4 py-3 text-sm text-gray-500 dark:text-gray-400 whitespace-nowrap">
                      {new Date(r.created_at).toLocaleDateString()}
                    </td>
                    <td className="px-4 py-3 text-sm flex gap-2">
                      {r.name !== 'admin' ? (
                        <>
                          <button onClick={() => startEdit(r)} className="text-xs text-blue-600 hover:underline">Edit</button>
                          <button onClick={() => deleteRole(r.id)} className="text-xs text-red-500 hover:underline">Delete</button>
                        </>
                      ) : (
                        <span className="text-xs text-gray-400 dark:text-gray-500">Protected</span>
                      )}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </div>
    </div>
  );
}
