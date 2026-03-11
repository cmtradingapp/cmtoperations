import { useEffect, useState } from 'react';
import { useNavigate } from 'react-router-dom';
import axios from 'axios';

import { useAuthStore } from '../../store/useAuthStore';

const api = axios.create({ baseURL: import.meta.env.VITE_API_BASE_URL || '/api' });
api.interceptors.request.use((config) => {
  const auth = JSON.parse(localStorage.getItem('auth') || '{}');
  const token = auth?.state?.token;
  if (token) config.headers.Authorization = `Bearer ${token}`;
  return config;
});

interface User {
  id: number;
  username: string;
  email: string | null;
  role: string;
  is_active: boolean;
  created_at: string;
  office: string | null;
  department: string | null;
  team: string | null;
}

interface Role {
  id: number;
  name: string;
}

interface Toast {
  type: 'success' | 'error';
  message: string;
}

export function UsersPage() {
  const navigate = useNavigate();
  const { role: currentRole, setImpersonation } = useAuthStore();
  const isAdmin = currentRole === 'admin';

  const [users, setUsers] = useState<User[]>([]);
  const [roles, setRoles] = useState<Role[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState('');
  const [showForm, setShowForm] = useState(false);
  const [form, setForm] = useState({ username: '', email: '', password: '', role: 'user' });
  const [saving, setSaving] = useState(false);
  const [formError, setFormError] = useState('');

  // Sync button state
  const [syncing, setSyncing] = useState(false);
  const [toast, setToast] = useState<Toast | null>(null);

  // Login As state
  const [loginAsLoadingId, setLoginAsLoadingId] = useState<number | null>(null);

  // Search state
  const [searchQuery, setSearchQuery] = useState('');

  // Inline edit state
  const [editingId, setEditingId] = useState<number | null>(null);
  const [editFields, setEditFields] = useState({ role: '', office: '', department: '', team: '', password: '' });

  const startEdit = (u: User) => {
    setEditingId(u.id);
    setEditFields({ role: u.role, office: u.office ?? '', department: u.department ?? '', team: u.team ?? '', password: '' });
  };

  const saveEdit = async (id: number) => {
    const payload: Record<string, string> = {
      role: editFields.role,
      office: editFields.office,
      department: editFields.department,
      team: editFields.team,
    };
    if (editFields.password) payload.password = editFields.password;
    await api.patch(`/admin/users/${id}`, payload);
    setEditingId(null);
    load();
  };

  const showToast = (type: 'success' | 'error', message: string) => {
    setToast({ type, message });
    setTimeout(() => setToast(null), 5000);
  };

  const load = async () => {
    try {
      const [usersRes, rolesRes] = await Promise.all([
        api.get('/admin/users'),
        api.get('/admin/roles'),
      ]);
      setUsers(usersRes.data);
      setRoles(rolesRes.data);
    } catch {
      setError('Failed to load users');
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => { load(); }, []);

  const handleCreate = async (e: React.FormEvent) => {
    e.preventDefault();
    setSaving(true);
    setFormError('');
    try {
      await api.post('/admin/users', form);
      setForm({ username: '', email: '', password: '', role: 'user' });
      setShowForm(false);
      load();
    } catch (err: any) {
      setFormError(err.response?.data?.detail || 'Failed to create user');
    } finally {
      setSaving(false);
    }
  };

  const toggleActive = async (user: User) => {
    await api.patch(`/admin/users/${user.id}`, { is_active: !user.is_active });
    load();
  };

  const deleteUser = async (id: number) => {
    if (!confirm('Delete this user?')) return;
    await api.delete(`/admin/users/${id}`);
    load();
  };

  const handleSyncRetentionUsers = async () => {
    setSyncing(true);
    try {
      const res = await api.post<{ created: number; skipped: number }>('/admin/sync-vtiger-users');
      showToast('success', `Sync complete: ${res.data.created} created, ${res.data.skipped} skipped`);
      load();
    } catch (err: any) {
      showToast('error', err.response?.data?.detail || 'Sync failed. Please try again.');
    } finally {
      setSyncing(false);
    }
  };

  const handleLoginAs = async (user: User) => {
    setLoginAsLoadingId(user.id);
    try {
      const res = await api.post<{ access_token: string; username: string; role: string; permissions: string[] }>(
        `/admin/login-as/${user.id}`
      );
      setImpersonation(res.data.access_token, res.data.username, res.data.role, res.data.permissions ?? []);
      navigate('/retention');
    } catch (err: any) {
      showToast('error', err.response?.data?.detail || 'Login As failed. Please try again.');
    } finally {
      setLoginAsLoadingId(null);
    }
  };

  return (
    <div className="space-y-4">
      {/* Toast notification */}
      {toast && (
        <div
          className={`fixed top-4 right-4 z-50 px-5 py-3 rounded-md shadow-lg text-sm font-medium transition-all ${
            toast.type === 'success'
              ? 'bg-green-50 dark:bg-green-900/20 text-green-800 dark:text-green-300 border border-green-200 dark:border-green-800'
              : 'bg-red-50 dark:bg-red-900/20 text-red-800 dark:text-red-300 border border-red-200 dark:border-red-800'
          }`}
        >
          {toast.message}
        </div>
      )}

      {/* Top toolbar */}
      <div className="flex justify-between items-center flex-wrap gap-3">
        <p className="text-sm text-gray-500 dark:text-gray-400">{users.length} user{users.length !== 1 ? 's' : ''}</p>
        <div className="flex items-center gap-3">
          {isAdmin && (
            <button
              onClick={handleSyncRetentionUsers}
              disabled={syncing}
              className="px-4 py-2 bg-amber-500 text-white rounded-md text-sm font-medium hover:bg-amber-600 disabled:opacity-50 transition-colors"
            >
              {syncing ? 'Syncing…' : 'Sync Retention Users'}
            </button>
          )}
          <button
            onClick={() => setShowForm(!showForm)}
            className="px-4 py-2 bg-blue-600 text-white rounded-md text-sm font-medium hover:bg-blue-700 transition-colors"
          >
            {showForm ? 'Cancel' : '+ Add User'}
          </button>
        </div>
      </div>

      {showForm && (
        <form onSubmit={handleCreate} className="bg-white dark:bg-gray-800 rounded-lg shadow p-5 space-y-3">
          <h3 className="text-sm font-semibold text-gray-700 dark:text-gray-300">New User</h3>
          <div className="grid grid-cols-1 md:grid-cols-2 gap-3">
            <div>
              <label className="block text-xs font-medium text-gray-600 dark:text-gray-400 mb-1">Username *</label>
              <input
                required
                value={form.username}
                onChange={(e) => setForm({ ...form, username: e.target.value })}
                className="w-full border border-gray-300 dark:border-gray-600 rounded-md px-3 py-1.5 text-sm bg-white dark:bg-gray-700 text-gray-900 dark:text-gray-100 focus:outline-none focus:ring-2 focus:ring-blue-500"
              />
            </div>
            <div>
              <label className="block text-xs font-medium text-gray-600 dark:text-gray-400 mb-1">Email</label>
              <input
                type="email"
                value={form.email}
                onChange={(e) => setForm({ ...form, email: e.target.value })}
                className="w-full border border-gray-300 dark:border-gray-600 rounded-md px-3 py-1.5 text-sm bg-white dark:bg-gray-700 text-gray-900 dark:text-gray-100 focus:outline-none focus:ring-2 focus:ring-blue-500"
              />
            </div>
            <div>
              <label className="block text-xs font-medium text-gray-600 dark:text-gray-400 mb-1">Password *</label>
              <input
                required
                type="password"
                value={form.password}
                onChange={(e) => setForm({ ...form, password: e.target.value })}
                className="w-full border border-gray-300 dark:border-gray-600 rounded-md px-3 py-1.5 text-sm bg-white dark:bg-gray-700 text-gray-900 dark:text-gray-100 focus:outline-none focus:ring-2 focus:ring-blue-500"
              />
            </div>
            <div>
              <label className="block text-xs font-medium text-gray-600 dark:text-gray-400 mb-1">Role *</label>
              <select
                value={form.role}
                onChange={(e) => setForm({ ...form, role: e.target.value })}
                className="w-full border border-gray-300 dark:border-gray-600 rounded-md px-3 py-1.5 text-sm bg-white dark:bg-gray-700 text-gray-900 dark:text-gray-100 focus:outline-none focus:ring-2 focus:ring-blue-500"
              >
                {roles.map((r) => (
                  <option key={r.id} value={r.name}>{r.name}</option>
                ))}
              </select>
            </div>
          </div>
          {formError && <p className="text-sm text-red-600">{formError}</p>}
          <button
            type="submit"
            disabled={saving}
            className="px-4 py-1.5 bg-blue-600 text-white rounded-md text-sm font-medium hover:bg-blue-700 disabled:opacity-50 transition-colors"
          >
            {saving ? 'Creating…' : 'Create User'}
          </button>
        </form>
      )}

      {/* Search bar — sticky so it stays visible even when the Add User form is open */}
      <div className="sticky top-0 z-20 bg-white dark:bg-gray-800 border border-gray-200 dark:border-gray-700 rounded-lg shadow-sm px-4 py-3">
        <input
          type="text"
          value={searchQuery}
          onChange={(e) => setSearchQuery(e.target.value)}
          placeholder="Search by name, email, role, office, department…"
          className="w-full border border-gray-300 dark:border-gray-600 rounded-md px-3 py-1.5 text-sm bg-white dark:bg-gray-700 text-gray-900 dark:text-gray-100 focus:outline-none focus:ring-2 focus:ring-blue-500"
        />
      </div>

      <div className="bg-white dark:bg-gray-800 rounded-lg shadow overflow-hidden">
        {loading ? (
          <div className="p-12 text-center text-gray-400 dark:text-gray-500 text-sm">Loading…</div>
        ) : error ? (
          <div className="p-6 text-red-600 text-sm">{error}</div>
        ) : (
          <div className="overflow-x-auto overflow-y-auto" style={{ maxHeight: 'calc(100vh - 380px)' }}>
            <table className="w-full">
              <thead className="bg-gray-50 dark:bg-gray-700 sticky top-0 z-10 shadow-[0_1px_0_0_rgba(229,231,235,1)]">
                <tr>
                  {['ID', 'Username', 'Email', 'Role', 'Office', 'Department', 'Team', 'New Password', 'Status', 'Created', 'Actions'].map((h) => (
                    <th key={h} className="px-4 py-3 text-left text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider">
                      {h}
                    </th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {users.filter((u) => {
                  if (!searchQuery) return true;
                  const q = searchQuery.toLowerCase();
                  return (
                    u.username.toLowerCase().includes(q) ||
                    (u.email ?? '').toLowerCase().includes(q) ||
                    u.role.toLowerCase().includes(q) ||
                    (u.office ?? '').toLowerCase().includes(q) ||
                    (u.department ?? '').toLowerCase().includes(q) ||
                    (u.team ?? '').toLowerCase().includes(q)
                  );
                }).map((u) => (
                  <tr key={u.id} className="border-b border-gray-100 dark:border-gray-700 hover:bg-gray-50 dark:hover:bg-gray-700">
                    <td className="px-4 py-3 text-sm text-gray-500 dark:text-gray-400">{u.id}</td>
                    <td className="px-4 py-3 text-sm font-medium text-gray-900 dark:text-gray-100">{u.username}</td>
                    <td className="px-4 py-3 text-sm text-gray-600 dark:text-gray-400">{u.email ?? '—'}</td>
                    <td className="px-4 py-3 text-sm">
                      {editingId === u.id ? (
                        <select
                          value={editFields.role}
                          onChange={(e) => setEditFields({ ...editFields, role: e.target.value })}
                          className="w-full border border-gray-300 dark:border-gray-600 rounded px-2 py-0.5 text-sm bg-white dark:bg-gray-700 text-gray-900 dark:text-gray-100 focus:outline-none focus:ring-1 focus:ring-blue-500"
                        >
                          {roles.map((r) => <option key={r.id} value={r.name}>{r.name}</option>)}
                          <option value="admin">admin</option>
                        </select>
                      ) : (
                        <span className="px-2 py-0.5 rounded-full text-xs font-medium bg-blue-100 text-blue-800 dark:bg-blue-900 dark:text-blue-200">
                          {u.role}
                        </span>
                      )}
                    </td>
                    <td className="px-4 py-3 text-sm text-gray-600 dark:text-gray-400">
                      {editingId === u.id ? (
                        <input
                          value={editFields.office}
                          onChange={(e) => setEditFields({ ...editFields, office: e.target.value })}
                          className="w-full border border-gray-300 dark:border-gray-600 rounded px-2 py-0.5 text-sm bg-white dark:bg-gray-700 text-gray-900 dark:text-gray-100 focus:outline-none focus:ring-1 focus:ring-blue-500"
                        />
                      ) : (u.office ?? '—')}
                    </td>
                    <td className="px-4 py-3 text-sm text-gray-600 dark:text-gray-400">
                      {editingId === u.id ? (
                        <input
                          value={editFields.department}
                          onChange={(e) => setEditFields({ ...editFields, department: e.target.value })}
                          className="w-full border border-gray-300 dark:border-gray-600 rounded px-2 py-0.5 text-sm bg-white dark:bg-gray-700 text-gray-900 dark:text-gray-100 focus:outline-none focus:ring-1 focus:ring-blue-500"
                        />
                      ) : (u.department ?? '—')}
                    </td>
                    <td className="px-4 py-3 text-sm text-gray-600 dark:text-gray-400">
                      {editingId === u.id ? (
                        <input
                          value={editFields.team}
                          onChange={(e) => setEditFields({ ...editFields, team: e.target.value })}
                          placeholder="e.g. Team Alpha"
                          className="w-full border border-gray-300 dark:border-gray-600 rounded px-2 py-0.5 text-sm bg-white dark:bg-gray-700 text-gray-900 dark:text-gray-100 focus:outline-none focus:ring-1 focus:ring-blue-500"
                        />
                      ) : (u.team ? <span className="px-1.5 py-0.5 rounded text-xs bg-indigo-100 text-indigo-700 dark:bg-indigo-900/40 dark:text-indigo-300">{u.team}</span> : '—')}
                    </td>
                    <td className="px-4 py-3 text-sm text-gray-600 dark:text-gray-400">
                      {editingId === u.id ? (
                        <input
                          type="password"
                          placeholder="Leave blank to keep"
                          value={editFields.password}
                          onChange={(e) => setEditFields({ ...editFields, password: e.target.value })}
                          className="w-full border border-gray-300 dark:border-gray-600 rounded px-2 py-0.5 text-sm bg-white dark:bg-gray-700 text-gray-900 dark:text-gray-100 focus:outline-none focus:ring-1 focus:ring-blue-500"
                        />
                      ) : '—'}
                    </td>
                    <td className="px-4 py-3 text-sm">
                      <span className={`px-2 py-0.5 rounded-full text-xs font-medium ${u.is_active ? 'bg-green-100 text-green-800 dark:bg-green-900 dark:text-green-200' : 'bg-gray-100 text-gray-500 dark:bg-gray-700 dark:text-gray-400'}`}>
                        {u.is_active ? 'Active' : 'Inactive'}
                      </span>
                    </td>
                    <td className="px-4 py-3 text-sm text-gray-500 dark:text-gray-400 whitespace-nowrap">
                      {new Date(u.created_at).toLocaleDateString()}
                    </td>
                    <td className="px-4 py-3 text-sm">
                      <div className="flex items-center gap-2 flex-wrap">
                        {editingId === u.id ? (
                          <>
                            <button onClick={() => saveEdit(u.id)} className="text-xs text-green-600 hover:underline font-medium">Save</button>
                            <button onClick={() => setEditingId(null)} className="text-xs text-gray-500 dark:text-gray-400 hover:underline">Cancel</button>
                          </>
                        ) : (
                          <button onClick={() => startEdit(u)} className="text-xs text-gray-600 dark:text-gray-400 hover:underline">Edit</button>
                        )}
                        <button
                          onClick={() => toggleActive(u)}
                          className="text-xs text-blue-600 hover:underline"
                        >
                          {u.is_active ? 'Deactivate' : 'Activate'}
                        </button>
                        <button
                          onClick={() => deleteUser(u.id)}
                          className="text-xs text-red-500 hover:underline"
                        >
                          Delete
                        </button>
                        {isAdmin && u.role !== 'admin' && (
                          <button
                            onClick={() => handleLoginAs(u)}
                            disabled={loginAsLoadingId === u.id}
                            className="text-xs px-2 py-0.5 rounded bg-purple-100 text-purple-700 hover:bg-purple-200 disabled:opacity-50 transition-colors font-medium"
                          >
                            {loginAsLoadingId === u.id ? 'Loading…' : 'Login As'}
                          </button>
                        )}
                      </div>
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
