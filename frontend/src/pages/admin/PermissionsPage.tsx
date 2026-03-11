import { useEffect, useState } from 'react';
import { useAuthStore } from '../../store/useAuthStore';
import {
  fetchAllPermissions,
  togglePermission,
  type PermissionRow,
} from '../../api/permissions';

/** Format a snake_case action name as a readable label. */
function formatAction(action: string): string {
  return action
    .replace(/_/g, ' ')
    .replace(/\b\w/g, (c) => c.toUpperCase());
}

/** Format a snake_case role name as a readable label. */
function formatRole(role: string): string {
  return role
    .replace(/_/g, ' ')
    .replace(/\b\w/g, (c) => c.toUpperCase());
}

export function PermissionsPage() {
  const token = useAuthStore((s) => s.token);
  const [permissions, setPermissions] = useState<PermissionRow[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState('');
  const [toggling, setToggling] = useState<Set<number>>(new Set());

  const load = async () => {
    if (!token) return;
    try {
      const data = await fetchAllPermissions(token);
      setPermissions(data);
      setError('');
    } catch (err: any) {
      setError(err.response?.data?.detail || 'Failed to load permissions');
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    load();
  }, []);

  const handleToggle = async (perm: PermissionRow) => {
    if (!token) return;
    setToggling((prev) => new Set(prev).add(perm.id));
    try {
      const updated = await togglePermission(token, perm.id, !perm.enabled);
      setPermissions((prev) =>
        prev.map((p) => (p.id === updated.id ? updated : p))
      );
    } catch (err: any) {
      setError(err.response?.data?.detail || 'Failed to toggle permission');
    } finally {
      setToggling((prev) => {
        const next = new Set(prev);
        next.delete(perm.id);
        return next;
      });
    }
  };

  if (loading) {
    return <div className="text-center text-gray-400 dark:text-gray-500 py-12">Loading...</div>;
  }

  if (error) {
    return (
      <div className="text-center py-12">
        <p className="text-red-600 text-sm">{error}</p>
        <button
          onClick={() => { setLoading(true); load(); }}
          className="mt-2 text-sm text-blue-600 hover:underline"
        >
          Retry
        </button>
      </div>
    );
  }

  // Group permissions by role for a matrix display
  const roles = [...new Set(permissions.map((p) => p.role))];
  const actions = [...new Set(permissions.map((p) => p.action))];

  // Build a lookup: role -> action -> PermissionRow
  const lookup: Record<string, Record<string, PermissionRow>> = {};
  for (const perm of permissions) {
    if (!lookup[perm.role]) lookup[perm.role] = {};
    lookup[perm.role][perm.action] = perm;
  }

  return (
    <div className="space-y-6">
      <div>
        <h2 className="text-base font-semibold text-gray-700 dark:text-gray-300 mb-1">CRM Action Permissions</h2>
        <p className="text-sm text-gray-500 dark:text-gray-400 mb-4">
          Control which actions each role can perform on client records. Changes take effect on the user's next login.
        </p>
      </div>

      <div className="bg-white dark:bg-gray-800 rounded-lg shadow dark:shadow-gray-900 overflow-x-auto">
        <table className="min-w-full divide-y divide-gray-200 dark:divide-gray-700">
          <thead className="bg-gray-50 dark:bg-gray-900">
            <tr>
              <th className="px-4 py-3 text-left text-xs font-semibold text-gray-600 dark:text-gray-400 uppercase tracking-wider">
                Role
              </th>
              {actions.map((action) => (
                <th
                  key={action}
                  className="px-4 py-3 text-center text-xs font-semibold text-gray-600 dark:text-gray-400 uppercase tracking-wider"
                >
                  {formatAction(action)}
                </th>
              ))}
            </tr>
          </thead>
          <tbody className="divide-y divide-gray-100 dark:divide-gray-700">
            {roles.map((role) => (
              <tr key={role} className="hover:bg-gray-50 dark:hover:bg-gray-700">
                <td className="px-4 py-3 text-sm font-medium text-gray-800 dark:text-gray-100 whitespace-nowrap">
                  {formatRole(role)}
                </td>
                {actions.map((action) => {
                  const perm = lookup[role]?.[action];
                  if (!perm) {
                    return (
                      <td key={action} className="px-4 py-3 text-center text-gray-300 dark:text-gray-600 text-xs">
                        --
                      </td>
                    );
                  }
                  const isToggling = toggling.has(perm.id);
                  const isAdminLocked = role === 'admin';
                  return (
                    <td key={action} className="px-4 py-3 text-center">
                      <button
                        onClick={() => !isAdminLocked && handleToggle(perm)}
                        disabled={isToggling || isAdminLocked}
                        className={`relative inline-flex h-6 w-11 items-center rounded-full transition-colors focus:outline-none focus:ring-2 focus:ring-blue-500 focus:ring-offset-1 ${
                          perm.enabled ? 'bg-blue-600' : 'bg-gray-300 dark:bg-gray-600'
                        } ${isToggling ? 'opacity-50 cursor-wait' : isAdminLocked ? 'opacity-60 cursor-not-allowed' : 'cursor-pointer'}`}
                        title={isAdminLocked ? 'Admin permissions are locked' : perm.enabled ? 'Enabled -- click to disable' : 'Disabled -- click to enable'}
                      >
                        <span
                          className={`inline-block h-4 w-4 transform rounded-full bg-white shadow transition-transform ${
                            perm.enabled ? 'translate-x-6' : 'translate-x-1'
                          }`}
                        />
                      </button>
                    </td>
                  );
                })}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}
