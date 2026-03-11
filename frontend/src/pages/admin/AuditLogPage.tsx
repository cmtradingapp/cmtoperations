import { useCallback, useEffect, useState } from 'react';
import axios from 'axios';

const api = axios.create({ baseURL: import.meta.env.VITE_API_BASE_URL || '/api' });
api.interceptors.request.use((config) => {
  const auth = JSON.parse(localStorage.getItem('auth') || '{}');
  const token = auth?.state?.token;
  if (token) config.headers.Authorization = `Bearer ${token}`;
  return config;
});

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

type ActionType = 'status_change' | 'note_added' | 'call_initiated' | 'whatsapp_opened';

interface AuditLogEntry {
  id: number;
  agent_id: number;
  agent_username: string;
  client_account_id: string;
  action_type: ActionType;
  action_value: string | null;
  timestamp: string;
}

interface AuditLogResponse {
  items: AuditLogEntry[];
  total: number;
  page: number;
  page_size: number;
  pages: number;
}

interface Filters {
  agent_username: string;
  action_type: string;
  date_from: string;
  date_to: string;
}

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const PAGE_SIZE = 50;

const ACTION_TYPE_OPTIONS: { value: string; label: string }[] = [
  { value: '', label: 'All Actions' },
  { value: 'status_change', label: 'Status Change' },
  { value: 'note_added', label: 'Note Added' },
  { value: 'call_initiated', label: 'Call' },
  { value: 'whatsapp_opened', label: 'WhatsApp' },
];

const ACTION_BADGE_CONFIG: Record<
  ActionType,
  { label: string; className: string }
> = {
  status_change: {
    label: 'Status Change',
    className: 'bg-blue-100 text-blue-800 dark:bg-blue-900 dark:text-blue-200',
  },
  note_added: {
    label: 'Note Added',
    className: 'bg-green-100 text-green-800 dark:bg-green-900 dark:text-green-200',
  },
  call_initiated: {
    label: 'Call',
    className: 'bg-purple-100 text-purple-800 dark:bg-purple-900 dark:text-purple-200',
  },
  whatsapp_opened: {
    label: 'WhatsApp',
    className: 'bg-green-100 text-green-800 dark:bg-green-900 dark:text-green-200',
  },
};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function ActionBadge({ actionType }: { actionType: string }) {
  const config = ACTION_BADGE_CONFIG[actionType as ActionType];
  if (!config) {
    return (
      <span className="px-2 py-0.5 rounded-full text-xs font-medium bg-gray-100 dark:bg-gray-700 text-gray-600 dark:text-gray-400">
        {actionType}
      </span>
    );
  }
  return (
    <span className={`px-2 py-0.5 rounded-full text-xs font-medium ${config.className}`}>
      {config.label}
    </span>
  );
}

// ---------------------------------------------------------------------------
// Main component
// ---------------------------------------------------------------------------

export function AuditLogPage() {
  const [data, setData] = useState<AuditLogResponse | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState('');
  const [page, setPage] = useState(1);

  // Pending filter values (controlled by form inputs)
  const [pendingFilters, setPendingFilters] = useState<Filters>({
    agent_username: '',
    action_type: '',
    date_from: '',
    date_to: '',
  });

  // Applied filters (used in the actual API call)
  const [appliedFilters, setAppliedFilters] = useState<Filters>({
    agent_username: '',
    action_type: '',
    date_from: '',
    date_to: '',
  });

  const fetchData = useCallback(async (currentPage: number, filters: Filters) => {
    setLoading(true);
    setError('');
    try {
      const params: Record<string, string | number> = {
        page: currentPage,
        page_size: PAGE_SIZE,
      };
      if (filters.agent_username) params.agent_username = filters.agent_username;
      if (filters.action_type) params.action_type = filters.action_type;
      if (filters.date_from) params.date_from = filters.date_from;
      if (filters.date_to) params.date_to = filters.date_to;

      const res = await api.get<AuditLogResponse>('/admin/audit-log', { params });
      setData(res.data);
    } catch (err: any) {
      setError(err.response?.data?.detail || 'Failed to load audit log');
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    fetchData(page, appliedFilters);
  }, [fetchData, page, appliedFilters]);

  const handleApplyFilters = () => {
    setPage(1);
    setAppliedFilters({ ...pendingFilters });
  };

  const handlePageChange = (newPage: number) => {
    setPage(newPage);
  };

  const totalPages = data?.pages ?? 0;

  return (
    <div className="space-y-4">
      {/* Filter bar */}
      <div className="bg-white dark:bg-gray-800 rounded-lg shadow dark:shadow-gray-900 p-4">
        <div className="flex flex-wrap gap-3 items-end">
          {/* Agent username */}
          <div>
            <label className="block text-xs font-medium text-gray-600 dark:text-gray-400 mb-1">Agent Username</label>
            <input
              type="text"
              value={pendingFilters.agent_username}
              onChange={(e) =>
                setPendingFilters((f) => ({ ...f, agent_username: e.target.value }))
              }
              placeholder="Filter by agent..."
              className="border border-gray-300 dark:border-gray-600 rounded-md px-3 py-1.5 text-sm bg-white dark:bg-gray-800 text-gray-900 dark:text-gray-100 placeholder-gray-400 dark:placeholder-gray-500 focus:outline-none focus:ring-2 focus:ring-blue-500 w-44"
            />
          </div>

          {/* Action type */}
          <div>
            <label className="block text-xs font-medium text-gray-600 dark:text-gray-400 mb-1">Action Type</label>
            <select
              value={pendingFilters.action_type}
              onChange={(e) =>
                setPendingFilters((f) => ({ ...f, action_type: e.target.value }))
              }
              className="border border-gray-300 dark:border-gray-600 rounded-md px-3 py-1.5 text-sm bg-white dark:bg-gray-800 text-gray-900 dark:text-gray-100 focus:outline-none focus:ring-2 focus:ring-blue-500 w-44"
            >
              {ACTION_TYPE_OPTIONS.map((opt) => (
                <option key={opt.value} value={opt.value}>
                  {opt.label}
                </option>
              ))}
            </select>
          </div>

          {/* Date from */}
          <div>
            <label className="block text-xs font-medium text-gray-600 dark:text-gray-400 mb-1">Date From</label>
            <input
              type="date"
              value={pendingFilters.date_from}
              onChange={(e) =>
                setPendingFilters((f) => ({ ...f, date_from: e.target.value }))
              }
              className="border border-gray-300 dark:border-gray-600 rounded-md px-3 py-1.5 text-sm bg-white dark:bg-gray-800 text-gray-900 dark:text-gray-100 focus:outline-none focus:ring-2 focus:ring-blue-500"
            />
          </div>

          {/* Date to */}
          <div>
            <label className="block text-xs font-medium text-gray-600 dark:text-gray-400 mb-1">Date To</label>
            <input
              type="date"
              value={pendingFilters.date_to}
              onChange={(e) =>
                setPendingFilters((f) => ({ ...f, date_to: e.target.value }))
              }
              className="border border-gray-300 dark:border-gray-600 rounded-md px-3 py-1.5 text-sm bg-white dark:bg-gray-800 text-gray-900 dark:text-gray-100 focus:outline-none focus:ring-2 focus:ring-blue-500"
            />
          </div>

          {/* Apply button */}
          <button
            onClick={handleApplyFilters}
            className="px-4 py-1.5 bg-blue-600 text-white rounded-md text-sm font-medium hover:bg-blue-700 transition-colors"
          >
            Apply Filters
          </button>
        </div>
      </div>

      {/* Results summary */}
      {data && !loading && (
        <p className="text-sm text-gray-500 dark:text-gray-400">
          {data.total} {data.total === 1 ? 'entry' : 'entries'} found
          {totalPages > 1 && ` — page ${page} of ${totalPages}`}
        </p>
      )}

      {/* Table */}
      <div className="bg-white dark:bg-gray-800 rounded-lg shadow dark:shadow-gray-900 overflow-hidden">
        {loading ? (
          <div className="flex items-center justify-center py-20">
            <div className="flex flex-col items-center gap-3">
              <div className="h-8 w-8 animate-spin rounded-full border-4 border-blue-600 border-t-transparent" />
              <span className="text-sm text-gray-500 dark:text-gray-400">Loading audit log...</span>
            </div>
          </div>
        ) : error ? (
          <div className="p-6 text-center">
            <p className="text-sm text-red-600">{error}</p>
            <button
              onClick={() => fetchData(page, appliedFilters)}
              className="mt-3 px-4 py-1.5 bg-red-600 text-white rounded-md text-sm font-medium hover:bg-red-700 transition-colors"
            >
              Retry
            </button>
          </div>
        ) : data?.items.length === 0 ? (
          <div className="p-12 text-center">
            <p className="text-sm font-medium text-gray-500 dark:text-gray-400">No audit log entries found</p>
            <p className="text-xs text-gray-400 dark:text-gray-500 mt-1">
              Try adjusting your filters or check back once agents have taken actions.
            </p>
          </div>
        ) : (
          <div className="overflow-x-auto">
            <table className="w-full">
              <thead className="bg-gray-50 dark:bg-gray-900">
                <tr>
                  {['Timestamp', 'Agent', 'Client ID', 'Action', 'Value'].map((h) => (
                    <th
                      key={h}
                      className="px-4 py-3 text-left text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider whitespace-nowrap"
                    >
                      {h}
                    </th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {data?.items.map((entry) => (
                  <tr key={entry.id} className="border-b border-gray-100 dark:border-gray-700 hover:bg-gray-50 dark:hover:bg-gray-700">
                    <td className="px-4 py-3 text-sm text-gray-500 dark:text-gray-400 whitespace-nowrap">
                      {new Date(entry.timestamp).toLocaleString()}
                    </td>
                    <td className="px-4 py-3 text-sm font-medium text-gray-900 dark:text-gray-100">
                      {entry.agent_username}
                    </td>
                    <td className="px-4 py-3 text-sm text-gray-600 dark:text-gray-400 font-mono text-xs">
                      {entry.client_account_id}
                    </td>
                    <td className="px-4 py-3 text-sm">
                      <ActionBadge actionType={entry.action_type} />
                    </td>
                    <td className="px-4 py-3 text-sm text-gray-600 dark:text-gray-400 max-w-xs truncate">
                      {entry.action_value ?? <span className="text-gray-300 dark:text-gray-600">—</span>}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </div>

      {/* Pagination */}
      {totalPages > 1 && (
        <div className="flex items-center justify-between">
          <button
            onClick={() => handlePageChange(page - 1)}
            disabled={page <= 1}
            className="px-3 py-1.5 text-sm font-medium rounded-md border border-gray-300 dark:border-gray-600 text-gray-700 dark:text-gray-300 hover:bg-gray-50 dark:hover:bg-gray-700 disabled:opacity-40 disabled:cursor-not-allowed transition-colors"
          >
            Previous
          </button>

          <div className="flex items-center gap-1">
            {Array.from({ length: Math.min(totalPages, 7) }, (_, i) => {
              // Show pages around current page
              let pageNum: number;
              if (totalPages <= 7) {
                pageNum = i + 1;
              } else if (page <= 4) {
                pageNum = i + 1;
              } else if (page >= totalPages - 3) {
                pageNum = totalPages - 6 + i;
              } else {
                pageNum = page - 3 + i;
              }
              return (
                <button
                  key={pageNum}
                  onClick={() => handlePageChange(pageNum)}
                  className={`w-8 h-8 text-sm font-medium rounded-md transition-colors ${
                    pageNum === page
                      ? 'bg-blue-600 text-white'
                      : 'text-gray-700 dark:text-gray-300 hover:bg-gray-100 dark:hover:bg-gray-700'
                  }`}
                >
                  {pageNum}
                </button>
              );
            })}
          </div>

          <button
            onClick={() => handlePageChange(page + 1)}
            disabled={page >= totalPages}
            className="px-3 py-1.5 text-sm font-medium rounded-md border border-gray-300 dark:border-gray-600 text-gray-700 dark:text-gray-300 hover:bg-gray-50 dark:hover:bg-gray-700 disabled:opacity-40 disabled:cursor-not-allowed transition-colors"
          >
            Next
          </button>
        </div>
      )}
    </div>
  );
}
