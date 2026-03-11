import { useEffect, useState } from 'react';
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

interface Condition {
  column: string;
  op: string;
  value: string;
}

type TaskColor = 'red' | 'orange' | 'yellow' | 'green' | 'blue' | 'purple' | 'pink' | 'grey';

interface RetentionTask {
  id: number;
  name: string;
  conditions: Condition[];
  color: TaskColor;
  created_at: string;
}

interface TaskClient {
  accountid: string;
  balance: number;
  credit: number;
  equity: number;
  trade_count: number;
  total_profit: number;
  last_trade_date: string | null;
  active: boolean;
  agent_name: string | null;
}

interface TaskClientsState {
  total: number;
  page: number;
  clients: TaskClient[];
  loading: boolean;
}

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const COND_COLS = [
  // --- Financial ---
  { key: 'balance',             label: 'Balance',              type: 'number' },
  { key: 'credit',              label: 'Credit',               type: 'number' },
  { key: 'equity',              label: 'Equity',               type: 'number' },
  { key: 'live_equity',         label: 'Live Equity',          type: 'number' },
  { key: 'margin',              label: 'Margin',               type: 'number' },
  { key: 'total_profit',        label: 'Total Profit',         type: 'number' },
  { key: 'total_deposit',       label: 'Total Deposits',       type: 'number' },
  { key: 'net_deposit',         label: 'Net Deposit',          type: 'number' },
  { key: 'total_withdrawal',    label: 'Total Withdrawals',    type: 'number' },
  { key: 'turnover',            label: 'Turnover',             type: 'number' },
  { key: 'exposure_usd',        label: 'Exposure (USD)',       type: 'number' },
  { key: 'exposure_pct',        label: 'Exposure %',           type: 'number' },
  { key: 'open_pnl',            label: 'Open PnL',             type: 'number' },
  // --- Trading Activity ---
  { key: 'trade_count',         label: 'Trade Count',          type: 'number' },
  { key: 'max_open_trade',      label: 'Max Open Trade',       type: 'number' },
  { key: 'max_volume',          label: 'Max Volume',           type: 'number' },
  { key: 'max_volume_usd',      label: 'Max Volume (USD)',     type: 'number' },
  { key: 'open_volume',         label: 'Open Volume',          type: 'number' },
  { key: 'avg_trade_size',      label: 'Avg Trade Size',       type: 'number' },
  { key: 'win_rate',            label: 'Win Rate',             type: 'number' },
  { key: 'days_from_last_trade',label: 'Days from Last Trade', type: 'number' },
  { key: 'open_positions',      label: 'Open Positions',       type: 'number' },
  { key: 'unique_symbols',      label: 'Unique Symbols',       type: 'number' },
  // --- Engagement ---
  { key: 'days_in_retention',   label: 'Days in Retention',    type: 'number' },
  { key: 'deposit_count',       label: 'Deposit Count',        type: 'number' },
  { key: 'withdrawal_count',    label: 'Withdrawal Count',     type: 'number' },
  { key: 'sales_potential',     label: 'Retention Status (numeric)', type: 'number' },
  { key: 'days_since_last_communication', label: 'Days Since Last Communication', type: 'number' },
  // --- Profile ---
  { key: 'age',                 label: 'Age',                  type: 'number' },
  { key: 'score',               label: 'Score',                type: 'number' },
  { key: 'card_type',           label: 'Card Type',            type: 'text' },
  { key: 'accountid',           label: 'Account ID',           type: 'text' },
  { key: 'full_name',           label: 'Client Full Name',     type: 'text' },
  { key: 'sales_client_potential', label: 'Retention Status',  type: 'text' },
  { key: 'assigned_to',         label: 'Assigned To',          type: 'text' },
  { key: 'agent_name',          label: 'Assigned Agent',       type: 'text' },
  { key: 'country',             label: 'Country',              type: 'text' },
  { key: 'desk',                label: 'Desk',                 type: 'text' },
  { key: 'is_favorite',         label: 'Favorite',             type: 'number' },
  { key: 'task_type',           label: 'Task Type',            type: 'text' },
  // --- Date-based (days since) ---
  { key: 'ftd_date',            label: 'Days Since FTD',       type: 'number' },
  { key: 'reg_date',            label: 'Days Since Registration', type: 'number' },
  // --- Special booleans ---
  { key: 'active',              label: 'Active',               type: 'boolean' },
  { key: 'active_ftd',          label: 'Active FTD',           type: 'boolean' },
];

const NUM_OPS = [
  { key: 'eq',  label: '= Equal' },
  { key: 'gt',  label: '> Greater than' },
  { key: 'lt',  label: '< Less than' },
  { key: 'gte', label: '≥ At least' },
  { key: 'lte', label: '≤ At most' },
];

const TEXT_OPS = [
  { key: 'eq',       label: '= Equal' },
  { key: 'contains', label: '~ Contains' },
];

const OP_LABELS: Record<string, string> = { eq: '=', gt: '>', lt: '<', gte: '≥', lte: '≤', contains: '~' };

const TASK_COLORS: { key: TaskColor; label: string; hex: string; bg: string; text: string; border: string }[] = [
  { key: 'red',    label: 'Red',    hex: '#EF4444', bg: 'bg-red-100',    text: 'text-red-700',    border: 'border-red-400' },
  { key: 'orange', label: 'Orange', hex: '#F97316', bg: 'bg-orange-100', text: 'text-orange-700', border: 'border-orange-400' },
  { key: 'yellow', label: 'Yellow', hex: '#EAB308', bg: 'bg-yellow-100', text: 'text-yellow-700', border: 'border-yellow-400' },
  { key: 'green',  label: 'Green',  hex: '#22C55E', bg: 'bg-green-100',  text: 'text-green-700',  border: 'border-green-400' },
  { key: 'blue',   label: 'Blue',   hex: '#3B82F6', bg: 'bg-blue-100',   text: 'text-blue-700',   border: 'border-blue-400' },
  { key: 'purple', label: 'Purple', hex: '#A855F7', bg: 'bg-purple-100', text: 'text-purple-700', border: 'border-purple-400' },
  { key: 'pink',   label: 'Pink',   hex: '#EC4899', bg: 'bg-pink-100',   text: 'text-pink-700',   border: 'border-pink-400' },
  { key: 'grey',   label: 'Grey',   hex: '#6B7280', bg: 'bg-gray-100',   text: 'text-gray-700',   border: 'border-gray-400' },
];

const COLOR_MAP = Object.fromEntries(TASK_COLORS.map((c) => [c.key, c])) as Record<TaskColor, typeof TASK_COLORS[number]>;

const PAGE_SIZE = 50;

const DEFAULT_CONDITION: Condition = { column: 'balance', op: 'lt', value: '' };

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function condLabel(c: Condition): string {
  const col = COND_COLS.find((x) => x.key === c.column);
  const colLabel = col?.label ?? c.column;
  if (col?.type === 'boolean') return `${colLabel} = ${c.value === 'true' ? 'Yes' : 'No'}`;
  return `${colLabel} ${OP_LABELS[c.op] ?? c.op} ${c.value}`;
}

function fmt(n: number | null | undefined): string {
  if (n == null) return '—';
  return n.toFixed(2);
}

function opsForCol(colKey: string): { key: string; label: string }[] {
  const col = COND_COLS.find((x) => x.key === colKey);
  if (!col) return NUM_OPS;
  if (col.type === 'boolean') return [{ key: 'eq', label: '= Equal' }];
  if (col.type === 'text') return TEXT_OPS;
  return NUM_OPS;
}

// ---------------------------------------------------------------------------
// Condition row
// ---------------------------------------------------------------------------

interface CondRowProps {
  cond: Condition;
  onChange: (c: Condition) => void;
  onRemove: () => void;
}

function ConditionRow({ cond, onChange, onRemove }: CondRowProps) {
  const col = COND_COLS.find((x) => x.key === cond.column);
  const ops = opsForCol(cond.column);

  function handleColChange(colKey: string) {
    const newCol = COND_COLS.find((x) => x.key === colKey);
    const newOp = newCol?.type === 'boolean' || newCol?.type === 'text' ? 'eq' : cond.op;
    const newValue = newCol?.type === 'boolean' ? 'true' : '';
    onChange({ column: colKey, op: newOp, value: newValue });
  }

  function handleOpChange(opKey: string) {
    onChange({ ...cond, op: opKey });
  }

  function handleValueChange(val: string) {
    onChange({ ...cond, value: val });
  }

  return (
    <div className="flex items-center gap-2 flex-wrap">
      {/* Column */}
      <select
        value={cond.column}
        onChange={(e) => handleColChange(e.target.value)}
        className="border border-gray-300 dark:border-gray-600 rounded-md px-2 py-1.5 text-sm focus:outline-none focus:ring-2 focus:ring-blue-500 dark:bg-gray-800 dark:text-gray-100"
      >
        {COND_COLS.map((c) => (
          <option key={c.key} value={c.key}>{c.label}</option>
        ))}
      </select>

      {/* Operator */}
      <select
        value={cond.op}
        onChange={(e) => handleOpChange(e.target.value)}
        className="border border-gray-300 dark:border-gray-600 rounded-md px-2 py-1.5 text-sm focus:outline-none focus:ring-2 focus:ring-blue-500 dark:bg-gray-800 dark:text-gray-100"
      >
        {ops.map((o) => (
          <option key={o.key} value={o.key}>{o.label}</option>
        ))}
      </select>

      {/* Value */}
      {col?.type === 'boolean' ? (
        <select
          value={cond.value}
          onChange={(e) => handleValueChange(e.target.value)}
          className="border border-gray-300 dark:border-gray-600 rounded-md px-2 py-1.5 text-sm focus:outline-none focus:ring-2 focus:ring-blue-500 dark:bg-gray-800 dark:text-gray-100"
        >
          <option value="true">Yes</option>
          <option value="false">No</option>
        </select>
      ) : col?.type === 'text' ? (
        <input
          type="text"
          value={cond.value}
          onChange={(e) => handleValueChange(e.target.value)}
          className="border border-gray-300 dark:border-gray-600 rounded-md px-2 py-1.5 text-sm focus:outline-none focus:ring-2 focus:ring-blue-500 w-36 dark:bg-gray-800 dark:text-gray-100 dark:placeholder-gray-500"
        />
      ) : (
        <input
          type="number"
          value={cond.value}
          onChange={(e) => handleValueChange(e.target.value)}
          className="border border-gray-300 dark:border-gray-600 rounded-md px-2 py-1.5 text-sm focus:outline-none focus:ring-2 focus:ring-blue-500 w-28 dark:bg-gray-800 dark:text-gray-100 dark:placeholder-gray-500"
        />
      )}

      {/* Remove */}
      <button
        type="button"
        onClick={onRemove}
        className="text-red-500 hover:text-red-700 font-bold text-base leading-none px-1"
        title="Remove condition"
      >
        &times;
      </button>
    </div>
  );
}

// ---------------------------------------------------------------------------
// Main page
// ---------------------------------------------------------------------------

export function RetentionTasksPage() {
  const [tasks, setTasks] = useState<RetentionTask[]>([]);
  const [loading, setLoading] = useState(true);

  // Form state
  const [formOpen, setFormOpen] = useState(false);
  const [editingTask, setEditingTask] = useState<RetentionTask | null>(null);
  const [formName, setFormName] = useState('');
  const [formColor, setFormColor] = useState<TaskColor>('grey');
  const [formConditions, setFormConditions] = useState<Condition[]>([{ ...DEFAULT_CONDITION }]);
  const [formError, setFormError] = useState('');
  const [formSaving, setFormSaving] = useState(false);

  // Expanded clients state
  const [expandedId, setExpandedId] = useState<number | null>(null);
  const [taskClients, setTaskClients] = useState<Record<number, TaskClientsState>>({});

  // ---------------------------------------------------------------------------
  // Data loading
  // ---------------------------------------------------------------------------

  async function loadTasks() {
    try {
      const res = await api.get<RetentionTask[]>('/retention/tasks');
      setTasks(res.data);
    } catch {
      // silently ignore; table may not exist yet on first render
    } finally {
      setLoading(false);
    }
  }

  useEffect(() => {
    loadTasks();
  }, []);

  // ---------------------------------------------------------------------------
  // Form handlers
  // ---------------------------------------------------------------------------

  function openCreate() {
    setEditingTask(null);
    setFormName('');
    setFormColor('grey');
    setFormConditions([{ ...DEFAULT_CONDITION }]);
    setFormError('');
    setFormOpen(true);
  }

  function openEdit(task: RetentionTask) {
    setEditingTask(task);
    setFormName(task.name);
    setFormColor(task.color || 'grey');
    setFormConditions(task.conditions.length > 0 ? task.conditions.map((c) => ({ ...c })) : [{ ...DEFAULT_CONDITION }]);
    setFormError('');
    setFormOpen(true);
  }

  function closeForm() {
    setFormOpen(false);
    setEditingTask(null);
    setFormError('');
  }

  function addCondition() {
    setFormConditions((prev) => [...prev, { ...DEFAULT_CONDITION }]);
  }

  function updateCondition(index: number, cond: Condition) {
    setFormConditions((prev) => prev.map((c, i) => (i === index ? cond : c)));
  }

  function removeCondition(index: number) {
    setFormConditions((prev) => prev.filter((_, i) => i !== index));
  }

  async function handleSave(e: React.FormEvent) {
    e.preventDefault();
    if (!formName.trim()) {
      setFormError('Task name is required.');
      return;
    }
    setFormSaving(true);
    setFormError('');
    try {
      if (editingTask) {
        await api.put(`/retention/tasks/${editingTask.id}`, {
          name: formName.trim(),
          conditions: formConditions,
          color: formColor,
        });
      } else {
        await api.post('/retention/tasks', {
          name: formName.trim(),
          conditions: formConditions,
          color: formColor,
        });
      }
      closeForm();
      await loadTasks();
    } catch (err: any) {
      setFormError(err.response?.data?.detail || 'Failed to save task.');
    } finally {
      setFormSaving(false);
    }
  }

  async function handleDelete(task: RetentionTask) {
    if (!window.confirm(`Delete task "${task.name}"?`)) return;
    try {
      await api.delete(`/retention/tasks/${task.id}`);
      if (expandedId === task.id) setExpandedId(null);
      setTaskClients((prev) => {
        const next = { ...prev };
        delete next[task.id];
        return next;
      });
      await loadTasks();
    } catch (err: any) {
      alert(err.response?.data?.detail || 'Failed to delete task.');
    }
  }

  // ---------------------------------------------------------------------------
  // Expand / clients
  // ---------------------------------------------------------------------------

  async function fetchClients(taskId: number, page: number) {
    setTaskClients((prev) => ({
      ...prev,
      [taskId]: { total: prev[taskId]?.total ?? 0, page, clients: prev[taskId]?.clients ?? [], loading: true },
    }));
    try {
      const res = await api.get(`/retention/tasks/${taskId}/clients`, {
        params: { page, page_size: PAGE_SIZE },
      });
      setTaskClients((prev) => ({
        ...prev,
        [taskId]: { total: res.data.total, page: res.data.page, clients: res.data.clients, loading: false },
      }));
    } catch (err: any) {
      setTaskClients((prev) => ({
        ...prev,
        [taskId]: { ...(prev[taskId] ?? { total: 0, page: 1, clients: [] }), loading: false },
      }));
      alert(err.response?.data?.detail || 'Failed to load clients.');
    }
  }

  function toggleExpand(taskId: number) {
    if (expandedId === taskId) {
      setExpandedId(null);
    } else {
      setExpandedId(taskId);
      if (!taskClients[taskId]) {
        fetchClients(taskId, 1);
      }
    }
  }

  function goToPage(taskId: number, page: number) {
    fetchClients(taskId, page);
  }

  // ---------------------------------------------------------------------------
  // Render
  // ---------------------------------------------------------------------------

  return (
    <div className="space-y-4">
      {/* Top bar */}
      <div className="flex justify-between items-center">
        <h2 className="text-lg font-semibold text-gray-800 dark:text-gray-100">Task Rules</h2>
        <button
          onClick={openCreate}
          className="px-4 py-2 bg-blue-600 text-white rounded-md text-sm font-medium hover:bg-blue-700 transition-colors"
        >
          New Task Rule
        </button>
      </div>

      {/* Create / Edit form */}
      {formOpen && (
        <form
          onSubmit={handleSave}
          className="bg-white dark:bg-gray-800 rounded-lg shadow dark:shadow-gray-900 p-5 space-y-4"
        >
          <h3 className="text-sm font-semibold text-gray-700 dark:text-gray-300">
            {editingTask ? 'Edit Task Rule' : 'Create Task Rule'}
          </h3>

          {/* Name */}
          <div>
            <label className="block text-xs font-medium text-gray-600 dark:text-gray-400 mb-1">Name *</label>
            <input
              type="text"
              required
              value={formName}
              onChange={(e) => setFormName(e.target.value)}
              placeholder="e.g. Inactive high-balance clients"
              className="w-full border border-gray-300 dark:border-gray-600 rounded-md px-3 py-1.5 text-sm focus:outline-none focus:ring-2 focus:ring-blue-500 dark:bg-gray-800 dark:text-gray-100 dark:placeholder-gray-500"
            />
          </div>

          {/* Color */}
          <div>
            <label className="block text-xs font-medium text-gray-600 dark:text-gray-400 mb-1">Color</label>
            <div className="flex items-center gap-2 flex-wrap">
              {TASK_COLORS.map((c) => (
                <button
                  key={c.key}
                  type="button"
                  onClick={() => setFormColor(c.key)}
                  className={`w-7 h-7 rounded-full border-2 transition-all ${
                    formColor === c.key ? 'ring-2 ring-offset-1 ring-blue-500 scale-110' : 'hover:scale-105'
                  }`}
                  style={{ backgroundColor: c.hex, borderColor: formColor === c.key ? '#3B82F6' : c.hex }}
                  title={c.label}
                  aria-label={`Select ${c.label} color`}
                />
              ))}
              <span className="text-xs text-gray-500 dark:text-gray-400 ml-2">
                {TASK_COLORS.find((c) => c.key === formColor)?.label ?? 'Grey'}
              </span>
            </div>
          </div>

          {/* Conditions */}
          <div className="space-y-2">
            <label className="block text-xs font-medium text-gray-600 dark:text-gray-400">
              Conditions (all must match)
            </label>
            {formConditions.map((cond, i) => (
              <ConditionRow
                key={i}
                cond={cond}
                onChange={(c) => updateCondition(i, c)}
                onRemove={() => removeCondition(i)}
              />
            ))}
            <button
              type="button"
              onClick={addCondition}
              className="px-3 py-1 text-xs bg-gray-100 dark:bg-gray-700 text-gray-700 dark:text-gray-300 rounded-md hover:bg-gray-200 dark:hover:bg-gray-600 transition-colors"
            >
              + Add Condition
            </button>
          </div>

          {formError && <p className="text-sm text-red-600">{formError}</p>}

          <div className="flex gap-2">
            <button
              type="submit"
              disabled={formSaving}
              className="px-4 py-1.5 bg-blue-600 text-white rounded-md text-sm font-medium hover:bg-blue-700 disabled:opacity-50 transition-colors"
            >
              {formSaving ? 'Saving…' : 'Save'}
            </button>
            <button
              type="button"
              onClick={closeForm}
              className="px-4 py-1.5 bg-gray-100 dark:bg-gray-700 text-gray-700 dark:text-gray-300 rounded-md text-sm font-medium hover:bg-gray-200 dark:hover:bg-gray-600 transition-colors"
            >
              Cancel
            </button>
          </div>
        </form>
      )}

      {/* Task list */}
      {loading ? (
        <div className="p-12 text-center text-gray-400 dark:text-gray-500 text-sm">Loading…</div>
      ) : tasks.length === 0 ? (
        <div className="p-12 text-center text-gray-400 dark:text-gray-500 text-sm">
          No task rules yet. Click &lsquo;New Task Rule&rsquo; to create one.
        </div>
      ) : (
        <div className="space-y-3">
          {tasks.map((task) => {
            const isExpanded = expandedId === task.id;
            const clientsState = taskClients[task.id];

            return (
              <div key={task.id} className="bg-white dark:bg-gray-800 rounded-lg shadow dark:shadow-gray-900 border-l-4" style={{ borderLeftColor: COLOR_MAP[task.color]?.hex || COLOR_MAP.grey.hex }}>
                {/* Card header */}
                <div className="px-4 py-3 flex flex-wrap items-center gap-2">
                  <span
                    className="inline-block w-3 h-3 rounded-full shrink-0"
                    style={{ backgroundColor: COLOR_MAP[task.color]?.hex || COLOR_MAP.grey.hex }}
                    title={COLOR_MAP[task.color]?.label || 'Grey'}
                  />
                  <span className="font-semibold text-gray-900 dark:text-gray-100 text-sm mr-2">{task.name}</span>
                  <div className="flex flex-wrap gap-1 flex-1">
                    {task.conditions.map((c, i) => (
                      <span
                        key={i}
                        className="inline-block bg-gray-100 dark:bg-gray-700 text-gray-600 dark:text-gray-300 text-xs px-2 py-0.5 rounded-full"
                      >
                        {condLabel(c)}
                      </span>
                    ))}
                  </div>
                  <div className="flex gap-2 ml-auto shrink-0">
                    <button
                      onClick={() => openEdit(task)}
                      className="px-3 py-1 text-xs bg-gray-100 dark:bg-gray-700 text-gray-700 dark:text-gray-300 rounded-md hover:bg-gray-200 dark:hover:bg-gray-600 transition-colors"
                    >
                      Edit
                    </button>
                    <button
                      onClick={() => handleDelete(task)}
                      className="px-3 py-1 text-xs bg-red-100 text-red-600 rounded-md hover:bg-red-200 transition-colors"
                    >
                      Delete
                    </button>
                  </div>
                </div>

                {/* Expand toggle */}
                <div className="px-4 pb-2">
                  <button
                    onClick={() => toggleExpand(task.id)}
                    className="text-xs text-blue-600 hover:underline"
                  >
                    {isExpanded ? '▲ Hide clients' : '▼ Show clients'}
                  </button>
                </div>

                {/* Expanded clients section */}
                {isExpanded && (
                  <div className="border-t border-gray-100 dark:border-gray-700 px-4 py-3">
                    {clientsState?.loading ? (
                      <div className="text-sm text-gray-400 dark:text-gray-500 py-4 text-center">Loading clients…</div>
                    ) : clientsState ? (
                      <>
                        <p className="text-sm text-gray-600 dark:text-gray-400 mb-2 font-medium">
                          {clientsState.total} client{clientsState.total !== 1 ? 's' : ''} match
                        </p>
                        <div className="overflow-x-auto">
                          <table className="w-full text-sm">
                            <thead className="bg-gray-50 dark:bg-gray-900">
                              <tr>
                                {[
                                  'Account ID',
                                  'Agent',
                                  'Balance',
                                  'Credit',
                                  'Equity',
                                  'Trades',
                                  'Profit',
                                  'Last Trade',
                                  'Active',
                                ].map((h) => (
                                  <th
                                    key={h}
                                    className="px-3 py-2 text-left text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider whitespace-nowrap"
                                  >
                                    {h}
                                  </th>
                                ))}
                              </tr>
                            </thead>
                            <tbody>
                              {clientsState.clients.length === 0 ? (
                                <tr>
                                  <td
                                    colSpan={9}
                                    className="px-3 py-6 text-center text-gray-400 dark:text-gray-500 text-xs"
                                  >
                                    No clients found.
                                  </td>
                                </tr>
                              ) : (
                                clientsState.clients.map((c) => (
                                  <tr
                                    key={c.accountid}
                                    className="border-t border-gray-100 dark:border-gray-700 hover:bg-gray-50 dark:hover:bg-gray-700"
                                  >
                                    <td className="px-3 py-2 whitespace-nowrap">
                                      <a
                                        href={`https://crm.cmtrading.com/#/users/user/${c.accountid}`}
                                        target="_blank"
                                        rel="noopener noreferrer"
                                        className="text-blue-600 hover:underline text-xs font-medium"
                                      >
                                        {c.accountid}
                                      </a>
                                    </td>
                                    <td className="px-3 py-2 text-xs text-gray-700 dark:text-gray-300 whitespace-nowrap">
                                      {c.agent_name ?? '—'}
                                    </td>
                                    <td className="px-3 py-2 text-xs text-gray-700 dark:text-gray-300 whitespace-nowrap">
                                      {fmt(c.balance)}
                                    </td>
                                    <td className="px-3 py-2 text-xs text-gray-700 dark:text-gray-300 whitespace-nowrap">
                                      {fmt(c.credit)}
                                    </td>
                                    <td className="px-3 py-2 text-xs text-gray-700 dark:text-gray-300 whitespace-nowrap">
                                      {fmt(c.equity)}
                                    </td>
                                    <td className="px-3 py-2 text-xs text-gray-700 dark:text-gray-300 whitespace-nowrap">
                                      {c.trade_count ?? '—'}
                                    </td>
                                    <td className="px-3 py-2 text-xs text-gray-700 dark:text-gray-300 whitespace-nowrap">
                                      {fmt(c.total_profit)}
                                    </td>
                                    <td className="px-3 py-2 text-xs text-gray-500 dark:text-gray-400 whitespace-nowrap">
                                      {c.last_trade_date
                                        ? new Date(c.last_trade_date).toLocaleDateString()
                                        : '—'}
                                    </td>
                                    <td className="px-3 py-2 whitespace-nowrap">
                                      {c.active ? (
                                        <span className="inline-block bg-green-100 text-green-700 text-xs px-2 py-0.5 rounded-full font-medium">
                                          Yes
                                        </span>
                                      ) : (
                                        <span className="inline-block bg-gray-100 dark:bg-gray-700 text-gray-500 dark:text-gray-400 text-xs px-2 py-0.5 rounded-full font-medium">
                                          No
                                        </span>
                                      )}
                                    </td>
                                  </tr>
                                ))
                              )}
                            </tbody>
                          </table>
                        </div>

                        {/* Pagination */}
                        {clientsState.total > PAGE_SIZE && (
                          <div className="flex items-center gap-3 mt-3">
                            <button
                              disabled={clientsState.page <= 1}
                              onClick={() => goToPage(task.id, clientsState.page - 1)}
                              className="px-3 py-1 text-xs bg-gray-100 dark:bg-gray-700 text-gray-700 dark:text-gray-300 rounded-md hover:bg-gray-200 dark:hover:bg-gray-600 disabled:opacity-40 transition-colors"
                            >
                              Prev
                            </button>
                            <span className="text-xs text-gray-500 dark:text-gray-400">
                              Page {clientsState.page} of{' '}
                              {Math.ceil(clientsState.total / PAGE_SIZE)}
                            </span>
                            <button
                              disabled={
                                clientsState.page >= Math.ceil(clientsState.total / PAGE_SIZE)
                              }
                              onClick={() => goToPage(task.id, clientsState.page + 1)}
                              className="px-3 py-1 text-xs bg-gray-100 dark:bg-gray-700 text-gray-700 dark:text-gray-300 rounded-md hover:bg-gray-200 dark:hover:bg-gray-600 disabled:opacity-40 transition-colors"
                            >
                              Next
                            </button>
                          </div>
                        )}
                      </>
                    ) : null}
                  </div>
                )}
              </div>
            );
          })}
        </div>
      )}
    </div>
  );
}
