import { useCallback, useEffect, useRef, useState } from 'react';
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

interface ScoringRule {
  id: number;
  field: string;
  operator: string;
  value: string;
  score: number;
  created_at: string;
  value_min: number | null;
  value_max: number | null;
}

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const FIELD_OPTIONS = [
  // --- Financial ---
  { key: 'balance',              label: 'Balance' },
  { key: 'credit',               label: 'Credit' },
  { key: 'equity',               label: 'Equity' },
  { key: 'live_equity',          label: 'Live Equity' },
  { key: 'margin',               label: 'Margin' },
  { key: 'total_profit',         label: 'Total Profit' },
  { key: 'total_deposit',        label: 'Total Deposits' },
  { key: 'net_deposit',          label: 'Net Deposit' },
  { key: 'total_withdrawal',     label: 'Total Withdrawals' },
  { key: 'turnover',             label: 'Turnover' },
  { key: 'exposure_usd',         label: 'Exposure (USD)' },
  { key: 'exposure_pct',         label: 'Exposure %' },
  { key: 'open_pnl',             label: 'Open PnL' },
  // --- Trading Activity ---
  { key: 'trade_count',          label: 'Trade Count' },
  { key: 'max_open_trade',       label: 'Max Open Trade' },
  { key: 'max_volume',           label: 'Max Volume' },
  { key: 'max_volume_usd',       label: 'Max Volume (USD)' },
  { key: 'open_volume',          label: 'Open Volume' },
  { key: 'avg_trade_size',       label: 'Avg Trade Size' },
  { key: 'win_rate',             label: 'Win Rate' },
  { key: 'days_from_last_trade', label: 'Days from Last Trade' },
  { key: 'open_positions',       label: 'Number of Open Positions' },
  { key: 'unique_symbols',       label: 'Unique Symbols Traded' },
  // --- Engagement ---
  { key: 'days_in_retention',    label: 'Days in Retention' },
  { key: 'deposit_count',        label: 'Deposit Count' },
  { key: 'withdrawal_count',     label: 'Withdrawal Count' },
  { key: 'sales_potential',      label: 'Retention Status (numeric)' },
  { key: 'days_since_last_communication', label: 'Days Since Last Communication' },
  // --- Profile ---
  { key: 'age',                  label: 'Age' },
  { key: 'score',                label: 'Score' },
  { key: 'card_type',            label: 'Card Type' },
  { key: 'accountid',            label: 'Account ID' },
  { key: 'full_name',            label: 'Client Full Name' },
  { key: 'sales_client_potential', label: 'Retention Status' },
  { key: 'agent_name',           label: 'Assigned Agent' },
  { key: 'country',              label: 'Country' },
  { key: 'desk',                 label: 'Desk' },
  { key: 'is_favorite',          label: 'Favorite' },
  { key: 'task_type',            label: 'Task Type' },
  // --- Date-based (days since) ---
  { key: 'ftd_date',             label: 'Days Since FTD' },
  { key: 'reg_date',             label: 'Days Since Registration' },
];

const TEXT_FIELDS = new Set([
  'card_type', 'accountid', 'full_name', 'sales_client_potential',
  'agent_name', 'country', 'desk', 'task_type',
]);

const OP_OPTIONS = [
  { key: 'eq',       label: '= Equal' },
  { key: 'gt',       label: '> Greater than' },
  { key: 'lt',       label: '< Less than' },
  { key: 'gte',      label: '>= At least' },
  { key: 'lte',      label: '<= At most' },
  { key: 'contains', label: '~ Contains' },
  { key: 'between',  label: '<> Between' },
];

const OP_LABELS: Record<string, string> = { eq: '=', gt: '>', lt: '<', gte: '>=', lte: '<=', contains: '~', between: '<>' };

const FIELD_LABELS: Record<string, string> = Object.fromEntries(
  FIELD_OPTIONS.map((f) => [f.key, f.label])
);

// ---------------------------------------------------------------------------
// Sort types
// ---------------------------------------------------------------------------

type SortBy = 'count' | 'alpha' | 'pts';

// ---------------------------------------------------------------------------
// Session storage helpers
// ---------------------------------------------------------------------------

const SESSION_KEY = 'scoring_sections';

function loadSectionState(): Record<string, boolean> {
  try {
    return JSON.parse(sessionStorage.getItem(SESSION_KEY) || '{}');
  } catch {
    return {};
  }
}

function saveSectionState(state: Record<string, boolean>) {
  sessionStorage.setItem(SESSION_KEY, JSON.stringify(state));
}

// ---------------------------------------------------------------------------
// Chevron icon
// ---------------------------------------------------------------------------

function Chevron({ open }: { open: boolean }) {
  return (
    <svg
      className={`w-4 h-4 text-gray-400 dark:text-gray-500 transition-transform duration-200 flex-shrink-0 ${open ? 'rotate-90' : ''}`}
      fill="none"
      viewBox="0 0 24 24"
      stroke="currentColor"
      strokeWidth={2.5}
    >
      <path strokeLinecap="round" strokeLinejoin="round" d="M9 5l7 7-7 7" />
    </svg>
  );
}

// ---------------------------------------------------------------------------
// Accordion section (field-based)
// ---------------------------------------------------------------------------

interface AccordionSectionProps {
  fieldKey: string;
  fieldLabel: string;
  rules: ScoringRule[];
  open: boolean;
  onToggle: () => void;
  onEdit: (rule: ScoringRule) => void;
  onDelete: (rule: ScoringRule) => void;
  onAddRule: (fieldKey: string) => void;
}

function AccordionSection({ fieldKey, fieldLabel, rules, open, onToggle, onEdit, onDelete, onAddRule }: AccordionSectionProps) {
  const totalPts = rules.reduce((sum, r) => sum + r.score, 0);
  const ruleCount = rules.length;

  const bodyRef = useRef<HTMLDivElement>(null);

  return (
    <div className="bg-white dark:bg-gray-800 rounded-lg shadow dark:shadow-gray-900 overflow-hidden">
      <div className="flex items-center px-5 py-4 hover:bg-gray-50 dark:hover:bg-gray-700 transition-colors">
        <button
          type="button"
          onClick={onToggle}
          className="flex items-center gap-3 min-w-0 flex-1 text-left focus:outline-none focus:ring-2 focus:ring-inset focus:ring-blue-500"
          aria-expanded={open}
        >
          <Chevron open={open} />
          <span className="text-sm font-semibold text-gray-800 dark:text-gray-100">{fieldLabel}</span>
          <span className="text-xs text-gray-400 dark:text-gray-500 ml-1">
            {ruleCount === 0
              ? 'No rules'
              : `${ruleCount} ${ruleCount === 1 ? 'rule' : 'rules'} \u00b7 ${totalPts} pts`}
          </span>
        </button>
        <button
          type="button"
          onClick={(e) => { e.stopPropagation(); onAddRule(fieldKey); }}
          className="flex-shrink-0 ml-4 px-2.5 py-1 text-xs bg-blue-50 dark:bg-blue-900/30 text-blue-600 dark:text-blue-400 rounded-md hover:bg-blue-100 dark:hover:bg-blue-900/50 transition-colors whitespace-nowrap"
        >
          + Add Rule
        </button>
      </div>

      <div
        ref={bodyRef}
        className="overflow-hidden transition-all duration-200"
        style={{ maxHeight: open ? (bodyRef.current?.scrollHeight ?? 2000) + 'px' : '0px' }}
      >
        {rules.length === 0 ? (
          <div className="px-5 pb-5 pt-1 text-sm text-gray-400 dark:text-gray-500">
            No rules defined for this field yet.
          </div>
        ) : (
          <div className="overflow-x-auto border-t border-gray-100 dark:border-gray-700">
            <table className="w-full">
              <thead className="bg-gray-50 dark:bg-gray-900">
                <tr>
                  {['Operator', 'Value', 'Score', 'Created', 'Actions'].map((h) => (
                    <th
                      key={h}
                      className="px-2 md:px-4 py-2 md:py-3 text-left text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider whitespace-nowrap"
                    >
                      {h}
                    </th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {rules.map((rule) => (
                  <tr key={rule.id} className="border-t border-gray-100 dark:border-gray-700 hover:bg-gray-50 dark:hover:bg-gray-700">
                    <td className="px-2 md:px-4 py-2 md:py-3 text-sm text-gray-700 dark:text-gray-300">
                      {OP_LABELS[rule.operator] || rule.operator}
                    </td>
                    <td className="px-2 md:px-4 py-2 md:py-3 text-sm text-gray-700 dark:text-gray-300">
                      {rule.operator === 'between' && rule.value_min != null && rule.value_max != null
                        ? `${rule.value_min} \u2013 ${rule.value_max}`
                        : rule.value}
                    </td>
                    <td className="px-2 md:px-4 py-2 md:py-3 text-sm font-semibold text-blue-700">{rule.score}</td>
                    <td className="px-2 md:px-4 py-2 md:py-3 text-sm text-gray-500 dark:text-gray-400 whitespace-nowrap">
                      {rule.created_at ? new Date(rule.created_at).toLocaleDateString() : '--'}
                    </td>
                    <td className="px-2 md:px-4 py-2 md:py-3 text-sm flex gap-2">
                      <button
                        onClick={() => onEdit(rule)}
                        className="min-h-[44px] min-w-[44px] px-3 py-1 text-xs bg-gray-100 dark:bg-gray-700 text-gray-700 dark:text-gray-300 rounded-md hover:bg-gray-200 dark:hover:bg-gray-600 transition-colors"
                      >
                        Edit
                      </button>
                      <button
                        onClick={() => onDelete(rule)}
                        className="min-h-[44px] min-w-[44px] px-3 py-1 text-xs bg-red-100 text-red-600 rounded-md hover:bg-red-200 transition-colors"
                      >
                        Delete
                      </button>
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

// ---------------------------------------------------------------------------
// Main component
// ---------------------------------------------------------------------------

export function ClientScoringPage() {
  const [rules, setRules] = useState<ScoringRule[]>([]);
  const [loading, setLoading] = useState(true);
  const [sortBy, setSortBy] = useState<SortBy>('count');

  const [openSections, setOpenSections] = useState<Record<string, boolean>>(() => {
    const saved = loadSectionState();
    return saved;
  });

  // Form state -- shared between create (inline) and edit (modal) modes
  const [formOpen, setFormOpen] = useState(false);
  const [editingRule, setEditingRule] = useState<ScoringRule | null>(null);
  const [formField, setFormField] = useState('balance');
  const [formOp, setFormOp] = useState('gt');
  const [formValue, setFormValue] = useState('');
  const [formValueMin, setFormValueMin] = useState('');
  const [formValueMax, setFormValueMax] = useState('');
  const [formScore, setFormScore] = useState('');
  const [formError, setFormError] = useState('');
  const [formSaving, setFormSaving] = useState(false);

  // ---------------------------------------------------------------------------
  // Data loading
  // ---------------------------------------------------------------------------

  async function loadRules() {
    try {
      const res = await api.get<ScoringRule[]>('/retention/scoring-rules');
      setRules(res.data);
    } catch {
      // silently ignore on first load
    } finally {
      setLoading(false);
    }
  }

  useEffect(() => {
    loadRules();
  }, []);

  // ---------------------------------------------------------------------------
  // Field grouping
  // ---------------------------------------------------------------------------

  const fieldGroups: { fieldKey: string; fieldLabel: string; rules: ScoringRule[] }[] = (() => {
    const grouped: Record<string, ScoringRule[]> = {};
    for (const rule of rules) {
      if (!grouped[rule.field]) grouped[rule.field] = [];
      grouped[rule.field].push(rule);
    }
    const entries = Object.entries(grouped);
    entries.sort(([aKey, aRules], [bKey, bRules]) => {
      if (sortBy === 'count') {
        return bRules.length - aRules.length || aKey.localeCompare(bKey);
      } else if (sortBy === 'pts') {
        const aTotal = aRules.reduce((s, r) => s + r.score, 0);
        const bTotal = bRules.reduce((s, r) => s + r.score, 0);
        return bTotal - aTotal || aKey.localeCompare(bKey);
      } else {
        const aLabel = FIELD_LABELS[aKey] || aKey;
        const bLabel = FIELD_LABELS[bKey] || bKey;
        return aLabel.localeCompare(bLabel);
      }
    });
    return entries.map(([fieldKey, fieldRules]) => ({
      fieldKey,
      fieldLabel: FIELD_LABELS[fieldKey] || fieldKey,
      rules: fieldRules,
    }));
  })();

  const allFieldKeys = fieldGroups.map((g) => g.fieldKey);

  // ---------------------------------------------------------------------------
  // Accordion helpers
  // ---------------------------------------------------------------------------

  function toggleSection(id: string) {
    setOpenSections((prev) => {
      const next = { ...prev, [id]: !prev[id] };
      saveSectionState(next);
      return next;
    });
  }

  function expandAll() {
    const next: Record<string, boolean> = {};
    allFieldKeys.forEach((id) => { next[id] = true; });
    setOpenSections(next);
    saveSectionState(next);
  }

  function collapseAll() {
    const next: Record<string, boolean> = {};
    allFieldKeys.forEach((id) => { next[id] = false; });
    setOpenSections(next);
    saveSectionState(next);
  }

  // ---------------------------------------------------------------------------
  // Form handlers
  // ---------------------------------------------------------------------------

  function openCreate(field?: string) {
    setEditingRule(null);
    setFormField(field || 'balance');
    setFormOp(field && TEXT_FIELDS.has(field) ? 'eq' : 'gt');
    setFormValue('');
    setFormValueMin('');
    setFormValueMax('');
    setFormScore('');
    setFormError('');
    setFormOpen(true);
  }

  function openEdit(rule: ScoringRule) {
    setEditingRule(rule);
    setFormField(rule.field);
    setFormOp(rule.operator);
    setFormValue(rule.value);
    setFormValueMin(rule.value_min != null ? String(rule.value_min) : '');
    setFormValueMax(rule.value_max != null ? String(rule.value_max) : '');
    setFormScore(String(rule.score));
    setFormError('');
    setFormOpen(true);
  }

  function closeForm() {
    setFormOpen(false);
    setEditingRule(null);
    setFormError('');
  }

  async function handleSave(e: React.FormEvent) {
    e.preventDefault();
    if (formOp === 'between') {
      if (!formValueMin.trim() || !formValueMax.trim()) {
        setFormError('Both Min Value and Max Value are required for Between.');
        return;
      }
      const min = Number(formValueMin);
      const max = Number(formValueMax);
      if (isNaN(min) || isNaN(max)) {
        setFormError('Min and Max values must be valid numbers.');
        return;
      }
      if (min >= max) {
        setFormError('Min Value must be less than Max Value.');
        return;
      }
    } else {
      if (!formValue.trim()) {
        setFormError('Value is required.');
        return;
      }
    }
    if (!formScore.trim() || isNaN(Number(formScore))) {
      setFormError('Score must be a valid number.');
      return;
    }
    setFormSaving(true);
    setFormError('');
    try {
      const payload: Record<string, unknown> = {
        field: formField,
        operator: formOp,
        value: formOp === 'between' ? `${formValueMin.trim()}-${formValueMax.trim()}` : formValue.trim(),
        score: parseInt(formScore, 10),
      };
      if (formOp === 'between') {
        payload.value_min = parseFloat(formValueMin.trim());
        payload.value_max = parseFloat(formValueMax.trim());
      } else {
        payload.value_min = null;
        payload.value_max = null;
      }
      if (editingRule) {
        await api.put(`/retention/scoring-rules/${editingRule.id}`, payload);
      } else {
        await api.post('/retention/scoring-rules', payload);
      }
      closeForm();
      await loadRules();
    } catch (err: unknown) {
      const axiosErr = err as { response?: { data?: { detail?: string } } };
      setFormError(axiosErr.response?.data?.detail || 'Failed to save scoring rule.');
    } finally {
      setFormSaving(false);
    }
  }

  async function handleDelete(rule: ScoringRule) {
    if (!window.confirm(`Delete scoring rule "${FIELD_LABELS[rule.field] || rule.field} ${OP_LABELS[rule.operator] || rule.operator} ${rule.value}"?`)) return;
    try {
      await api.delete(`/retention/scoring-rules/${rule.id}`);
      await loadRules();
    } catch (err: unknown) {
      const axiosErr = err as { response?: { data?: { detail?: string } } };
      alert(axiosErr.response?.data?.detail || 'Failed to delete scoring rule.');
    }
  }

  // ---------------------------------------------------------------------------
  // Edit modal effects -- ESC to close, body scroll lock
  // ---------------------------------------------------------------------------

  const closeEditModal = useCallback(() => {
    setFormOpen(false);
    setEditingRule(null);
    setFormError('');
  }, []);

  useEffect(() => {
    const handler = (e: KeyboardEvent) => {
      if (e.key === 'Escape' && editingRule) closeEditModal();
    };
    if (editingRule) document.addEventListener('keydown', handler);
    return () => document.removeEventListener('keydown', handler);
  }, [editingRule, closeEditModal]);

  useEffect(() => {
    document.body.style.overflow = editingRule ? 'hidden' : '';
    return () => {
      document.body.style.overflow = '';
    };
  }, [editingRule]);

  // ---------------------------------------------------------------------------
  // Shared form fields JSX
  // ---------------------------------------------------------------------------

  const formFields = (
    <>
      <div className="flex items-end gap-3 flex-wrap">
        <div>
          <label className="block text-xs font-medium text-gray-600 dark:text-gray-400 mb-1">Field</label>
          <select
            value={formField}
            onChange={(e) => {
              const f = e.target.value;
              setFormField(f);
              if (TEXT_FIELDS.has(f) && !['eq', 'contains'].includes(formOp)) setFormOp('contains');
              if (!TEXT_FIELDS.has(f) && formOp === 'contains') setFormOp('gt');
              if (TEXT_FIELDS.has(f) && formOp === 'between') setFormOp('eq');
            }}
            className="border border-gray-300 dark:border-gray-600 rounded-md px-2 py-1.5 text-sm focus:outline-none focus:ring-2 focus:ring-blue-500 dark:bg-gray-800 dark:text-gray-100"
          >
            {FIELD_OPTIONS.map((f) => (
              <option key={f.key} value={f.key}>{f.label}</option>
            ))}
          </select>
        </div>

        <div>
          <label className="block text-xs font-medium text-gray-600 dark:text-gray-400 mb-1">Operator</label>
          <select
            value={formOp}
            onChange={(e) => {
              const newOp = e.target.value;
              setFormOp(newOp);
              if (newOp !== 'between') {
                setFormValueMin('');
                setFormValueMax('');
              }
            }}
            className="border border-gray-300 dark:border-gray-600 rounded-md px-2 py-1.5 text-sm focus:outline-none focus:ring-2 focus:ring-blue-500 dark:bg-gray-800 dark:text-gray-100"
          >
            {OP_OPTIONS.filter((o) =>
              TEXT_FIELDS.has(formField)
                ? ['eq', 'contains'].includes(o.key)
                : o.key !== 'contains'
            ).map((o) => (
              <option key={o.key} value={o.key}>{o.label}</option>
            ))}
          </select>
        </div>

        {formOp === 'between' ? (
          <div className="flex items-end gap-1">
            <div>
              <label className="block text-xs font-medium text-gray-600 dark:text-gray-400 mb-1">Min Value</label>
              <input
                type="number"
                step="any"
                value={formValueMin}
                onChange={(e) => setFormValueMin(e.target.value)}
                placeholder="e.g. 100"
                className="border border-gray-300 dark:border-gray-600 rounded-md px-2 py-1.5 text-sm focus:outline-none focus:ring-2 focus:ring-blue-500 w-28 dark:bg-gray-800 dark:text-gray-100 dark:placeholder-gray-500"
              />
            </div>
            <span className="text-gray-400 dark:text-gray-500 text-sm pb-1.5">&ndash;</span>
            <div>
              <label className="block text-xs font-medium text-gray-600 dark:text-gray-400 mb-1">Max Value</label>
              <input
                type="number"
                step="any"
                value={formValueMax}
                onChange={(e) => setFormValueMax(e.target.value)}
                placeholder="e.g. 500"
                className="border border-gray-300 dark:border-gray-600 rounded-md px-2 py-1.5 text-sm focus:outline-none focus:ring-2 focus:ring-blue-500 w-28 dark:bg-gray-800 dark:text-gray-100 dark:placeholder-gray-500"
              />
            </div>
          </div>
        ) : (
          <div>
            <label className="block text-xs font-medium text-gray-600 dark:text-gray-400 mb-1">Value</label>
            <input
              type={TEXT_FIELDS.has(formField) ? 'text' : 'number'}
              step="any"
              value={formValue}
              onChange={(e) => setFormValue(e.target.value)}
              placeholder={TEXT_FIELDS.has(formField) ? 'e.g. Visa' : 'e.g. 300'}
              className="border border-gray-300 dark:border-gray-600 rounded-md px-2 py-1.5 text-sm focus:outline-none focus:ring-2 focus:ring-blue-500 w-28 dark:bg-gray-800 dark:text-gray-100 dark:placeholder-gray-500"
            />
          </div>
        )}

        <div>
          <label className="block text-xs font-medium text-gray-600 dark:text-gray-400 mb-1">Score</label>
          <input
            type="number"
            value={formScore}
            onChange={(e) => setFormScore(e.target.value)}
            placeholder="e.g. 5"
            className="border border-gray-300 dark:border-gray-600 rounded-md px-2 py-1.5 text-sm focus:outline-none focus:ring-2 focus:ring-blue-500 w-20 dark:bg-gray-800 dark:text-gray-100 dark:placeholder-gray-500"
          />
        </div>
      </div>

      {formError && <p className="text-sm text-red-600">{formError}</p>}
    </>
  );

  // ---------------------------------------------------------------------------
  // Render
  // ---------------------------------------------------------------------------

  const anyOpen = allFieldKeys.some((id) => openSections[id]);
  const totalRules = rules.length;
  const totalPts = rules.reduce((sum, r) => sum + r.score, 0);

  const sortBtnClass = (s: SortBy) =>
    `px-3 py-1 text-xs rounded-md transition-colors ${
      sortBy === s
        ? 'bg-blue-600 text-white'
        : 'bg-gray-100 dark:bg-gray-700 text-gray-700 dark:text-gray-300 hover:bg-gray-200 dark:hover:bg-gray-600'
    }`;

  return (
    <div className="space-y-4">
      <div className="flex flex-col md:flex-row md:justify-between md:items-center gap-2">
        <div>
          <h2 className="text-base md:text-lg font-semibold text-gray-800 dark:text-gray-100">Scoring Rules</h2>
          {!loading && (
            <p className="text-xs text-gray-400 dark:text-gray-500 mt-0.5">
              {fieldGroups.length} {fieldGroups.length === 1 ? 'field' : 'fields'} &middot; {totalRules} {totalRules === 1 ? 'rule' : 'rules'} &middot; {totalPts} total pts
            </p>
          )}
        </div>
        <div className="flex items-center gap-2 flex-wrap">
          {/* Sort toggle */}
          <div className="flex items-center gap-1 bg-gray-50 dark:bg-gray-800 border border-gray-200 dark:border-gray-700 rounded-md p-1">
            <button onClick={() => setSortBy('count')} className={sortBtnClass('count')}>Most Rules</button>
            <button onClick={() => setSortBy('alpha')} className={sortBtnClass('alpha')}>A–Z</button>
            <button onClick={() => setSortBy('pts')} className={sortBtnClass('pts')}>Highest Pts</button>
          </div>
          <button
            onClick={anyOpen ? collapseAll : expandAll}
            className="min-h-[44px] px-3 py-1.5 bg-gray-100 dark:bg-gray-700 text-gray-700 dark:text-gray-300 rounded-md text-sm font-medium hover:bg-gray-200 dark:hover:bg-gray-600 transition-colors"
          >
            {anyOpen ? 'Collapse All' : 'Expand All'}
          </button>
          <button
            onClick={() => openCreate()}
            className="min-h-[44px] px-4 py-2 bg-blue-600 text-white rounded-md text-sm font-medium hover:bg-blue-700 transition-colors"
          >
            New Scoring Rule
          </button>
        </div>
      </div>

      <p className="text-sm text-gray-500 dark:text-gray-400">
        Define rules to score clients in the Retention Manager table. Each rule assigns a score when a condition is met. The total score for each client is the sum of all matching rules.
      </p>

      {/* Create form -- inline at top of page, shown only when creating a new rule */}
      {formOpen && !editingRule && (
        <form
          onSubmit={handleSave}
          className="bg-white dark:bg-gray-800 rounded-lg shadow dark:shadow-gray-900 p-5 space-y-4"
        >
          <h3 className="text-sm font-semibold text-gray-700 dark:text-gray-300">Create Scoring Rule</h3>

          {formFields}

          <div className="flex gap-2">
            <button
              type="submit"
              disabled={formSaving}
              className="px-4 py-1.5 bg-blue-600 text-white rounded-md text-sm font-medium hover:bg-blue-700 disabled:opacity-50 transition-colors"
            >
              {formSaving ? 'Saving...' : 'Save'}
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

      {/* Edit modal -- centered overlay, shown only when editing an existing rule */}
      {formOpen && editingRule && (
        <div
          className="fixed inset-0 z-50 flex items-center justify-center"
          onClick={(e) => { if (e.target === e.currentTarget) closeEditModal(); }}
        >
          <div className="absolute inset-0 bg-black/50" />

          <div className="relative bg-white dark:bg-gray-800 rounded-xl shadow-2xl dark:shadow-gray-900 w-full max-w-lg mx-4 max-h-[90vh] overflow-y-auto p-6 space-y-4">
            <div className="flex items-center justify-between mb-2">
              <h2 className="text-base font-semibold text-gray-800 dark:text-gray-100">Edit Criterion</h2>
              <button
                type="button"
                onClick={closeEditModal}
                className="text-gray-400 hover:text-gray-600 dark:hover:text-gray-300 text-xl leading-none"
              >
                &times;
              </button>
            </div>

            <form onSubmit={handleSave} className="space-y-4">
              {formFields}

              <div className="flex gap-2 pt-2 border-t border-gray-100 dark:border-gray-700">
                <button
                  type="submit"
                  disabled={formSaving}
                  className="px-4 py-2 bg-blue-600 text-white rounded-md text-sm font-medium hover:bg-blue-700 disabled:opacity-50 transition-colors"
                >
                  {formSaving ? 'Saving...' : 'Save'}
                </button>
                <button
                  type="button"
                  onClick={closeEditModal}
                  className="px-4 py-2 bg-gray-100 dark:bg-gray-700 text-gray-700 dark:text-gray-300 rounded-md text-sm font-medium hover:bg-gray-200 dark:hover:bg-gray-600 transition-colors"
                >
                  Cancel
                </button>
              </div>
            </form>
          </div>
        </div>
      )}

      {/* Accordion sections -- one per field */}
      {loading ? (
        <div className="bg-white dark:bg-gray-800 rounded-lg shadow dark:shadow-gray-900 p-12 text-center text-gray-400 dark:text-gray-500 text-sm">Loading...</div>
      ) : fieldGroups.length === 0 ? (
        <div className="bg-white dark:bg-gray-800 rounded-lg shadow dark:shadow-gray-900 p-12 text-center text-gray-400 dark:text-gray-500 text-sm">
          No scoring rules defined yet. Click "New Scoring Rule" to get started.
        </div>
      ) : (
        <div className="space-y-2">
          {fieldGroups.map(({ fieldKey, fieldLabel, rules: fieldRules }) => (
            <AccordionSection
              key={fieldKey}
              fieldKey={fieldKey}
              fieldLabel={fieldLabel}
              rules={fieldRules}
              open={!!openSections[fieldKey]}
              onToggle={() => toggleSection(fieldKey)}
              onEdit={openEdit}
              onDelete={handleDelete}
              onAddRule={(fk) => openCreate(fk)}
            />
          ))}
        </div>
      )}
    </div>
  );
}
