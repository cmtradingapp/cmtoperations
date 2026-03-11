/**
 * CLAUD-96: Action Bonuses — Admin page for rule-based credit rewards.
 */
import { useEffect, useState, useCallback } from 'react';
import {
  DndContext,
  closestCenter,
  KeyboardSensor,
  PointerSensor,
  useSensor,
  useSensors,
  DragEndEvent,
} from '@dnd-kit/core';
import {
  arrayMove,
  SortableContext,
  sortableKeyboardCoordinates,
  useSortable,
  verticalListSortingStrategy,
} from '@dnd-kit/sortable';
import { CSS } from '@dnd-kit/utilities';

import {
  getRules,
  createRule,
  updateRule,
  toggleRule,
  deleteRule,
  reorderRules,
  getBonusLog,
  getCampaigns,
  getCountries,
} from '../../api/actionBonuses';
import type { ActionBonusRule, ActionType, BonusLogItem, Campaign } from '../../api/actionBonuses';

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

interface RuleForm {
  action: ActionType;
  countries: string[];
  affiliates: string[];
  reward_amount: string;
  isactive: boolean;
  // UI helpers
  all_countries: boolean;
  all_affiliates: boolean;
}

const ACTION_LABELS: Record<ActionType, string> = {
  live_details: 'Live Details',
  submit_documents: 'Submit Documents',
};

const EMPTY_FORM: RuleForm = {
  action: 'live_details',
  countries: [],
  affiliates: [],
  reward_amount: '',
  isactive: true,
  all_countries: true,
  all_affiliates: true,
};

// ---------------------------------------------------------------------------
// Sortable rule row
// ---------------------------------------------------------------------------

function SortableRuleRow({
  rule,
  index,
  isFirst,
  isLast,
  onToggle,
  onEdit,
  onDelete,
  campaignMap = {},
}: {
  rule: ActionBonusRule;
  index: number;
  isFirst: boolean;
  isLast: boolean;
  onToggle: (id: number) => void;
  onEdit: (rule: ActionBonusRule) => void;
  onDelete: (id: number) => void;
  campaignMap?: Record<string, string>;
}) {
  const { attributes, listeners, setNodeRef, transform, transition, isDragging } = useSortable({
    id: rule.id,
  });

  const style = {
    transform: CSS.Transform.toString(transform),
    transition,
    opacity: isDragging ? 0.5 : 1,
  };

  return (
    <div
      ref={setNodeRef}
      style={style}
      className={`flex items-center gap-3 px-4 py-3 bg-white dark:bg-gray-800 border border-gray-200 dark:border-gray-700 rounded-lg mb-2 ${isDragging ? 'shadow-lg' : 'hover:bg-gray-50 dark:hover:bg-gray-700/30'} transition-colors`}
    >
      {/* Drag handle */}
      <button
        {...attributes}
        {...listeners}
        className="text-gray-400 hover:text-gray-600 cursor-grab active:cursor-grabbing touch-none"
        title="Drag to reorder"
      >
        <svg className="w-4 h-4" fill="currentColor" viewBox="0 0 20 20">
          <path d="M7 2a2 2 0 1 0 .001 4.001A2 2 0 0 0 7 2zm0 6a2 2 0 1 0 .001 4.001A2 2 0 0 0 7 8zm0 6a2 2 0 1 0 .001 4.001A2 2 0 0 0 7 14zm6-8a2 2 0 1 0-.001-4.001A2 2 0 0 0 13 6zm0 2a2 2 0 1 0 .001 4.001A2 2 0 0 0 13 8zm0 6a2 2 0 1 0 .001 4.001A2 2 0 0 0 13 14z" />
        </svg>
      </button>

      {/* Priority badge */}
      <span className="text-xs text-gray-400 w-6 text-center font-mono">{index + 1}</span>

      {/* Priority label */}
      {isFirst && (
        <span className="text-xs bg-green-100 text-green-700 dark:bg-green-900/30 dark:text-green-400 px-2 py-0.5 rounded font-medium">
          Highest Priority
        </span>
      )}
      {isLast && !isFirst && (
        <span className="text-xs bg-gray-100 text-gray-500 dark:bg-gray-700 px-2 py-0.5 rounded font-medium">
          Lowest Priority
        </span>
      )}

      {/* Countries */}
      <div className="flex-1 min-w-0">
        <div className="text-xs text-gray-500 dark:text-gray-400 mb-0.5">Countries</div>
        {rule.countries && rule.countries.length > 0 ? (
          <div className="flex flex-wrap gap-1">
            {rule.countries.slice(0, 3).map(c => (
              <span key={c} className="text-xs bg-blue-100 text-blue-700 dark:bg-blue-900/30 dark:text-blue-400 px-1.5 py-0.5 rounded">
                {c}
              </span>
            ))}
            {rule.countries.length > 3 && (
              <span className="text-xs text-gray-400">+{rule.countries.length - 3} more</span>
            )}
          </div>
        ) : (
          <span className="text-xs bg-gray-100 text-gray-500 dark:bg-gray-700 dark:text-gray-400 px-1.5 py-0.5 rounded">
            All countries
          </span>
        )}
      </div>

      {/* Affiliates */}
      <div className="flex-1 min-w-0">
        <div className="text-xs text-gray-500 dark:text-gray-400 mb-0.5">Affiliates</div>
        {rule.affiliates && rule.affiliates.length > 0 ? (
          <div className="flex flex-wrap gap-1">
            {rule.affiliates.slice(0, 2).map(a => (
              <span key={a} className="text-xs bg-purple-100 text-purple-700 dark:bg-purple-900/30 dark:text-purple-400 px-1.5 py-0.5 rounded">
                {campaignMap[a] || a}
              </span>
            ))}
            {rule.affiliates.length > 2 && (
              <span className="text-xs text-gray-400">+{rule.affiliates.length - 2} more</span>
            )}
          </div>
        ) : (
          <span className="text-xs bg-gray-100 text-gray-500 dark:bg-gray-700 dark:text-gray-400 px-1.5 py-0.5 rounded">
            All affiliates
          </span>
        )}
      </div>

      {/* Reward */}
      <div className="text-sm font-semibold text-green-600 dark:text-green-400 w-16 text-right">
        ${rule.reward_amount.toFixed(2)}
      </div>

      {/* Status toggle */}
      <button
        onClick={() => onToggle(rule.id)}
        className={`text-xs px-2 py-1 rounded font-medium transition-colors ${
          rule.isactive
            ? 'bg-green-100 text-green-700 hover:bg-green-200 dark:bg-green-900/30 dark:text-green-400'
            : 'bg-gray-100 text-gray-500 hover:bg-gray-200 dark:bg-gray-700 dark:text-gray-400'
        }`}
      >
        {rule.isactive ? 'Active' : 'Inactive'}
      </button>

      {/* Actions */}
      <button
        onClick={() => onEdit(rule)}
        className="text-xs text-blue-600 hover:text-blue-700 dark:text-blue-400 font-medium"
      >
        Edit
      </button>
      <button
        onClick={() => onDelete(rule.id)}
        className="text-xs text-red-500 hover:text-red-600 dark:text-red-400 font-medium"
      >
        Delete
      </button>
    </div>
  );
}

// ---------------------------------------------------------------------------
// Multi-select component
// ---------------------------------------------------------------------------

function MultiSelect({
  options,
  selected,
  onChange,
  placeholder,
  searchable = true,
  labelMap = {},
}: {
  options: string[];
  selected: string[];
  onChange: (vals: string[]) => void;
  placeholder?: string;
  searchable?: boolean;
  labelMap?: Record<string, string>;
}) {
  const [search, setSearch] = useState('');
  const [open, setOpen] = useState(false);

  const label = (val: string) => labelMap[val] || val;

  const filtered = searchable
    ? options.filter(o => label(o).toLowerCase().includes(search.toLowerCase()))
    : options;

  const toggle = (val: string) => {
    onChange(selected.includes(val) ? selected.filter(s => s !== val) : [...selected, val]);
  };

  return (
    <div className="relative">
      <div
        className="min-h-9 rounded border border-gray-300 dark:border-gray-600 bg-white dark:bg-gray-800 px-2 py-1 cursor-pointer flex flex-wrap gap-1 items-center"
        onClick={() => setOpen(o => !o)}
      >
        {selected.length === 0 ? (
          <span className="text-sm text-gray-400">{placeholder || 'Select\u2026'}</span>
        ) : (
          selected.map(s => (
            <span
              key={s}
              className="text-xs bg-blue-100 text-blue-700 dark:bg-blue-900/30 dark:text-blue-400 px-1.5 py-0.5 rounded flex items-center gap-1"
            >
              {label(s)}
              <button
                onClick={e => { e.stopPropagation(); toggle(s); }}
                className="hover:text-red-500"
              >&times;</button>
            </span>
          ))
        )}
        <span className="ml-auto text-gray-400 text-xs">{open ? '\u25b2' : '\u25bc'}</span>
      </div>
      {open && (
        <div className="absolute z-20 top-full left-0 right-0 mt-1 bg-white dark:bg-gray-800 border border-gray-200 dark:border-gray-600 rounded shadow-lg max-h-48 overflow-y-auto">
          {searchable && (
            <div className="p-2 border-b border-gray-100 dark:border-gray-700">
              <input
                autoFocus
                type="text"
                value={search}
                onChange={e => setSearch(e.target.value)}
                placeholder="Search\u2026"
                className="w-full text-sm px-2 py-1 rounded border border-gray-200 dark:border-gray-600 bg-white dark:bg-gray-800"
                onClick={e => e.stopPropagation()}
              />
            </div>
          )}
          {filtered.length === 0 ? (
            <div className="px-3 py-2 text-sm text-gray-400">No results</div>
          ) : (
            filtered.map(opt => (
              <div
                key={opt}
                className="px-3 py-1.5 text-sm hover:bg-gray-50 dark:hover:bg-gray-700 cursor-pointer flex items-center gap-2"
                onClick={e => { e.stopPropagation(); toggle(opt); }}
              >
                <input type="checkbox" readOnly checked={selected.includes(opt)} className="accent-blue-600" />
                {label(opt)}
              </div>
            ))
          )}
        </div>
      )}
    </div>
  );
}

// ---------------------------------------------------------------------------
// Rule form modal
// ---------------------------------------------------------------------------

function RuleFormModal({
  initial,
  actionLocked,
  countries,
  campaigns,
  onSave,
  onClose,
}: {
  initial: RuleForm;
  actionLocked?: ActionType;
  countries: string[];
  campaigns: Campaign[];
  onSave: (form: RuleForm) => Promise<void>;
  onClose: () => void;
}) {
  const [form, setForm] = useState<RuleForm>(initial);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState('');

  const affiliateOptions = campaigns.map(c => c.id);
  const affiliateLabelMap = Object.fromEntries(campaigns.map(c => [c.id, c.name || c.id]));

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!form.reward_amount || isNaN(Number(form.reward_amount)) || Number(form.reward_amount) <= 0) {
      setError('Reward amount must be a positive number');
      return;
    }
    setLoading(true);
    setError('');
    try {
      await onSave(form);
    } catch (err: unknown) {
      setError(err instanceof Error ? err.message : 'Save failed');
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/40 p-4">
      <div className="bg-white dark:bg-gray-900 rounded-xl shadow-xl w-full max-w-lg max-h-[90vh] overflow-y-auto">
        <div className="px-6 py-4 border-b border-gray-200 dark:border-gray-700 flex justify-between items-center">
          <h2 className="text-base font-semibold text-gray-800 dark:text-gray-100">{initial.reward_amount ? 'Edit Rule' : 'Add Rule'}</h2>
          <button onClick={onClose} className="text-gray-400 hover:text-gray-600 text-xl leading-none">&times;</button>
        </div>

        <form onSubmit={handleSubmit} className="px-6 py-4 space-y-4">
          {/* Action */}
          <div>
            <label className="block text-xs text-gray-500 dark:text-gray-400 mb-1">Action</label>
            <select
              value={form.action}
              disabled={!!actionLocked}
              onChange={e => setForm(f => ({ ...f, action: e.target.value as ActionType }))}
              className="w-full rounded border border-gray-300 dark:border-gray-600 bg-white dark:bg-gray-800 px-3 py-2 text-sm disabled:opacity-60"
            >
              <option value="live_details">Live Details</option>
              <option value="submit_documents">Submit Documents</option>
            </select>
          </div>

          {/* Countries */}
          <div>
            <div className="flex items-center justify-between mb-1">
              <label className="text-xs text-gray-500 dark:text-gray-400">Countries</label>
              <label className="flex items-center gap-1 text-xs text-gray-500 dark:text-gray-400 cursor-pointer">
                <input
                  type="checkbox"
                  checked={form.all_countries}
                  onChange={e => setForm(f => ({ ...f, all_countries: e.target.checked, countries: e.target.checked ? [] : f.countries }))}
                  className="accent-blue-600"
                />
                All countries
              </label>
            </div>
            {!form.all_countries && (
              <MultiSelect
                options={countries}
                selected={form.countries}
                onChange={vals => setForm(f => ({ ...f, countries: vals }))}
                placeholder="Select countries\u2026"
              />
            )}
            {form.all_countries && (
              <div className="text-xs text-gray-400 bg-gray-50 dark:bg-gray-800 rounded px-2 py-1.5">
                Applies to all countries
              </div>
            )}
          </div>

          {/* Affiliates */}
          <div>
            <div className="flex items-center justify-between mb-1">
              <label className="text-xs text-gray-500 dark:text-gray-400">Affiliates / Campaigns</label>
              <label className="flex items-center gap-1 text-xs text-gray-500 dark:text-gray-400 cursor-pointer">
                <input
                  type="checkbox"
                  checked={form.all_affiliates}
                  onChange={e => setForm(f => ({ ...f, all_affiliates: e.target.checked, affiliates: e.target.checked ? [] : f.affiliates }))}
                  className="accent-blue-600"
                />
                All affiliates
              </label>
            </div>
            {!form.all_affiliates && (
              <MultiSelect
                options={affiliateOptions}
                selected={form.affiliates}
                onChange={vals => setForm(f => ({ ...f, affiliates: vals }))}
                placeholder="Select campaigns\u2026"
                labelMap={affiliateLabelMap}
              />
            )}
            {form.all_affiliates && (
              <div className="text-xs text-gray-400 bg-gray-50 dark:bg-gray-800 rounded px-2 py-1.5">
                Applies to all affiliates
              </div>
            )}
          </div>

          {/* Reward Amount */}
          <div>
            <label className="block text-xs text-gray-500 dark:text-gray-400 mb-1">Reward Amount (USD)</label>
            <input
              type="number"
              min="0.01"
              step="0.01"
              value={form.reward_amount}
              onChange={e => setForm(f => ({ ...f, reward_amount: e.target.value }))}
              placeholder="e.g. 25.00"
              className="w-full rounded border border-gray-300 dark:border-gray-600 bg-white dark:bg-gray-800 px-3 py-2 text-sm"
              required
            />
          </div>

          {/* Active */}
          <div className="flex items-center gap-2">
            <input
              type="checkbox"
              id="isactive"
              checked={form.isactive}
              onChange={e => setForm(f => ({ ...f, isactive: e.target.checked }))}
              className="accent-blue-600"
            />
            <label htmlFor="isactive" className="text-sm text-gray-700 dark:text-gray-300 cursor-pointer">Active</label>
          </div>

          {error && (
            <p className="bg-red-50 dark:bg-red-900/30 border border-red-200 dark:border-red-700 rounded-lg p-3 text-red-700 dark:text-red-300 text-sm">
              {error}
            </p>
          )}

          <div className="flex gap-2 pt-2">
            <button
              type="submit"
              disabled={loading}
              className="flex-1 px-4 py-2 bg-blue-600 hover:bg-blue-700 text-white text-sm font-medium rounded-lg transition-colors disabled:opacity-50"
            >
              {loading ? 'Saving\u2026' : 'Save Rule'}
            </button>
            <button
              type="button"
              onClick={onClose}
              className="flex-1 px-4 py-2 rounded-lg border border-gray-300 dark:border-gray-600 text-sm text-gray-700 dark:text-gray-300 hover:bg-gray-50 dark:hover:bg-gray-800 transition-colors"
            >
              Cancel
            </button>
          </div>
        </form>
      </div>
    </div>
  );
}

// ---------------------------------------------------------------------------
// Main page
// ---------------------------------------------------------------------------

export function ActionBonusesPage() {
  const [mainTab, setMainTab] = useState<'rules' | 'log'>('rules');
  const [actionTab, setActionTab] = useState<ActionType>('live_details');

  // Rules state
  const [rules, setRules] = useState<Record<ActionType, ActionBonusRule[]>>({
    live_details: [],
    submit_documents: [],
  });
  const [rulesLoading, setRulesLoading] = useState(false);

  // Form modal
  const [formModal, setFormModal] = useState<{ open: boolean; editing: ActionBonusRule | null }>({
    open: false,
    editing: null,
  });

  // Reference data
  const [countries, setCountries] = useState<string[]>([]);
  const [campaigns, setCampaigns] = useState<Campaign[]>([]);

  // Log state
  const [logItems, setLogItems] = useState<BonusLogItem[]>([]);
  const [logTotal, setLogTotal] = useState(0);
  const [logLoading, setLogLoading] = useState(false);
  const [logFilters, setLogFilters] = useState({
    action: '' as ActionType | '',
    country: '',
    success: '' as '' | 'true' | 'false',
    date_from: '',
    date_to: '',
    page: 1,
  });

  const [error, setError] = useState('');

  // DnD sensors
  const sensors = useSensors(
    useSensor(PointerSensor),
    useSensor(KeyboardSensor, { coordinateGetter: sortableKeyboardCoordinates }),
  );

  // Load rules
  const loadRules = useCallback(async () => {
    setRulesLoading(true);
    try {
      const all = await getRules();
      const grouped: Record<ActionType, ActionBonusRule[]> = {
        live_details: [],
        submit_documents: [],
      };
      for (const r of all) {
        grouped[r.action].push(r);
      }
      // Sort by priority
      for (const key of Object.keys(grouped) as ActionType[]) {
        grouped[key].sort((a, b) => a.priority - b.priority);
      }
      setRules(grouped);
    } catch (e: unknown) {
      setError(e instanceof Error ? e.message : 'Failed to load rules');
    } finally {
      setRulesLoading(false);
    }
  }, []);

  // Load log
  const loadLog = useCallback(async () => {
    setLogLoading(true);
    try {
      const params: Parameters<typeof getBonusLog>[0] = {
        page: logFilters.page,
        page_size: 50,
      };
      if (logFilters.action) params.action = logFilters.action as ActionType;
      if (logFilters.country) params.country = logFilters.country;
      if (logFilters.success !== '') params.success = logFilters.success === 'true';
      if (logFilters.date_from) params.date_from = logFilters.date_from;
      if (logFilters.date_to) params.date_to = logFilters.date_to;
      const data = await getBonusLog(params);
      setLogItems(data.items);
      setLogTotal(data.total);
    } catch (e: unknown) {
      setError(e instanceof Error ? e.message : 'Failed to load log');
    } finally {
      setLogLoading(false);
    }
  }, [logFilters]);

  useEffect(() => {
    loadRules();
    getCampaigns().then(setCampaigns).catch(() => {});
    getCountries().then(setCountries).catch(() => {});
  }, [loadRules]);

  useEffect(() => {
    if (mainTab === 'log') loadLog();
  }, [mainTab, loadLog]);

  // Drag end handler
  const handleDragEnd = async (event: DragEndEvent) => {
    const { active, over } = event;
    if (!over || active.id === over.id) return;

    const current = rules[actionTab];
    const oldIndex = current.findIndex(r => r.id === active.id);
    const newIndex = current.findIndex(r => r.id === over.id);
    if (oldIndex === -1 || newIndex === -1) return;

    const reordered = arrayMove(current, oldIndex, newIndex);
    setRules(prev => ({ ...prev, [actionTab]: reordered }));

    try {
      await reorderRules(actionTab, reordered.map(r => r.id));
    } catch {
      loadRules(); // revert on failure
    }
  };

  const handleToggle = async (id: number) => {
    try {
      await toggleRule(id);
      await loadRules();
    } catch (e: unknown) {
      setError(e instanceof Error ? e.message : 'Toggle failed');
    }
  };

  const handleDelete = async (id: number) => {
    if (!confirm('Delete this rule?')) return;
    try {
      await deleteRule(id);
      await loadRules();
    } catch (e: unknown) {
      setError(e instanceof Error ? e.message : 'Delete failed');
    }
  };

  const handleOpenCreate = () => {
    setFormModal({ open: true, editing: null });
  };

  const handleOpenEdit = (rule: ActionBonusRule) => {
    setFormModal({ open: true, editing: rule });
  };

  const handleSave = async (form: RuleForm) => {
    const payload = {
      action: form.action,
      countries: form.all_countries ? null : form.countries,
      affiliates: form.all_affiliates ? null : form.affiliates,
      reward_amount: Number(form.reward_amount),
      isactive: form.isactive,
    };
    if (formModal.editing) {
      await updateRule(formModal.editing.id, payload);
    } else {
      await createRule(payload);
    }
    setFormModal({ open: false, editing: null });
    await loadRules();
  };

  const buildInitialForm = (rule: ActionBonusRule | null): RuleForm => {
    if (!rule) return { ...EMPTY_FORM, action: actionTab };
    return {
      action: rule.action,
      countries: rule.countries ?? [],
      affiliates: rule.affiliates ?? [],
      reward_amount: String(rule.reward_amount),
      isactive: rule.isactive,
      all_countries: !rule.countries || rule.countries.length === 0,
      all_affiliates: !rule.affiliates || rule.affiliates.length === 0,
    };
  };

  const currentRules = rules[actionTab];

  return (
    <div className="space-y-6">
      {/* Header */}
      <div>
        <h1 className="text-xl font-semibold text-gray-800 dark:text-gray-100">Action Bonuses</h1>
        <p className="text-sm text-gray-500 dark:text-gray-400 mt-1">
          Rule-based credit rewards issued automatically when clients complete lifecycle events.
        </p>
      </div>

      {error && (
        <div className="bg-red-50 dark:bg-red-900/30 border border-red-200 dark:border-red-700 rounded-lg p-4 text-red-700 dark:text-red-300 text-sm flex justify-between items-start">
          <span>{error}</span>
          <button onClick={() => setError('')} className="text-red-400 hover:text-red-600 ml-2 flex-shrink-0">&times;</button>
        </div>
      )}

      {/* Main tabs */}
      <div className="flex border-b border-gray-200 dark:border-gray-700">
        {(['rules', 'log'] as const).map(tab => (
          <button
            key={tab}
            onClick={() => setMainTab(tab)}
            className={`px-4 py-2.5 text-sm font-medium border-b-2 transition-colors ${
              mainTab === tab
                ? 'border-blue-600 text-blue-600 dark:border-blue-400 dark:text-blue-400'
                : 'border-transparent text-gray-500 dark:text-gray-400 hover:text-gray-700 dark:hover:text-gray-300'
            }`}
          >
            {tab === 'rules' ? 'Rules' : 'Bonus Log'}
          </button>
        ))}
      </div>

      {/* Rules tab */}
      {mainTab === 'rules' && (
        <div className="space-y-4">
          {/* Action sub-tabs */}
          <div className="flex border-b border-gray-200 dark:border-gray-700">
            {(['live_details', 'submit_documents'] as ActionType[]).map(action => (
              <button
                key={action}
                onClick={() => setActionTab(action)}
                className={`px-4 py-2.5 text-sm font-medium border-b-2 transition-colors ${
                  actionTab === action
                    ? 'border-blue-600 text-blue-600 dark:border-blue-400 dark:text-blue-400'
                    : 'border-transparent text-gray-500 dark:text-gray-400 hover:text-gray-700 dark:hover:text-gray-300'
                }`}
              >
                {ACTION_LABELS[action]}
              </button>
            ))}
          </div>

          {/* Add rule button */}
          <div className="flex justify-end">
            <button
              onClick={handleOpenCreate}
              className="px-4 py-2 bg-blue-600 hover:bg-blue-700 text-white text-sm font-medium rounded-lg transition-colors"
            >
              + Add Rule
            </button>
          </div>

          {/* Rules list */}
          {rulesLoading ? (
            <div className="text-center text-gray-400 py-8 text-sm">Loading\u2026</div>
          ) : currentRules.length === 0 ? (
            <div className="text-center text-gray-400 py-12 text-sm border-2 border-dashed border-gray-200 dark:border-gray-700 rounded-lg">
              No rules for {ACTION_LABELS[actionTab]}. Add one to get started.
            </div>
          ) : (
            <DndContext sensors={sensors} collisionDetection={closestCenter} onDragEnd={handleDragEnd}>
              <SortableContext items={currentRules.map(r => r.id)} strategy={verticalListSortingStrategy}>
                {currentRules.map((rule, index) => (
                  <SortableRuleRow
                    key={rule.id}
                    rule={rule}
                    index={index}
                    isFirst={index === 0}
                    isLast={index === currentRules.length - 1}
                    onToggle={handleToggle}
                    onEdit={handleOpenEdit}
                    onDelete={handleDelete}
                    campaignMap={Object.fromEntries(campaigns.map(c => [c.id, c.name || c.id]))}
                  />
                ))}
              </SortableContext>
            </DndContext>
          )}

          {currentRules.length > 0 && (
            <p className="text-xs text-gray-400 text-center">
              Drag rows to reorder &mdash; highest priority rule fires first when multiple rules match a client.
            </p>
          )}
        </div>
      )}

      {/* Bonus Log tab */}
      {mainTab === 'log' && (
        <div className="space-y-4">
          {/* Filters */}
          <div className="flex flex-wrap gap-3">
            <select
              value={logFilters.action}
              onChange={e => setLogFilters(f => ({ ...f, action: e.target.value as ActionType | '', page: 1 }))}
              className="rounded-lg border border-gray-300 dark:border-gray-600 bg-white dark:bg-gray-800 text-sm px-3 py-1.5"
            >
              <option value="">All actions</option>
              <option value="live_details">Live Details</option>
              <option value="submit_documents">Submit Documents</option>
            </select>

            <select
              value={logFilters.success}
              onChange={e => setLogFilters(f => ({ ...f, success: e.target.value as '' | 'true' | 'false', page: 1 }))}
              className="rounded-lg border border-gray-300 dark:border-gray-600 bg-white dark:bg-gray-800 text-sm px-3 py-1.5"
            >
              <option value="">All statuses</option>
              <option value="true">Success</option>
              <option value="false">Failed</option>
            </select>

            <input
              type="text"
              placeholder="Country\u2026"
              value={logFilters.country}
              onChange={e => setLogFilters(f => ({ ...f, country: e.target.value, page: 1 }))}
              className="rounded-lg border border-gray-300 dark:border-gray-600 bg-white dark:bg-gray-800 text-sm px-3 py-1.5 w-36"
            />

            <input
              type="date"
              value={logFilters.date_from}
              onChange={e => setLogFilters(f => ({ ...f, date_from: e.target.value, page: 1 }))}
              className="rounded-lg border border-gray-300 dark:border-gray-600 bg-white dark:bg-gray-800 text-sm px-3 py-1.5"
            />
            <span className="text-gray-400 self-center text-sm">&rarr;</span>
            <input
              type="date"
              value={logFilters.date_to}
              onChange={e => setLogFilters(f => ({ ...f, date_to: e.target.value, page: 1 }))}
              className="rounded-lg border border-gray-300 dark:border-gray-600 bg-white dark:bg-gray-800 text-sm px-3 py-1.5"
            />

            <button
              onClick={loadLog}
              className="px-4 py-2 bg-blue-600 hover:bg-blue-700 text-white text-sm font-medium rounded-lg transition-colors"
            >
              Search
            </button>
          </div>

          {logLoading ? (
            <div className="text-center text-gray-400 py-8 text-sm">Loading\u2026</div>
          ) : (
            <>
              <div className="bg-white dark:bg-gray-800 rounded-xl shadow overflow-hidden">
                <div className="overflow-x-auto">
                  <table className="min-w-full">
                    <thead className="bg-gray-50 dark:bg-gray-700/50">
                      <tr>
                        <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider">Account ID</th>
                        <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider">Action</th>
                        <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider">Country</th>
                        <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider">Affiliate</th>
                        <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider">Rule</th>
                        <th className="px-6 py-3 text-right text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider">Reward</th>
                        <th className="px-6 py-3 text-center text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider">Status</th>
                        <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider">Timestamp</th>
                      </tr>
                    </thead>
                    <tbody className="divide-y divide-gray-200 dark:divide-gray-700">
                      {logItems.length === 0 ? (
                        <tr>
                          <td colSpan={8} className="text-center py-8 text-gray-400 text-sm">No log entries found</td>
                        </tr>
                      ) : (
                        logItems.map(item => (
                          <tr key={item.id} className="hover:bg-gray-50 dark:hover:bg-gray-700/30 transition-colors">
                            <td className="px-6 py-4 text-sm font-mono text-xs text-gray-800 dark:text-gray-200">{item.accountid}</td>
                            <td className="px-6 py-4 text-sm">
                              <span className={`text-xs px-1.5 py-0.5 rounded font-medium ${
                                item.action === 'live_details'
                                  ? 'bg-blue-100 text-blue-700 dark:bg-blue-900/30 dark:text-blue-400'
                                  : 'bg-violet-100 text-violet-700 dark:bg-violet-900/30 dark:text-violet-400'
                              }`}>
                                {ACTION_LABELS[item.action]}
                              </span>
                            </td>
                            <td className="px-6 py-4 text-sm text-gray-600 dark:text-gray-400">{item.country || '\u2014'}</td>
                            <td className="px-6 py-4 text-sm text-gray-600 dark:text-gray-400 font-mono text-xs">{item.affiliate || '\u2014'}</td>
                            <td className="px-6 py-4 text-sm text-gray-500 dark:text-gray-400">#{item.rule_id}</td>
                            <td className="px-6 py-4 text-sm text-right font-semibold text-green-600 dark:text-green-400">
                              ${item.reward_amount.toFixed(2)}
                            </td>
                            <td className="px-6 py-4 text-sm text-center">
                              <span className={`text-xs px-1.5 py-0.5 rounded font-medium ${
                                item.success
                                  ? 'bg-green-100 text-green-700 dark:bg-green-900/30 dark:text-green-400'
                                  : 'bg-red-100 text-red-700 dark:bg-red-900/30 dark:text-red-400'
                              }`}>
                                {item.success ? 'Success' : 'Failed'}
                              </span>
                            </td>
                            <td className="px-6 py-4 text-sm text-gray-500 dark:text-gray-400 whitespace-nowrap">
                              {item.created_at ? new Date(item.created_at).toLocaleString() : '\u2014'}
                            </td>
                          </tr>
                        ))
                      )}
                    </tbody>
                  </table>
                </div>
              </div>

              {/* Pagination */}
              {logTotal > 50 && (
                <div className="flex justify-between items-center text-sm text-gray-500 dark:text-gray-400">
                  <span>{logTotal} total entries</span>
                  <div className="flex gap-2">
                    <button
                      disabled={logFilters.page === 1}
                      onClick={() => setLogFilters(f => ({ ...f, page: f.page - 1 }))}
                      className="px-3 py-1 rounded-lg border border-gray-300 dark:border-gray-600 disabled:opacity-40 hover:bg-gray-50 dark:hover:bg-gray-800 transition-colors"
                    >
                      &larr; Prev
                    </button>
                    <span className="px-2 py-1">Page {logFilters.page}</span>
                    <button
                      disabled={logFilters.page * 50 >= logTotal}
                      onClick={() => setLogFilters(f => ({ ...f, page: f.page + 1 }))}
                      className="px-3 py-1 rounded-lg border border-gray-300 dark:border-gray-600 disabled:opacity-40 hover:bg-gray-50 dark:hover:bg-gray-800 transition-colors"
                    >
                      Next &rarr;
                    </button>
                  </div>
                </div>
              )}
            </>
          )}
        </div>
      )}

      {/* Form modal */}
      {formModal.open && (
        <RuleFormModal
          initial={buildInitialForm(formModal.editing)}
          actionLocked={formModal.editing ? formModal.editing.action : undefined}
          countries={countries}
          campaigns={campaigns}
          onSave={handleSave}
          onClose={() => setFormModal({ open: false, editing: null })}
        />
      )}
    </div>
  );
}
