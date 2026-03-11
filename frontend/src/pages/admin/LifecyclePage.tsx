/**
 * CLAUD-97: Lifecycle Stages -- Admin settings page for configurable client lifecycle milestones.
 * DnD sortable list, live preview bar, inline edit, add/delete with confirmation.
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
  getStages,
  createStage,
  updateStage,
  deleteStage,
  toggleStage,
  reorderStages,
} from '../../api/lifecycle';
import type { LifecycleStage, MetricType } from '../../api/lifecycle';

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const METRIC_LABELS: Record<MetricType, string> = {
  ftd: 'FTD',
  deposit: 'Deposit',
  position: 'Position',
  volume: 'Volume',
  custom: 'Custom',
};

const METRIC_OPTIONS: { value: MetricType; label: string }[] = [
  { value: 'ftd', label: 'FTD (First Time Deposit)' },
  { value: 'deposit', label: 'Deposit (cumulative count)' },
  { value: 'position', label: 'Position (trades opened)' },
  { value: 'volume', label: 'Volume (trading USD)' },
  { value: 'custom', label: 'Custom' },
];

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function formatThreshold(val: number): string {
  if (val >= 1_000_000) {
    const m = val / 1_000_000;
    return Number.isInteger(m) ? `${m}M` : `${m.toFixed(1)}M`;
  }
  if (val >= 1_000) {
    const k = val / 1_000;
    return Number.isInteger(k) ? `${k}K` : `${k.toFixed(1)}K`;
  }
  return String(val);
}

function formatThresholdDisplay(val: number): string {
  return val.toLocaleString('en-US', { maximumFractionDigits: 2 });
}

// ---------------------------------------------------------------------------
// Live Preview Bar
// ---------------------------------------------------------------------------

function PreviewBar({ stages }: { stages: LifecycleStage[] }) {
  const active = stages.filter((s) => s.is_active);

  if (active.length === 0) {
    return (
      <div className="text-sm text-gray-400 dark:text-gray-500 italic py-4 text-center">
        No active stages to preview
      </div>
    );
  }

  return (
    <div className="flex items-center justify-center gap-0 overflow-x-auto py-4 px-2">
      {active.map((stage, i) => (
        <div key={stage.id} className="flex items-center">
          {/* Connector line (before, except first) */}
          {i > 0 && (
            <div className="w-8 sm:w-12 h-0.5 bg-gray-300 dark:bg-gray-600 flex-shrink-0" />
          )}
          {/* Stage circle + label */}
          <div className="flex flex-col items-center flex-shrink-0">
            <div className="w-9 h-9 rounded-full border-2 border-gray-300 dark:border-gray-600 flex items-center justify-center bg-white dark:bg-gray-800">
              <span className="text-[10px] font-semibold text-gray-400 dark:text-gray-500">
                {i + 1}
              </span>
            </div>
            <span className="text-[10px] text-gray-500 dark:text-gray-400 mt-1 max-w-[56px] text-center truncate leading-tight">
              {stage.name}
            </span>
            <span className="text-[9px] text-gray-400 dark:text-gray-500 leading-tight">
              {formatThreshold(stage.threshold)}
            </span>
          </div>
        </div>
      ))}
    </div>
  );
}

// ---------------------------------------------------------------------------
// Sortable Row
// ---------------------------------------------------------------------------

function SortableRow({
  stage,
  index,
  total,
  isEditing,
  editForm,
  onEditFormChange,
  onStartEdit,
  onSaveEdit,
  onCancelEdit,
  onToggle,
  onDelete,
  onMoveUp,
  onMoveDown,
}: {
  stage: LifecycleStage;
  index: number;
  total: number;
  isEditing: boolean;
  editForm: { name: string; metric_type: MetricType; threshold: string };
  onEditFormChange: (field: string, value: string) => void;
  onStartEdit: () => void;
  onSaveEdit: () => void;
  onCancelEdit: () => void;
  onToggle: () => void;
  onDelete: () => void;
  onMoveUp: () => void;
  onMoveDown: () => void;
}) {
  const { attributes, listeners, setNodeRef, transform, transition, isDragging } = useSortable({
    id: stage.id,
  });

  const style = {
    transform: CSS.Transform.toString(transform),
    transition,
    opacity: isDragging ? 0.5 : 1,
  };

  if (isEditing) {
    return (
      <tr ref={setNodeRef} style={style} className="bg-blue-50 dark:bg-blue-900/20">
        {/* # */}
        <td className="px-6 py-3 text-sm text-gray-500 dark:text-gray-400 font-mono">{index + 1}</td>
        {/* Name */}
        <td className="px-6 py-3">
          <input
            type="text"
            value={editForm.name}
            onChange={(e) => onEditFormChange('name', e.target.value)}
            className="w-full px-2 py-1 text-sm rounded border border-gray-300 dark:border-gray-600 bg-white dark:bg-gray-800 text-gray-900 dark:text-gray-100"
          />
        </td>
        {/* Metric Type */}
        <td className="px-6 py-3">
          <select
            value={editForm.metric_type}
            onChange={(e) => onEditFormChange('metric_type', e.target.value)}
            className="w-full px-2 py-1 text-sm rounded border border-gray-300 dark:border-gray-600 bg-white dark:bg-gray-800 text-gray-900 dark:text-gray-100"
          >
            {METRIC_OPTIONS.map((opt) => (
              <option key={opt.value} value={opt.value}>{opt.label}</option>
            ))}
          </select>
        </td>
        {/* Threshold */}
        <td className="px-6 py-3">
          <input
            type="number"
            step="any"
            min="0.01"
            value={editForm.threshold}
            onChange={(e) => onEditFormChange('threshold', e.target.value)}
            className="w-32 px-2 py-1 text-sm rounded border border-gray-300 dark:border-gray-600 bg-white dark:bg-gray-800 text-gray-900 dark:text-gray-100"
          />
        </td>
        {/* Status */}
        <td className="px-6 py-3">
          <span className={`text-xs px-2 py-1 rounded font-medium ${
            stage.is_active
              ? 'bg-green-100 text-green-700 dark:bg-green-900/30 dark:text-green-400'
              : 'bg-gray-100 text-gray-500 dark:bg-gray-700 dark:text-gray-400'
          }`}>
            {stage.is_active ? 'Active' : 'Inactive'}
          </span>
        </td>
        {/* Actions */}
        <td className="px-6 py-3">
          <div className="flex items-center gap-2">
            <button
              onClick={onSaveEdit}
              className="px-3 py-1 bg-blue-600 hover:bg-blue-700 text-white text-xs font-medium rounded-lg transition-colors"
            >
              Save
            </button>
            <button
              onClick={onCancelEdit}
              className="px-3 py-1 bg-gray-200 hover:bg-gray-300 dark:bg-gray-700 dark:hover:bg-gray-600 text-gray-700 dark:text-gray-300 text-xs font-medium rounded-lg transition-colors"
            >
              Cancel
            </button>
          </div>
        </td>
      </tr>
    );
  }

  return (
    <tr
      ref={setNodeRef}
      style={style}
      className={`hover:bg-gray-50 dark:hover:bg-gray-700/30 transition-colors ${isDragging ? 'shadow-lg' : ''}`}
    >
      {/* # with drag handle and move buttons */}
      <td className="px-6 py-4 text-sm">
        <div className="flex items-center gap-1">
          <button
            {...attributes}
            {...listeners}
            className="text-gray-400 hover:text-gray-600 dark:hover:text-gray-300 cursor-grab active:cursor-grabbing touch-none"
            title="Drag to reorder"
          >
            <svg className="w-4 h-4" fill="currentColor" viewBox="0 0 20 20">
              <path d="M7 2a2 2 0 1 0 .001 4.001A2 2 0 0 0 7 2zm0 6a2 2 0 1 0 .001 4.001A2 2 0 0 0 7 8zm0 6a2 2 0 1 0 .001 4.001A2 2 0 0 0 7 14zm6-8a2 2 0 1 0-.001-4.001A2 2 0 0 0 13 6zm0 2a2 2 0 1 0 .001 4.001A2 2 0 0 0 13 8zm0 6a2 2 0 1 0 .001 4.001A2 2 0 0 0 13 14z" />
            </svg>
          </button>
          <span className="text-gray-500 dark:text-gray-400 font-mono w-6 text-center">{index + 1}</span>
          <div className="flex flex-col gap-0.5 ml-1">
            <button
              onClick={onMoveUp}
              disabled={index === 0}
              className="text-gray-400 hover:text-gray-600 dark:hover:text-gray-300 disabled:opacity-30 disabled:cursor-not-allowed"
              title="Move up"
            >
              <svg className="w-3 h-3" fill="currentColor" viewBox="0 0 20 20">
                <path fillRule="evenodd" d="M14.707 12.707a1 1 0 01-1.414 0L10 9.414l-3.293 3.293a1 1 0 01-1.414-1.414l4-4a1 1 0 011.414 0l4 4a1 1 0 010 1.414z" clipRule="evenodd" />
              </svg>
            </button>
            <button
              onClick={onMoveDown}
              disabled={index === total - 1}
              className="text-gray-400 hover:text-gray-600 dark:hover:text-gray-300 disabled:opacity-30 disabled:cursor-not-allowed"
              title="Move down"
            >
              <svg className="w-3 h-3" fill="currentColor" viewBox="0 0 20 20">
                <path fillRule="evenodd" d="M5.293 7.293a1 1 0 011.414 0L10 10.586l3.293-3.293a1 1 0 111.414 1.414l-4 4a1 1 0 01-1.414 0l-4-4a1 1 0 010-1.414z" clipRule="evenodd" />
              </svg>
            </button>
          </div>
        </div>
      </td>
      {/* Stage Name */}
      <td className="px-6 py-4 text-sm text-gray-900 dark:text-gray-100 font-medium">{stage.name}</td>
      {/* Metric Type */}
      <td className="px-6 py-4">
        <span className="text-xs bg-blue-100 text-blue-700 dark:bg-blue-900/30 dark:text-blue-400 px-2 py-0.5 rounded font-medium">
          {METRIC_LABELS[stage.metric_type as MetricType] || stage.metric_type}
        </span>
      </td>
      {/* Threshold */}
      <td className="px-6 py-4 text-sm text-gray-700 dark:text-gray-300 font-mono">
        {formatThresholdDisplay(stage.threshold)}
      </td>
      {/* Status toggle */}
      <td className="px-6 py-4">
        <button
          onClick={onToggle}
          className={`text-xs px-2 py-1 rounded font-medium transition-colors cursor-pointer ${
            stage.is_active
              ? 'bg-green-100 text-green-700 hover:bg-green-200 dark:bg-green-900/30 dark:text-green-400'
              : 'bg-gray-100 text-gray-500 hover:bg-gray-200 dark:bg-gray-700 dark:text-gray-400'
          }`}
        >
          {stage.is_active ? 'Active' : 'Inactive'}
        </button>
      </td>
      {/* Actions */}
      <td className="px-6 py-4">
        <div className="flex items-center gap-3">
          <button
            onClick={onStartEdit}
            className="text-xs text-blue-600 hover:text-blue-700 dark:text-blue-400 font-medium"
          >
            Edit
          </button>
          <button
            onClick={onDelete}
            className="text-xs text-red-500 hover:text-red-600 dark:text-red-400 font-medium"
          >
            Delete
          </button>
        </div>
      </td>
    </tr>
  );
}

// ---------------------------------------------------------------------------
// Delete Confirmation Modal
// ---------------------------------------------------------------------------

function DeleteModal({
  stageName,
  onConfirm,
  onCancel,
  loading,
}: {
  stageName: string;
  onConfirm: () => void;
  onCancel: () => void;
  loading: boolean;
}) {
  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/40 p-4">
      <div className="bg-white dark:bg-gray-900 rounded-xl shadow-xl w-full max-w-md">
        <div className="px-6 py-4 border-b border-gray-200 dark:border-gray-700">
          <h2 className="text-base font-semibold text-gray-800 dark:text-gray-100">Delete Stage</h2>
        </div>
        <div className="px-6 py-4 space-y-3">
          <p className="text-sm text-gray-700 dark:text-gray-300">
            Are you sure you want to delete <strong>{stageName}</strong>?
          </p>
          <div className="bg-amber-50 dark:bg-amber-900/20 border border-amber-200 dark:border-amber-700 rounded-lg p-3">
            <p className="text-xs text-amber-700 dark:text-amber-300">
              Clients who have already reached this milestone retain their data, but the stage will no longer appear on the progress bar.
            </p>
          </div>
        </div>
        <div className="px-6 py-4 border-t border-gray-200 dark:border-gray-700 flex justify-end gap-3">
          <button
            onClick={onCancel}
            disabled={loading}
            className="px-4 py-2 bg-gray-200 hover:bg-gray-300 dark:bg-gray-700 dark:hover:bg-gray-600 text-gray-700 dark:text-gray-300 text-sm font-medium rounded-lg transition-colors"
          >
            Cancel
          </button>
          <button
            onClick={onConfirm}
            disabled={loading}
            className="px-4 py-2 bg-red-600 hover:bg-red-700 text-white text-sm font-medium rounded-lg transition-colors disabled:opacity-50"
          >
            {loading ? 'Deleting...' : 'Delete'}
          </button>
        </div>
      </div>
    </div>
  );
}

// ---------------------------------------------------------------------------
// Main Page
// ---------------------------------------------------------------------------

export function LifecyclePage() {
  const [stages, setStages] = useState<LifecycleStage[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState('');

  // Inline edit state
  const [editingId, setEditingId] = useState<number | null>(null);
  const [editForm, setEditForm] = useState({ name: '', metric_type: 'ftd' as MetricType, threshold: '' });

  // Add form state
  const [showAddForm, setShowAddForm] = useState(false);
  const [addForm, setAddForm] = useState({ name: '', metric_type: 'deposit' as MetricType, threshold: '' });
  const [addError, setAddError] = useState('');
  const [addLoading, setAddLoading] = useState(false);

  // Delete modal state
  const [deleteTarget, setDeleteTarget] = useState<LifecycleStage | null>(null);
  const [deleteLoading, setDeleteLoading] = useState(false);

  // DnD sensors
  const sensors = useSensors(
    useSensor(PointerSensor, { activationConstraint: { distance: 5 } }),
    useSensor(KeyboardSensor, { coordinateGetter: sortableKeyboardCoordinates }),
  );

  // ---------------------------------------------------------------------------
  // Data fetch
  // ---------------------------------------------------------------------------

  const fetchStages = useCallback(async () => {
    try {
      setError('');
      const data = await getStages();
      setStages(data);
    } catch (err: unknown) {
      setError(err instanceof Error ? err.message : 'Failed to load stages');
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    fetchStages();
  }, [fetchStages]);

  // ---------------------------------------------------------------------------
  // Drag and drop
  // ---------------------------------------------------------------------------

  const handleDragEnd = async (event: DragEndEvent) => {
    const { active, over } = event;
    if (!over || active.id === over.id) return;

    const oldIndex = stages.findIndex((s) => s.id === active.id);
    const newIndex = stages.findIndex((s) => s.id === over.id);
    if (oldIndex === -1 || newIndex === -1) return;

    const newStages = arrayMove(stages, oldIndex, newIndex);
    setStages(newStages);

    try {
      await reorderStages(newStages.map((s) => s.id));
    } catch {
      fetchStages(); // revert on failure
    }
  };

  // ---------------------------------------------------------------------------
  // Move up/down
  // ---------------------------------------------------------------------------

  const handleMove = async (index: number, direction: 'up' | 'down') => {
    const newIndex = direction === 'up' ? index - 1 : index + 1;
    if (newIndex < 0 || newIndex >= stages.length) return;

    const newStages = arrayMove(stages, index, newIndex);
    setStages(newStages);

    try {
      await reorderStages(newStages.map((s) => s.id));
    } catch {
      fetchStages();
    }
  };

  // ---------------------------------------------------------------------------
  // Inline edit
  // ---------------------------------------------------------------------------

  const startEdit = (stage: LifecycleStage) => {
    setEditingId(stage.id);
    setEditForm({
      name: stage.name,
      metric_type: stage.metric_type as MetricType,
      threshold: String(stage.threshold),
    });
  };

  const cancelEdit = () => {
    setEditingId(null);
  };

  const saveEdit = async () => {
    if (!editingId) return;
    const threshold = Number(editForm.threshold);
    if (!editForm.name.trim() || isNaN(threshold) || threshold <= 0) {
      setError('Name and threshold (> 0) are required.');
      return;
    }
    try {
      setError('');
      const updated = await updateStage(editingId, {
        name: editForm.name.trim(),
        metric_type: editForm.metric_type,
        threshold,
      });
      setStages((prev) => prev.map((s) => (s.id === editingId ? updated : s)));
      setEditingId(null);
    } catch (err: unknown) {
      setError(err instanceof Error ? err.message : 'Failed to save');
    }
  };

  // ---------------------------------------------------------------------------
  // Toggle
  // ---------------------------------------------------------------------------

  const handleToggle = async (id: number) => {
    try {
      setError('');
      const result = await toggleStage(id);
      setStages((prev) =>
        prev.map((s) => (s.id === id ? { ...s, is_active: result.is_active } : s)),
      );
    } catch (err: unknown) {
      setError(err instanceof Error ? err.message : 'Failed to toggle');
    }
  };

  // ---------------------------------------------------------------------------
  // Delete
  // ---------------------------------------------------------------------------

  const confirmDelete = async () => {
    if (!deleteTarget) return;
    setDeleteLoading(true);
    try {
      setError('');
      await deleteStage(deleteTarget.id);
      setStages((prev) => prev.filter((s) => s.id !== deleteTarget.id));
      setDeleteTarget(null);
    } catch (err: unknown) {
      setError(err instanceof Error ? err.message : 'Failed to delete');
    } finally {
      setDeleteLoading(false);
    }
  };

  // ---------------------------------------------------------------------------
  // Add stage
  // ---------------------------------------------------------------------------

  const handleAdd = async () => {
    const threshold = Number(addForm.threshold);
    if (!addForm.name.trim()) {
      setAddError('Stage name is required.');
      return;
    }
    if (isNaN(threshold) || threshold <= 0) {
      setAddError('Threshold must be a positive number.');
      return;
    }
    setAddLoading(true);
    setAddError('');
    try {
      const created = await createStage({
        name: addForm.name.trim(),
        metric_type: addForm.metric_type,
        threshold,
      });
      setStages((prev) => [...prev, created]);
      setAddForm({ name: '', metric_type: 'deposit', threshold: '' });
      setShowAddForm(false);
    } catch (err: unknown) {
      setAddError(err instanceof Error ? err.message : 'Failed to create stage');
    } finally {
      setAddLoading(false);
    }
  };

  // ---------------------------------------------------------------------------
  // Auto-generated key preview
  // ---------------------------------------------------------------------------

  const previewKey = addForm.name
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9_\s]/g, '')
    .replace(/\s+/g, '_');

  // ---------------------------------------------------------------------------
  // Render
  // ---------------------------------------------------------------------------

  if (loading) {
    return (
      <div className="flex items-center justify-center h-64">
        <div className="text-gray-400 dark:text-gray-500">Loading lifecycle stages...</div>
      </div>
    );
  }

  return (
    <div className="space-y-6 max-w-6xl mx-auto">
      {/* Page title */}
      <div className="flex items-center justify-between">
        <h1 className="text-xl font-semibold text-gray-800 dark:text-gray-100">Lifecycle Stages</h1>
        <span className="text-sm text-gray-400 dark:text-gray-500">
          {stages.length} stage{stages.length !== 1 ? 's' : ''} configured
        </span>
      </div>

      {/* Error banner */}
      {error && (
        <div className="bg-red-50 dark:bg-red-900/30 border border-red-200 dark:border-red-700 rounded-lg p-4 text-red-700 dark:text-red-300 text-sm flex items-center justify-between">
          <span>{error}</span>
          <button onClick={() => setError('')} className="ml-2 text-red-400 hover:text-red-600 text-lg leading-none">&times;</button>
        </div>
      )}

      {/* Live Preview */}
      <div className="bg-white dark:bg-gray-800 rounded-xl shadow p-4">
        <h2 className="text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider mb-2">
          Live Preview -- Client Lifecycle Bar
        </h2>
        <PreviewBar stages={stages} />
      </div>

      {/* Stage List Table */}
      <div className="bg-white dark:bg-gray-800 rounded-xl shadow overflow-hidden">
        <div className="overflow-x-auto">
          <DndContext sensors={sensors} collisionDetection={closestCenter} onDragEnd={handleDragEnd}>
            <table className="w-full">
              <thead>
                <tr className="bg-gray-50 dark:bg-gray-700/50">
                  <th className="px-6 py-3 text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider text-left">#</th>
                  <th className="px-6 py-3 text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider text-left">Stage Name</th>
                  <th className="px-6 py-3 text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider text-left">Metric Type</th>
                  <th className="px-6 py-3 text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider text-left">Threshold</th>
                  <th className="px-6 py-3 text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider text-left">Status</th>
                  <th className="px-6 py-3 text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider text-left">Actions</th>
                </tr>
              </thead>
              <SortableContext items={stages.map((s) => s.id)} strategy={verticalListSortingStrategy}>
                <tbody className="divide-y divide-gray-200 dark:divide-gray-700">
                  {stages.map((stage, index) => (
                    <SortableRow
                      key={stage.id}
                      stage={stage}
                      index={index}
                      total={stages.length}
                      isEditing={editingId === stage.id}
                      editForm={editForm}
                      onEditFormChange={(field, value) =>
                        setEditForm((prev) => ({ ...prev, [field]: value }))
                      }
                      onStartEdit={() => startEdit(stage)}
                      onSaveEdit={saveEdit}
                      onCancelEdit={cancelEdit}
                      onToggle={() => handleToggle(stage.id)}
                      onDelete={() => setDeleteTarget(stage)}
                      onMoveUp={() => handleMove(index, 'up')}
                      onMoveDown={() => handleMove(index, 'down')}
                    />
                  ))}
                </tbody>
              </SortableContext>
            </table>
          </DndContext>
        </div>

        {stages.length === 0 && (
          <div className="py-12 text-center text-gray-400 dark:text-gray-500 text-sm">
            No lifecycle stages configured. Add your first stage below.
          </div>
        )}
      </div>

      {/* Add Stage Section */}
      <div className="bg-white dark:bg-gray-800 rounded-xl shadow">
        {!showAddForm ? (
          <div className="p-4">
            <button
              onClick={() => setShowAddForm(true)}
              className="px-4 py-2 bg-blue-600 hover:bg-blue-700 text-white text-sm font-medium rounded-lg transition-colors"
            >
              + Add Stage
            </button>
          </div>
        ) : (
          <div className="p-4 space-y-4">
            <h3 className="text-sm font-semibold text-gray-800 dark:text-gray-100">Add New Stage</h3>

            {addError && (
              <div className="bg-red-50 dark:bg-red-900/30 border border-red-200 dark:border-red-700 rounded-lg p-3 text-red-700 dark:text-red-300 text-sm">
                {addError}
              </div>
            )}

            <div className="grid grid-cols-1 sm:grid-cols-3 gap-4">
              {/* Stage Name */}
              <div>
                <label className="block text-xs text-gray-500 dark:text-gray-400 mb-1">Stage Name</label>
                <input
                  type="text"
                  value={addForm.name}
                  onChange={(e) => setAddForm((prev) => ({ ...prev, name: e.target.value }))}
                  placeholder="e.g. 5x Dep"
                  maxLength={64}
                  className="w-full px-3 py-2 text-sm rounded-lg border border-gray-300 dark:border-gray-600 bg-white dark:bg-gray-800 text-gray-900 dark:text-gray-100"
                />
                {previewKey && (
                  <span className="text-[10px] text-gray-400 dark:text-gray-500 mt-1 block">
                    Key: {previewKey}
                  </span>
                )}
              </div>

              {/* Metric Type */}
              <div>
                <label className="block text-xs text-gray-500 dark:text-gray-400 mb-1">Metric Type</label>
                <select
                  value={addForm.metric_type}
                  onChange={(e) => setAddForm((prev) => ({ ...prev, metric_type: e.target.value as MetricType }))}
                  className="w-full px-3 py-2 text-sm rounded-lg border border-gray-300 dark:border-gray-600 bg-white dark:bg-gray-800 text-gray-900 dark:text-gray-100"
                >
                  {METRIC_OPTIONS.map((opt) => (
                    <option key={opt.value} value={opt.value}>{opt.label}</option>
                  ))}
                </select>
              </div>

              {/* Threshold */}
              <div>
                <label className="block text-xs text-gray-500 dark:text-gray-400 mb-1">Threshold</label>
                <input
                  type="number"
                  step="any"
                  min="0.01"
                  value={addForm.threshold}
                  onChange={(e) => setAddForm((prev) => ({ ...prev, threshold: e.target.value }))}
                  placeholder="e.g. 5"
                  className="w-full px-3 py-2 text-sm rounded-lg border border-gray-300 dark:border-gray-600 bg-white dark:bg-gray-800 text-gray-900 dark:text-gray-100"
                />
              </div>
            </div>

            <div className="flex items-center gap-3 pt-2">
              <button
                onClick={handleAdd}
                disabled={addLoading}
                className="px-4 py-2 bg-blue-600 hover:bg-blue-700 text-white text-sm font-medium rounded-lg transition-colors disabled:opacity-50"
              >
                {addLoading ? 'Saving...' : 'Save Stage'}
              </button>
              <button
                onClick={() => {
                  setShowAddForm(false);
                  setAddForm({ name: '', metric_type: 'deposit', threshold: '' });
                  setAddError('');
                }}
                className="px-4 py-2 bg-gray-200 hover:bg-gray-300 dark:bg-gray-700 dark:hover:bg-gray-600 text-gray-700 dark:text-gray-300 text-sm font-medium rounded-lg transition-colors"
              >
                Cancel
              </button>
            </div>
          </div>
        )}
      </div>

      {/* Delete confirmation modal */}
      {deleteTarget && (
        <DeleteModal
          stageName={deleteTarget.name}
          onConfirm={confirmDelete}
          onCancel={() => setDeleteTarget(null)}
          loading={deleteLoading}
        />
      )}
    </div>
  );
}
