import { useCallManager } from '../hooks/useCallManager';
import { useAppStore } from '../store/useAppStore';

export function CallButton() {
  const { selectedIds, isCalling } = useAppStore();
  const { callSelected } = useCallManager();

  const count = selectedIds.size;
  if (count === 0) return null;

  return (
    <button
      onClick={callSelected}
      disabled={isCalling}
      className="px-4 py-2 bg-green-600 text-white rounded-md text-sm font-medium hover:bg-green-700 disabled:opacity-50 disabled:cursor-not-allowed transition-colors"
    >
      {isCalling ? 'Calling…' : `Call Selected (${count})`}
    </button>
  );
}
