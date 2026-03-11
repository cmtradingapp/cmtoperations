import type { CallStatusType, ClientDetail } from '../types';

interface StatusBadgeProps {
  status: string;
}

function StatusBadge({ status }: StatusBadgeProps) {
  return (
    <span className="px-2 py-0.5 rounded-full text-xs font-medium bg-gray-100 text-gray-700">
      {status}
    </span>
  );
}

interface CallStatusBadgeProps {
  status: CallStatusType;
  conversationId?: string;
}

function CallStatusBadge({ status, conversationId }: CallStatusBadgeProps) {
  if (conversationId) {
    return (
      <span
        className="px-2 py-0.5 rounded-full text-xs font-medium bg-green-100 text-green-800"
        title={conversationId}
      >
        Called ✓
      </span>
    );
  }
  if (status === 'idle') return null;
  const styles: Record<string, string> = {
    calling: 'bg-blue-100 text-blue-800',
    initiated: 'bg-green-100 text-green-800',
    failed: 'bg-red-100 text-red-800',
  };
  const labels: Record<string, string> = {
    calling: 'Calling…',
    initiated: 'Called',
    failed: 'Failed',
  };
  return (
    <span className={`px-2 py-0.5 rounded-full text-xs font-medium ${styles[status]}`}>
      {labels[status]}
    </span>
  );
}

interface ClientRowProps {
  client: ClientDetail;
  selected: boolean;
  callStatus: CallStatusType;
  conversationId?: string;
  onToggle: () => void;
}

export function ClientRow({ client, selected, callStatus, conversationId, onToggle }: ClientRowProps) {
  const alreadyCalled = !!conversationId;

  return (
    <tr
      className={`border-b border-gray-100 dark:border-gray-700 transition-colors ${
        alreadyCalled
          ? 'bg-gray-50 dark:bg-gray-700/30 opacity-60'
          : selected
          ? 'bg-blue-50 dark:bg-blue-900/20 hover:bg-blue-50 dark:hover:bg-blue-900/20'
          : 'hover:bg-gray-50 dark:hover:bg-gray-700/50'
      }`}
    >
      <td className="px-4 py-3">
        <input
          type="checkbox"
          checked={selected}
          onChange={onToggle}
          disabled={alreadyCalled}
          className="rounded border-gray-300 dark:border-gray-600 text-blue-600 focus:ring-blue-500 disabled:cursor-not-allowed disabled:opacity-40"
        />
      </td>
      <td className="px-4 py-3 text-sm font-medium text-gray-900 dark:text-gray-100">{client.client_id}</td>
      <td className="px-4 py-3 text-sm text-gray-700 dark:text-gray-300">{client.name}</td>
      <td className="px-4 py-3 text-sm">
        <StatusBadge status={client.status} />
      </td>
      <td className="px-4 py-3 text-sm text-gray-600 dark:text-gray-400">{client.region ?? '—'}</td>
      <td className="px-4 py-3 text-sm text-gray-600 dark:text-gray-400">{client.language ?? '—'}</td>
      <td className="px-4 py-3 text-sm text-gray-600 dark:text-gray-400">
        {client.sales_client_potential ?? '—'}
      </td>
      <td className="px-4 py-3 text-sm text-gray-600 dark:text-gray-400">
        {client.phone_number
          ? client.phone_number.slice(0, 5) + '*'.repeat(Math.max(0, client.phone_number.length - 5))
          : '—'}
      </td>
      <td className="px-4 py-3 text-sm text-gray-600 dark:text-gray-400">{client.email ?? '—'}</td>
      <td className="px-4 py-3 text-sm">
        <CallStatusBadge status={callStatus} conversationId={conversationId} />
      </td>
    </tr>
  );
}
