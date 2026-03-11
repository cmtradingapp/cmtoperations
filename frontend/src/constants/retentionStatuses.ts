/**
 * Approved retention statuses (CLAUD-65).
 *
 * This is the single source of truth for the retention status options
 * shown in all dropdowns, filters, and selectors across the app.
 *
 * The `key` values correspond to the CRM (vtiger) retentionStatus codes.
 */

export const APPROVED_RETENTION_STATUSES: readonly { key: number; label: string }[] = [
  { key: 20, label: 'Appointment' },
  { key: 23, label: 'Call Again' },
  { key: 36, label: 'Daily Trading with me' },
  { key: 0,  label: 'New' },
  { key: 3,  label: 'No Answer' },
  { key: 19, label: 'Potential' },
  { key: 28, label: 'Reassigned' },
  { key: 35, label: 'Remove From my Portfolio' },
  { key: 34, label: 'Terminated/Complain/Legal' },
] as const;

/** Just the labels, for use in multiselect filters */
export const APPROVED_RETENTION_STATUS_LABELS: readonly string[] =
  APPROVED_RETENTION_STATUSES.map((s) => s.label);

/** Set of valid CRM status keys for quick lookups */
export const APPROVED_STATUS_KEYS: ReadonlySet<number> =
  new Set(APPROVED_RETENTION_STATUSES.map((s) => s.key));
