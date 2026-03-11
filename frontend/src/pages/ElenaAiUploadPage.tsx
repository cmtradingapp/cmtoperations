import { useRef, useState } from 'react';
import axios from 'axios';

const api = axios.create({ baseURL: import.meta.env.VITE_API_BASE_URL || '/api' });
api.interceptors.request.use((config) => {
  const auth = JSON.parse(localStorage.getItem('auth') || '{}');
  const token = auth?.state?.token;
  if (token) config.headers.Authorization = `Bearer ${token}`;
  return config;
});

type RowStatus = 'pending' | 'sending' | 'success' | 'error';

interface CsvRow {
  accountid: string;
  campaign_id: string;
  status: RowStatus;
  error?: string;
}

interface ApiResult {
  accountid: string;
  campaign_id: string;
  status: 'success' | 'error';
  error?: string;
}

function parseCSV(text: string): { accountid: string; campaign_id: string }[] {
  const lines = text.trim().split(/\r?\n/);
  if (lines.length < 2) return []; // need header + at least one data row
  // Skip first row (header)
  return lines
    .slice(1)
    .map((line) => {
      const cols = line.split(',').map((c) => c.trim().replace(/"/g, ''));
      return { accountid: cols[0] || '', campaign_id: cols[1] || '' };
    })
    .filter((r) => r.accountid && r.campaign_id);
}

export function ElenaAiUploadPage() {
  const [rows, setRows] = useState<CsvRow[]>([]);
  const [sending, setSending] = useState(false);
  const [uploadError, setUploadError] = useState('');
  const fileRef = useRef<HTMLInputElement>(null);

  const totalCount = rows.length;
  const sentCount = rows.filter((r) => r.status === 'success').length;
  const failedCount = rows.filter((r) => r.status === 'error').length;

  const handleFile = async (e: React.ChangeEvent<HTMLInputElement>) => {
    const file = e.target.files?.[0];
    if (!file) return;
    setUploadError('');
    setRows([]);

    const text = await file.text();
    const parsed = parseCSV(text);

    if (parsed.length === 0) {
      setUploadError(
        'No valid rows found. CSV must have a header row and columns: Account ID, Campaign ID.',
      );
      return;
    }

    setRows(parsed.map((r) => ({ ...r, status: 'pending' })));
    if (fileRef.current) fileRef.current.value = '';
  };

  const BATCH_SIZE = 50;

  const sendToCampaign = async () => {
    const pending = rows.filter((r) => r.status === 'pending' || r.status === 'error');
    if (pending.length === 0) return;

    setSending(true);

    // Process in batches of BATCH_SIZE so we see live progress
    for (let i = 0; i < pending.length; i += BATCH_SIZE) {
      const batch = pending.slice(i, i + BATCH_SIZE);

      // Mark this batch as sending
      const batchKeys = new Set(batch.map((r) => `${r.accountid}__${r.campaign_id}`));
      setRows((prev) =>
        prev.map((r) =>
          batchKeys.has(`${r.accountid}__${r.campaign_id}`) && (r.status === 'pending' || r.status === 'error')
            ? { ...r, status: 'sending', error: undefined }
            : r,
        ),
      );

      try {
        const payload = batch.map((r) => ({ accountid: r.accountid, campaign_id: r.campaign_id }));
        const res = await api.post<ApiResult[]>('/elena-ai/campaign-upload', payload);

        const resultMap = new Map<string, ApiResult>();
        for (const result of res.data) {
          resultMap.set(`${result.accountid}__${result.campaign_id}`, result);
        }

        setRows((prev) =>
          prev.map((r) => {
            const key = `${r.accountid}__${r.campaign_id}`;
            const result = resultMap.get(key);
            if (result) {
              return {
                ...r,
                status: result.status === 'success' ? 'success' : 'error',
                error: result.error || undefined,
              };
            }
            return r;
          }),
        );
      } catch (err: any) {
        // Mark this batch as failed, but continue with next batch
        const msg = err?.response?.data?.detail || 'Request failed';
        setRows((prev) =>
          prev.map((r) =>
            batchKeys.has(`${r.accountid}__${r.campaign_id}`) && r.status === 'sending'
              ? { ...r, status: 'error', error: msg }
              : r,
          ),
        );
      }
    }

    setSending(false);
  };

  const hasPending = rows.some((r) => r.status === 'pending' || r.status === 'error');

  return (
    <div className="space-y-5">
      {/* Upload section */}
      <div className="bg-white dark:bg-gray-800 rounded-lg shadow dark:shadow-gray-900 p-5 space-y-4">
        <h2 className="text-sm font-semibold text-gray-700 dark:text-gray-300">Upload Clients to Campaign</h2>
        <p className="text-xs text-gray-500 dark:text-gray-400">
          Upload a CSV file with columns: <strong>Account ID</strong>, <strong>Campaign ID</strong>.
          The first row (header) is skipped.
        </p>

        <div>
          <label className="block text-xs font-medium text-gray-600 dark:text-gray-400 mb-1">
            Upload CSV{' '}
            <span className="text-gray-400 dark:text-gray-500 font-normal">
              (col 0 = Account ID, col 1 = Campaign ID)
            </span>
          </label>
          <input
            ref={fileRef}
            type="file"
            accept=".csv"
            onChange={handleFile}
            disabled={sending}
            className="block w-full text-sm text-gray-600 dark:text-gray-400 file:mr-3 file:py-1.5 file:px-4 file:rounded-md file:border-0 file:text-sm file:font-medium file:bg-blue-50 file:text-blue-700 hover:file:bg-blue-100 disabled:opacity-50"
          />
          {uploadError && <p className="mt-1 text-xs text-red-600">{uploadError}</p>}
        </div>
      </div>

      {/* Summary counts */}
      {rows.length > 0 && (
        <div className="grid grid-cols-3 gap-4">
          <div className="bg-white dark:bg-gray-800 rounded-lg shadow dark:shadow-gray-900 p-4 text-center">
            <p className="text-2xl font-bold text-gray-800 dark:text-gray-100">{totalCount}</p>
            <p className="text-xs text-gray-500 dark:text-gray-400 mt-1">Total Rows</p>
          </div>
          <div className="bg-white dark:bg-gray-800 rounded-lg shadow dark:shadow-gray-900 p-4 text-center">
            <p className="text-2xl font-bold text-green-600">{sentCount}</p>
            <p className="text-xs text-gray-500 dark:text-gray-400 mt-1">Sent</p>
          </div>
          <div className="bg-white dark:bg-gray-800 rounded-lg shadow dark:shadow-gray-900 p-4 text-center">
            <p className="text-2xl font-bold text-red-500">{failedCount}</p>
            <p className="text-xs text-gray-500 dark:text-gray-400 mt-1">Failed</p>
          </div>
        </div>
      )}

      {/* Table */}
      {rows.length > 0 && (
        <div className="bg-white dark:bg-gray-800 rounded-lg shadow dark:shadow-gray-900 overflow-hidden">
          <div className="px-4 py-3 border-b border-gray-200 dark:border-gray-700 bg-gray-50 dark:bg-gray-900 flex items-center justify-between flex-wrap gap-2">
            <span className="text-sm text-gray-600 dark:text-gray-400">{rows.length} rows loaded</span>
            <div className="flex items-center gap-2">
              {hasPending && (
                <button
                  onClick={sendToCampaign}
                  disabled={sending}
                  className="flex items-center gap-2 px-4 py-2 bg-blue-600 text-white rounded-md text-sm font-medium hover:bg-blue-700 disabled:opacity-70 transition-colors"
                >
                  {sending && (
                    <svg
                      className="animate-spin h-4 w-4 text-white"
                      xmlns="http://www.w3.org/2000/svg"
                      fill="none"
                      viewBox="0 0 24 24"
                    >
                      <circle
                        className="opacity-25"
                        cx="12"
                        cy="12"
                        r="10"
                        stroke="currentColor"
                        strokeWidth="4"
                      />
                      <path
                        className="opacity-75"
                        fill="currentColor"
                        d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4z"
                      />
                    </svg>
                  )}
                  {sending ? 'Sending...' : 'Send to Campaign'}
                </button>
              )}
            </div>
          </div>

          <div className="overflow-x-auto overflow-y-auto max-h-96">
            <table className="w-full">
              <thead className="bg-gray-50 dark:bg-gray-900 sticky top-0 z-10">
                <tr>
                  {['Account ID', 'Campaign ID', 'Status'].map((h) => (
                    <th
                      key={h}
                      className="px-4 py-3 text-left text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider"
                    >
                      {h}
                    </th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {rows.map((r, idx) => (
                  <tr
                    key={`${r.accountid}-${r.campaign_id}-${idx}`}
                    className="border-b border-gray-100 dark:border-gray-700 hover:bg-gray-50 dark:hover:bg-gray-700"
                  >
                    <td className="px-4 py-3 text-sm font-medium text-gray-900 dark:text-gray-100">
                      {r.accountid}
                    </td>
                    <td className="px-4 py-3 text-sm text-gray-700 dark:text-gray-300">{r.campaign_id}</td>
                    <td className="px-4 py-3 text-sm">
                      {r.status === 'pending' && (
                        <span className="px-2 py-0.5 rounded-full text-xs font-medium bg-gray-100 dark:bg-gray-700 text-gray-600 dark:text-gray-400">
                          Pending
                        </span>
                      )}
                      {r.status === 'sending' && (
                        <span className="px-2 py-0.5 rounded-full text-xs font-medium bg-blue-100 text-blue-800 animate-pulse">
                          Sending...
                        </span>
                      )}
                      {r.status === 'success' && (
                        <span className="px-2 py-0.5 rounded-full text-xs font-medium bg-green-100 text-green-800">
                          Sent
                        </span>
                      )}
                      {r.status === 'error' && (
                        <span className="text-xs text-red-700">
                          <span className="px-2 py-0.5 rounded-full font-medium bg-red-100 text-red-800">
                            Failed
                          </span>
                          {r.error && (
                            <span className="ml-1 text-red-500">-- {r.error}</span>
                          )}
                        </span>
                      )}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      )}
    </div>
  );
}
