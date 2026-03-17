import { useEffect, useRef, useState } from 'react';
import axios from 'axios';
import { AgentSelect } from '../components/AgentSelect';

const api = axios.create({ baseURL: import.meta.env.VITE_API_BASE_URL || '/api' });
api.interceptors.request.use((config) => {
  const auth = JSON.parse(localStorage.getItem('auth') || '{}');
  const token = auth?.state?.token;
  if (token) config.headers.Authorization = `Bearer ${token}`;
  return config;
});

interface BatchClient {
  id: string;
  first_name: string;
  email: string;
  phone?: string;
  retention_rep?: string;
  retention_status_display?: string;
  error?: string;
}

interface Summary {
  total: number;
  ready: number;
  errors: number;
}

interface JobStatus {
  job_id: string;
  status: 'queued' | 'running' | 'completed' | 'failed' | 'cancelled' | 'interrupted';
  total_records: number;
  processed_records: number;
  failed_records: number;
  error_message?: string | null;
  started_at?: string | null;
  completed_at?: string | null;
  created_at?: string | null;
  concurrency?: number;
}

function parseCSV(text: string): { id: string }[] {
  const lines = text.trim().split(/\r?\n/);
  if (lines.length < 1) return [];
  const firstLine = lines[0].trim().toLowerCase().replace(/"/g, '');
  const dataLines = firstLine === 'id' ? lines.slice(1) : lines;
  return dataLines
    .map((line) => ({ id: line.split(',')[0].trim().replace(/"/g, '') }))
    .filter((r) => r.id);
}

const maskPhone = (phone?: string) => {
  if (!phone) return '—';
  return phone.slice(0, 5) + '*'.repeat(Math.max(0, phone.length - 5));
};

const BATCH_SIZE = 10;

function JobStatusBadge({ status }: { status: string }) {
  const cls: Record<string, string> = {
    queued: 'bg-yellow-100 text-yellow-800',
    running: 'bg-blue-100 text-blue-800',
    completed: 'bg-green-100 text-green-800',
    failed: 'bg-red-100 text-red-800',
    cancelled: 'bg-gray-100 text-gray-600',
    interrupted: 'bg-orange-100 text-orange-800',
  };
  return (
    <span className={`px-2 py-0.5 rounded-full text-xs font-medium ${cls[status] ?? 'bg-gray-100 text-gray-600'}`}>
      {status}
    </span>
  );
}

const CONCURRENCY_PRESETS = [1, 5, 10, 25, 50, 100];
const AVG_CALL_SECS = 180; // ~3 min average call duration for estimate

function fmtEstimate(records: number, concurrency: number): string {
  if (records <= 0 || concurrency <= 0) return '';
  const totalSecs = Math.ceil(records / concurrency) * AVG_CALL_SECS;
  const mins = Math.round(totalSecs / 60);
  if (mins < 1) return '<1m';
  if (mins < 60) return `~${mins}m`;
  const h = Math.floor(mins / 60);
  const m = mins % 60;
  return m > 0 ? `~${h}h ${m}m` : `~${h}h`;
}

export function BatchCallPage() {
  const [agentId, setAgentId] = useState('');
  const [agentPhoneNumberId, setAgentPhoneNumberId] = useState('');
  const [callProvider, setCallProvider] = useState<'twilio' | 'sip_trunk'>('twilio');
  const [concurrency, setConcurrency] = useState(10);
  const [clients, setClients] = useState<BatchClient[]>([]);
  const [summary, setSummary] = useState<Summary | null>(null);
  const [loading, setLoading] = useState(false);
  const [progress, setProgress] = useState<{ current: number; total: number } | null>(null);
  const [uploadError, setUploadError] = useState('');
  const [jobStatus, setJobStatus] = useState<JobStatus | null>(null);
  const [history, setHistory] = useState<JobStatus[]>([]);
  const [submitting, setSubmitting] = useState(false);
  const fileRef = useRef<HTMLInputElement>(null);
  const cancelRef = useRef(false);
  const pollRef = useRef<ReturnType<typeof setInterval> | null>(null);

  const setConcurrencyValue = (v: number) => {
    setConcurrency(Math.max(1, Math.min(100, Math.round(v) || 1)));
  };

  const fetchHistory = async () => {
    try {
      const res = await api.get('/batch-calls/history');
      setHistory(res.data);
      return res.data as JobStatus[];
    } catch {
      return [];
    }
  };

  const startPolling = (jid: string) => {
    if (pollRef.current) clearInterval(pollRef.current);
    pollRef.current = setInterval(async () => {
      try {
        const res = await api.get(`/batch-calls/${jid}/status`);
        setJobStatus(res.data);
        if (['completed', 'failed', 'cancelled', 'interrupted'].includes(res.data.status)) {
          clearInterval(pollRef.current!);
          pollRef.current = null;
          fetchHistory();
        }
      } catch {}
    }, 3000);
  };

  useEffect(() => {
    fetchHistory().then((jobs) => {
      const running = jobs.find((j) => ['queued', 'running'].includes(j.status));
      if (running) {
        setJobStatus(running);
        startPolling(running.job_id);
      }
    });
    return () => {
      if (pollRef.current) clearInterval(pollRef.current);
    };
  }, []);

  const stopUpload = () => { cancelRef.current = true; };

  const handleFile = async (e: React.ChangeEvent<HTMLInputElement>) => {
    const file = e.target.files?.[0];
    if (!file) return;
    setUploadError('');
    setClients([]);
    setSummary(null);
    setProgress(null);
    cancelRef.current = false;

    const text = await file.text();
    const parsed = parseCSV(text);

    if (parsed.length === 0) {
      setUploadError('No valid IDs found. Make sure the CSV has an id column.');
      return;
    }

    setLoading(true);
    setProgress({ current: 0, total: parsed.length });

    const allEnriched: BatchClient[] = [];
    try {
      for (let i = 0; i < parsed.length; i += BATCH_SIZE) {
        if (cancelRef.current) break;
        const chunk = parsed.slice(i, i + BATCH_SIZE);
        try {
          const res = await api.post('/clients/lookup', { clients: chunk });
          const enriched: BatchClient[] = res.data.map((r: any) => ({
            id: r.id,
            first_name: r.first_name ?? '',
            email: r.email ?? '',
            phone: r.phone ?? undefined,
            retention_rep: r.retention_rep ?? undefined,
            retention_status_display: r.retention_status_display ?? undefined,
            error: r.error ?? undefined,
          }));
          allEnriched.push(...enriched);
        } catch {
          // Mark this chunk's clients as errored but continue processing
          chunk.forEach((c) => allEnriched.push({ id: c.id, first_name: '', email: '', error: 'CRM lookup failed' }));
        }
        setClients([...allEnriched]);
        setProgress({ current: Math.min(i + BATCH_SIZE, parsed.length), total: parsed.length });
      }
      if (allEnriched.length > 0) {
        setSummary({
          total: allEnriched.length,
          ready: allEnriched.filter((c) => c.phone).length,
          errors: allEnriched.filter((c) => !c.phone).length,
        });
      }
    } catch {
      setUploadError('Failed to look up clients from CRM');
    } finally {
      setLoading(false);
      setProgress(null);
      if (fileRef.current) fileRef.current.value = '';
    }
  };

  const callAll = async () => {
    const readyClients = clients.filter((c) => c.phone);
    if (readyClients.length === 0) return;

    setSubmitting(true);
    setUploadError('');
    try {
      const res = await api.post('/batch-calls/start', {
        clients: readyClients.map((c) => ({
          id: c.id,
          first_name: c.first_name,
          email: c.email,
          phone: c.phone,
          retention_rep: c.retention_rep ?? '',
          retention_status_display: c.retention_status_display ?? '',
        })),
        agent_id: agentId || undefined,
        agent_phone_number_id: agentPhoneNumberId || undefined,
        call_provider: callProvider,
        concurrency,
      });
      const newJob: JobStatus = {
        job_id: res.data.job_id,
        status: 'queued',
        total_records: readyClients.length,
        processed_records: 0,
        failed_records: 0,
      };
      setJobStatus(newJob);
      setHistory((prev) => [newJob, ...prev]);
      startPolling(res.data.job_id);
    } catch (err: any) {
      setUploadError(err?.response?.data?.detail || 'Failed to start batch job');
    } finally {
      setSubmitting(false);
    }
  };

  const cancelJob = async () => {
    if (!jobStatus) return;
    try {
      await api.post(`/batch-calls/${jobStatus.job_id}/cancel`);
    } catch {}
  };

  const isRunning = jobStatus && ['queued', 'running'].includes(jobStatus.status);
  const readyCount = clients.filter((c) => c.phone).length;

  return (
    <div className="space-y-5">
      {/* Settings */}
      <div className="bg-white dark:bg-gray-800 rounded-lg shadow dark:shadow-gray-900 p-5 space-y-4">
        <h2 className="text-sm font-semibold text-gray-700 dark:text-gray-300">Agent Settings</h2>
        <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
          <div>
            <label className="block text-xs font-medium text-gray-600 dark:text-gray-400 mb-1">Agent</label>
            <AgentSelect value={agentId} onChange={setAgentId} />
          </div>
          <div>
            <label className="block text-xs font-medium text-gray-600 dark:text-gray-400 mb-1">Agent Phone Number ID</label>
            <input
              type="text"
              placeholder="phnum_..."
              value={agentPhoneNumberId}
              onChange={(e) => setAgentPhoneNumberId(e.target.value)}
              className="w-full border border-gray-300 dark:border-gray-600 rounded-md px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-blue-500 bg-white dark:bg-gray-800 text-gray-900 dark:text-gray-100 placeholder-gray-400 dark:placeholder-gray-500"
            />
          </div>
          <div>
            <label className="block text-xs font-medium text-gray-600 dark:text-gray-400 mb-1">Provider</label>
            <select
              value={callProvider}
              onChange={(e) => setCallProvider(e.target.value as 'twilio' | 'sip_trunk')}
              className="w-full border border-gray-300 dark:border-gray-600 rounded-md px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-blue-500 bg-white dark:bg-gray-800 text-gray-900 dark:text-gray-100"
            >
              <option value="twilio">Twilio</option>
              <option value="sip_trunk">SIP Trunk</option>
            </select>
          </div>
        </div>

        {/* Simultaneous Calls control */}
        <div className="border border-gray-200 dark:border-gray-700 rounded-lg p-4 bg-gray-50 dark:bg-gray-900">
          <label className="block text-xs font-bold text-gray-400 uppercase tracking-wider mb-3">
            Simultaneous Calls
          </label>
          <div className="flex items-center gap-3 mb-3">
            <button
              type="button"
              onClick={() => setConcurrencyValue(concurrency - 1)}
              style={{ width: 32, height: 32 }}
              className="flex items-center justify-center border border-gray-200 dark:border-gray-600 rounded-md bg-white dark:bg-gray-800 text-gray-600 dark:text-gray-400 hover:bg-gray-100 dark:hover:bg-gray-700 font-bold text-base select-none"
            >−</button>
            <input
              type="number"
              min={1}
              max={100}
              value={concurrency}
              onChange={(e) => setConcurrencyValue(Number(e.target.value))}
              style={{ width: 64, height: 32, fontFamily: 'monospace', textAlign: 'center' }}
              className="border border-gray-200 dark:border-gray-600 rounded-md text-sm bg-white dark:bg-gray-800 text-gray-900 dark:text-gray-100 focus:outline-none focus:ring-2 focus:ring-teal-500"
            />
            <button
              type="button"
              onClick={() => setConcurrencyValue(concurrency + 1)}
              style={{ width: 32, height: 32 }}
              className="flex items-center justify-center border border-gray-200 dark:border-gray-600 rounded-md bg-white dark:bg-gray-800 text-gray-600 dark:text-gray-400 hover:bg-gray-100 dark:hover:bg-gray-700 font-bold text-base select-none"
            >+</button>
            <input
              type="range"
              min={1}
              max={100}
              value={concurrency}
              onChange={(e) => setConcurrencyValue(Number(e.target.value))}
              className="flex-1 accent-teal-600"
            />
          </div>
          <div className="flex items-center gap-2 mb-3 flex-wrap">
            {CONCURRENCY_PRESETS.map((p) => (
              <button
                key={p}
                type="button"
                onClick={() => setConcurrencyValue(p)}
                className="px-3 py-1 text-xs font-medium rounded-full border select-none"
                style={{
                  borderColor: concurrency === p ? '#0d9488' : '#e2e8f0',
                  background: concurrency === p ? '#f0fdfa' : '#fff',
                  color: concurrency === p ? '#0d9488' : '#64748b',
                  borderWidth: concurrency === p ? '1.5px' : '1px',
                }}
              >{p}</button>
            ))}
          </div>
          {readyCount > 0 && (
            <p className="text-xs text-gray-500 dark:text-gray-400">
              <span className="text-teal-600">⚡</span>{' '}
              {concurrency} call{concurrency !== 1 ? 's' : ''} at once · Est. {fmtEstimate(readyCount, concurrency)} for {readyCount} records
            </p>
          )}
        </div>

        <div>
          <label className="block text-xs font-medium text-gray-600 dark:text-gray-400 mb-1">
            Upload CSV <span className="text-gray-400 dark:text-gray-500 font-normal">(one ID per row — first name, email and phone fetched from CRM)</span>
          </label>
          <input
            ref={fileRef}
            type="file"
            accept=".csv"
            onChange={handleFile}
            disabled={loading}
            className="block w-full text-sm text-gray-600 dark:text-gray-400 file:mr-3 file:py-1.5 file:px-4 file:rounded-md file:border-0 file:text-sm file:font-medium file:bg-blue-50 file:text-blue-700 hover:file:bg-blue-100 disabled:opacity-50"
          />
          {uploadError && <p className="mt-1 text-xs text-red-600">{uploadError}</p>}
        </div>
      </div>

      {/* Summary */}
      {summary && (
        <div className="grid grid-cols-3 gap-4">
          <div className="bg-white dark:bg-gray-800 rounded-lg shadow dark:shadow-gray-900 p-4 text-center">
            <p className="text-2xl font-bold text-gray-800 dark:text-gray-100">{summary.total}</p>
            <p className="text-xs text-gray-500 dark:text-gray-400 mt-1">Total Imported</p>
          </div>
          <div className="bg-white dark:bg-gray-800 rounded-lg shadow dark:shadow-gray-900 p-4 text-center">
            <p className="text-2xl font-bold text-green-600">{summary.ready}</p>
            <p className="text-xs text-gray-500 dark:text-gray-400 mt-1">Ready to Call</p>
          </div>
          <div className="bg-white dark:bg-gray-800 rounded-lg shadow dark:shadow-gray-900 p-4 text-center">
            <p className="text-2xl font-bold text-red-500">{summary.errors}</p>
            <p className="text-xs text-gray-500 dark:text-gray-400 mt-1">Errors (No Phone)</p>
          </div>
        </div>
      )}

      {/* Active Job Progress */}
      {jobStatus && (
        <div className="bg-white dark:bg-gray-800 rounded-lg shadow dark:shadow-gray-900 p-5">
          <div className="flex items-center justify-between mb-3">
            <div className="flex items-center gap-3">
              <span className="text-sm font-semibold text-gray-700 dark:text-gray-300">Batch Job</span>
              <JobStatusBadge status={jobStatus.status} />
              {isRunning && (
                <svg className="animate-spin h-4 w-4 text-blue-500" xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24">
                  <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4" />
                  <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4z" />
                </svg>
              )}
            </div>
            {isRunning && (
              <button
                onClick={cancelJob}
                className="px-3 py-1.5 bg-red-100 text-red-700 rounded-md text-xs font-medium hover:bg-red-200 transition-colors"
              >
                Cancel
              </button>
            )}
          </div>
          <div className="flex items-center justify-between text-xs text-gray-500 dark:text-gray-400 mb-1.5">
            <span>
              {isRunning && jobStatus.concurrency && jobStatus.concurrency > 1 && (
                <span className="text-teal-600 mr-2">{jobStatus.concurrency} simultaneous ·</span>
              )}
              {jobStatus.processed_records} / {jobStatus.total_records} processed
              {jobStatus.failed_records > 0 && (
                <span className="text-red-500 ml-2">· {jobStatus.failed_records} failed</span>
              )}
            </span>
            <span>
              {jobStatus.total_records > 0
                ? Math.round((jobStatus.processed_records / jobStatus.total_records) * 100)
                : 0}%
            </span>
          </div>
          <div className="w-full bg-gray-200 dark:bg-gray-700 rounded-full h-2">
            <div
              className={`h-2 rounded-full transition-all duration-500 ${
                jobStatus.status === 'completed' ? 'bg-green-500' :
                jobStatus.status === 'failed' ? 'bg-red-500' :
                jobStatus.status === 'cancelled' ? 'bg-gray-400' : 'bg-blue-500'
              }`}
              style={{
                width: `${jobStatus.total_records > 0
                  ? (jobStatus.processed_records / jobStatus.total_records) * 100
                  : 0}%`,
              }}
            />
          </div>
          {jobStatus.status === 'completed' && (
            <p className="mt-2 text-xs text-green-600 font-medium">
              Job completed — {jobStatus.processed_records - jobStatus.failed_records} calls initiated
              {jobStatus.failed_records > 0 && `, ${jobStatus.failed_records} failed`}.
            </p>
          )}
          {isRunning && (
            <p className="mt-2 text-xs text-gray-500 dark:text-gray-400">
              This job continues running even if you log out or close the browser.
            </p>
          )}
        </div>
      )}

      {/* Client Table */}
      {(clients.length > 0 || loading) && (
        <div className="bg-white dark:bg-gray-800 rounded-lg shadow dark:shadow-gray-900 overflow-hidden">
          <div className="px-4 py-3 border-b border-gray-200 dark:border-gray-700 bg-gray-50 dark:bg-gray-900 flex items-center justify-between flex-wrap gap-2">
            <span className="text-sm text-gray-600 dark:text-gray-400">
              {loading && progress
                ? `${progress.current} of ${progress.total} clients loaded…`
                : `${clients.length} clients loaded`}
            </span>
            <div className="flex items-center gap-2">
              {readyCount > 0 && !isRunning && (
                <button
                  onClick={callAll}
                  disabled={submitting || loading}
                  className="flex items-center gap-2 px-4 py-2 bg-green-600 text-white rounded-md text-sm font-medium hover:bg-green-700 disabled:opacity-70 transition-colors"
                >
                  {submitting && (
                    <svg className="animate-spin h-4 w-4 text-white" xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24">
                      <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4" />
                      <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4z" />
                    </svg>
                  )}
                  {submitting ? 'Submitting…' : `Call All (${readyCount})`}
                </button>
              )}
            </div>
          </div>

          {loading && progress && (
            <div className="px-4 py-3 border-b border-gray-100 dark:border-gray-700 bg-blue-50 dark:bg-gray-800">
              <div className="flex items-center justify-between mb-1">
                <span className="text-xs text-blue-700 font-medium">
                  Looking up clients in CRM… {progress.current} / {progress.total}
                </span>
                <div className="flex items-center gap-3">
                  <span className="text-xs text-blue-600">
                    {Math.round((progress.current / progress.total) * 100)}%
                  </span>
                  <button
                    onClick={stopUpload}
                    className="px-2 py-0.5 text-xs font-medium bg-red-100 text-red-700 rounded hover:bg-red-200 transition-colors"
                  >
                    Stop
                  </button>
                </div>
              </div>
              <div className="w-full bg-blue-200 rounded-full h-1.5">
                <div
                  className="bg-blue-600 h-1.5 rounded-full transition-all duration-300"
                  style={{ width: `${(progress.current / progress.total) * 100}%` }}
                />
              </div>
            </div>
          )}

          {clients.length > 0 && (
            <div className="overflow-x-auto overflow-y-auto max-h-96">
              <table className="w-full">
                <thead className="bg-gray-50 dark:bg-gray-900 sticky top-0 z-10">
                  <tr>
                    {['ID', 'First Name', 'Email', 'Phone', 'Status'].map((h) => (
                      <th key={h} className="px-4 py-3 text-left text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider">
                        {h}
                      </th>
                    ))}
                  </tr>
                </thead>
                <tbody>
                  {clients.map((c) => (
                    <tr key={c.id} className={`border-b border-gray-100 dark:border-gray-700 ${!c.phone ? 'bg-red-50 dark:bg-gray-800 opacity-60' : 'hover:bg-gray-50 dark:hover:bg-gray-700'}`}>
                      <td className="px-4 py-3 text-sm font-medium text-gray-900 dark:text-gray-100">{c.id}</td>
                      <td className="px-4 py-3 text-sm text-gray-700 dark:text-gray-300">{c.first_name || '—'}</td>
                      <td className="px-4 py-3 text-sm text-gray-600 dark:text-gray-400">{c.email || '—'}</td>
                      <td className="px-4 py-3 text-sm text-gray-600 dark:text-gray-400">
                        {c.error ? (
                          <span className="text-red-500 text-xs">{c.error}</span>
                        ) : (
                          maskPhone(c.phone)
                        )}
                      </td>
                      <td className="px-4 py-3 text-sm">
                        {c.phone ? (
                          <span className="px-2 py-0.5 rounded-full text-xs font-medium bg-gray-100 dark:bg-gray-700 text-gray-600 dark:text-gray-400">Ready</span>
                        ) : (
                          <span className="px-2 py-0.5 rounded-full text-xs font-medium bg-red-100 text-red-700">No Phone</span>
                        )}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
        </div>
      )}

      {/* Job History */}
      {history.length > 0 && (
        <div className="bg-white dark:bg-gray-800 rounded-lg shadow dark:shadow-gray-900 overflow-hidden">
          <div className="px-4 py-3 border-b border-gray-200 dark:border-gray-700 bg-gray-50 dark:bg-gray-900">
            <span className="text-sm font-semibold text-gray-700 dark:text-gray-300">Recent Batch Jobs</span>
          </div>
          <div className="overflow-x-auto">
            <table className="w-full">
              <thead className="bg-gray-50 dark:bg-gray-900">
                <tr>
                  {['Job ID', 'Status', 'Progress', 'Failed', 'Started', 'Completed'].map((h) => (
                    <th key={h} className="px-4 py-3 text-left text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider">
                      {h}
                    </th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {history.map((job) => (
                  <tr
                    key={job.job_id}
                    className="border-b border-gray-100 dark:border-gray-700 hover:bg-gray-50 dark:hover:bg-gray-700 cursor-pointer"
                    onClick={() => setJobStatus(job)}
                  >
                    <td className="px-4 py-3 text-xs font-mono text-gray-500 dark:text-gray-400">{job.job_id.slice(0, 12)}…</td>
                    <td className="px-4 py-3"><JobStatusBadge status={job.status} /></td>
                    <td className="px-4 py-3 text-sm text-gray-700 dark:text-gray-300">{job.processed_records} / {job.total_records}</td>
                    <td className="px-4 py-3 text-sm">{job.failed_records > 0 ? <span className="text-red-500">{job.failed_records}</span> : <span className="text-gray-400">—</span>}</td>
                    <td className="px-4 py-3 text-xs text-gray-500 dark:text-gray-400">
                      {job.started_at ? new Date(job.started_at).toLocaleString() : '—'}
                    </td>
                    <td className="px-4 py-3 text-xs text-gray-500 dark:text-gray-400">
                      {job.completed_at ? new Date(job.completed_at).toLocaleString() : '—'}
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
