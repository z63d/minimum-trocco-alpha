import { useState } from "react";
import useSWR from "swr";
import { fetcher, runJobDefinition, type Job, type JobDefinition } from "../lib/api";

const STATUS_CLASS: Record<Job["status"], string> = {
  queued: "bg-slate-200 text-slate-700",
  pending: "bg-amber-200 text-amber-900",
  running: "bg-blue-200 text-blue-900",
  succeeded: "bg-emerald-200 text-emerald-900",
  failed: "bg-rose-200 text-rose-900",
};

export function JobDefinitionsPage() {
  const defs = useSWR<JobDefinition[]>("/api/job-definitions", fetcher, { refreshInterval: 10000 });
  const jobs = useSWR<Job[]>("/api/jobs", fetcher, { refreshInterval: 3000 });
  const [running, setRunning] = useState<Set<number>>(new Set());

  const handleRun = async (id: number) => {
    setRunning((prev) => new Set(prev).add(id));
    try {
      await runJobDefinition(id);
      await jobs.mutate();
    } finally {
      setRunning((prev) => {
        const next = new Set(prev);
        next.delete(id);
        return next;
      });
    }
  };

  return (
    <div className="space-y-6">
      <section className="rounded-lg border border-slate-200 bg-white">
        <div className="border-b border-slate-200 px-4 py-3 font-semibold">Job Definitions</div>
        <ul className="divide-y divide-slate-200">
          {defs.data?.map((d) => (
            <li key={d.id} className="flex items-center justify-between px-4 py-3">
              <div>
                <div className="font-medium">{d.name}</div>
                <div className="text-sm text-slate-500">
                  duration {d.dummy_duration_sec}s · failure rate {(d.failure_rate * 100).toFixed(0)}%
                </div>
              </div>
              <button
                type="button"
                disabled={running.has(d.id)}
                onClick={() => handleRun(d.id)}
                className="rounded-md bg-blue-600 px-3 py-1.5 text-sm font-medium text-white shadow-sm enabled:hover:bg-blue-700 disabled:opacity-50"
              >
                {running.has(d.id) ? "..." : "▶ Run"}
              </button>
            </li>
          ))}
          {defs.data?.length === 0 && <li className="px-4 py-6 text-sm text-slate-500">No job definitions.</li>}
          {defs.error && <li className="px-4 py-6 text-sm text-rose-600">Failed to load: {String(defs.error)}</li>}
        </ul>
      </section>

      <section className="rounded-lg border border-slate-200 bg-white">
        <div className="flex items-center justify-between border-b border-slate-200 px-4 py-3">
          <span className="font-semibold">Jobs (auto-refresh 3s)</span>
          {jobs.isValidating && <span className="text-xs text-slate-500">refreshing…</span>}
        </div>
        <table className="w-full text-sm">
          <thead className="bg-slate-50 text-left text-xs uppercase tracking-wide text-slate-500">
            <tr>
              <th className="px-4 py-2">ID</th>
              <th className="px-4 py-2">Name</th>
              <th className="px-4 py-2">Status</th>
              <th className="px-4 py-2">Started</th>
              <th className="px-4 py-2">Finished</th>
              <th className="px-4 py-2">k8s Job</th>
            </tr>
          </thead>
          <tbody className="divide-y divide-slate-200">
            {jobs.data?.map((j) => (
              <tr key={j.id}>
                <td className="px-4 py-2 font-mono">{j.id}</td>
                <td className="px-4 py-2">{j.job_name}</td>
                <td className="px-4 py-2">
                  <span className={`rounded px-2 py-0.5 text-xs font-semibold ${STATUS_CLASS[j.status]}`}>
                    {j.status}
                  </span>
                </td>
                <td className="px-4 py-2 text-slate-600">{formatTime(j.started_at)}</td>
                <td className="px-4 py-2 text-slate-600">{formatTime(j.finished_at)}</td>
                <td className="px-4 py-2 font-mono text-xs text-slate-500">{j.k8s_job_name ?? "-"}</td>
              </tr>
            ))}
            {jobs.data?.length === 0 && (
              <tr>
                <td colSpan={6} className="px-4 py-6 text-center text-slate-500">
                  No jobs yet.
                </td>
              </tr>
            )}
          </tbody>
        </table>
      </section>
    </div>
  );
}

function formatTime(iso: string | null): string {
  if (!iso) return "-";
  const d = new Date(iso);
  return d.toLocaleTimeString();
}
