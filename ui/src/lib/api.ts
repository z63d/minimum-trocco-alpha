export type JobDefinition = {
  id: number;
  name: string;
  dummy_duration_sec: number;
  failure_rate: number;
  created_at: string;
  updated_at: string;
};

export type Job = {
  id: number;
  job_definition_id: number;
  job_name: string;
  status: "queued" | "pending" | "running" | "succeeded" | "failed";
  k8s_job_name: string | null;
  started_at: string | null;
  finished_at: string | null;
  created_at: string;
  updated_at: string;
};

export const fetcher = async (url: string) => {
  const res = await fetch(url);
  if (!res.ok) throw new Error(`${res.status}: ${await res.text()}`);
  return res.json();
};

export async function runJobDefinition(id: number): Promise<{ job_id: number; status: string }> {
  const res = await fetch(`/api/job-definitions/${id}/run`, { method: "POST" });
  if (!res.ok) throw new Error(`${res.status}: ${await res.text()}`);
  return res.json();
}
