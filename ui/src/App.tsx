import { JobDefinitionsPage } from "./pages/JobDefinitionsPage";

export function App() {
  return (
    <div className="min-h-screen bg-slate-50 text-slate-900">
      <header className="border-b border-slate-200 bg-white">
        <div className="mx-auto max-w-5xl px-6 py-4">
          <h1 className="text-xl font-semibold">minimum-trocco-alpha</h1>
        </div>
      </header>
      <main className="mx-auto max-w-5xl px-6 py-6">
        <JobDefinitionsPage />
      </main>
    </div>
  );
}
