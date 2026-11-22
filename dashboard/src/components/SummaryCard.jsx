import { useCallback, useEffect, useState } from 'react';
import { loadSituationSummary } from '../lib/api.js';

const REFRESH_MS = 60_000;
const PROVIDER_LABEL = { anthropic: 'Claude', openrouter: 'OpenRouter', gemini: 'Gemini', template: 'Templat' };

function ageLabel(seconds) {
  if (seconds < 60) return 'baru saja';
  const minutes = Math.round(seconds / 60);
  return minutes < 60 ? `${minutes} menit lalu` : `${Math.round(minutes / 60)} jam lalu`;
}

// Fetches on its own timer so a slow or dead summarizer never delays the rest of the dashboard.
export function SummaryCard({ accessToken }) {
  const [summary, setSummary] = useState(undefined);

  const load = useCallback(async () => {
    setSummary(await loadSituationSummary(accessToken));
  }, [accessToken]);

  useEffect(() => {
    load();
    const id = setInterval(load, REFRESH_MS);
    return () => clearInterval(id);
  }, [load]);

  return (
    <section className="card summary-card" aria-labelledby="summary-title">
      <div className="summary-head">
        <h2 id="summary-title">Ringkasan situasi</h2>
        {summary ? (
          <span className={`summary-badge${summary.ai_generated ? '' : ' is-template'}`}>
            {summary.ai_generated ? 'AI' : 'Tanpa AI'}
          </span>
        ) : null}
      </div>
      {summary === undefined ? <div className="skeleton summary-skeleton" /> : null}
      {summary === null ? <p className="card-meta">Ringkasan tidak tersedia saat ini.</p> : null}
      {summary ? (
        <>
          <p className="summary-text">{summary.summary}</p>
          <p className="card-meta">
            {PROVIDER_LABEL[summary.provider] || summary.provider}
            {summary.model ? ` (${summary.model})` : ''} • {summary.event_count} kejadian dalam{' '}
            {summary.window_minutes} menit • ditulis {ageLabel(summary.age_seconds)}
            {summary.ai_generated ? ' • dibuat AI, periksa dengan data di bawah' : ' • dibuat dari hitungan, tanpa AI'}
          </p>
        </>
      ) : null}
    </section>
  );
}
