export async function login(clientId, clientSecret) {
  const response = await fetch('/auth/login', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ client_id: clientId, client_secret: clientSecret }),
  });
  if (!response.ok) {
    const body = await response.json().catch(() => ({}));
    const msg = body?.error?.message || 'Kredensial tidak valid';
    throw new Error(msg);
  }
  return response.json();
}

const RANGE_BUCKET = { '1h': 'hour', '6h': 'hour', '24h': 'hour', '7d': 'day' };

export class AnalyticsBusyError extends Error {}

// Counts come from the Aggregator, so the dashboard never pages through raw hazards.
export async function loadAnalytics(accessToken, rangeKey, rangeMs) {
  const to = new Date();
  const from = new Date(to.getTime() - rangeMs);
  const qs = new URLSearchParams({ from: from.toISOString(), to: to.toISOString(), bucket: RANGE_BUCKET[rangeKey] });
  // The Aggregator serves one analytics query per instance at a time; one retry absorbs a brief 429.
  for (let attempt = 0; attempt < 2; attempt += 1) {
    const response = await fetch(`/v1/analytics?${qs}`, {
      headers: { Authorization: `Bearer ${accessToken}` },
    });
    if (response.ok) return response.json();
    if (response.status !== 429) {
      throw new Error(`Gagal memuat analitik (HTTP ${response.status})`);
    }
    await new Promise((resolve) => setTimeout(resolve, 1000));
  }
  throw new AnalyticsBusyError('Analitik sedang sibuk, coba segarkan sebentar lagi.');
}

// The summary is optional: any failure returns null and the panel says it is unavailable.
export async function loadSituationSummary(accessToken) {
  try {
    const response = await fetch('/v1/situation-summary', {
      headers: { Authorization: `Bearer ${accessToken}` },
    });
    if (!response.ok) return null;
    return await response.json();
  } catch {
    return null;
  }
}
