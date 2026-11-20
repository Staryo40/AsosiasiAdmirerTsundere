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

async function fetchPage(accessToken, params) {
  const qs = new URLSearchParams({
    view: 'full',
    limit: '500',
    ...params,
  });
  const response = await fetch(`/v1/hazards?${qs}`, {
    headers: { Authorization: `Bearer ${accessToken}` },
  });
  if (response.status === 403) {
    throw new Error('Akses ditolak. Gunakan klien Field Team atau Internal Ops.');
  }
  if (!response.ok) {
    throw new Error(`Gagal memuat data (HTTP ${response.status})`);
  }
  return response.json();
}

export async function loadAllHazards(accessToken, rangeKey, rangeMs) {
  const end = new Date();
  const start = new Date(end.getTime() - rangeMs);
  const range = {
    occurred_from: start.toISOString(),
    occurred_to: end.toISOString(),
  };

  const items = [];
  let cursor = null;
  let pages = 0;
  let asOf = null;
  const maxPages = 20;

  do {
    const params = { ...range };
    if (cursor) params.cursor = cursor;
    const page = await fetchPage(accessToken, params);
    asOf = page.as_of || asOf;
    if (Array.isArray(page.items)) {
      items.push(...page.items);
    }
    cursor = page.next_cursor || null;
    pages += 1;
    if (pages >= maxPages) break;
  } while (cursor);

  return { items, asOf, truncated: pages >= maxPages && !!cursor, rangeKey };
}