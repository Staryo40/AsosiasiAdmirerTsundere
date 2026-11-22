export const CATEGORIES = [
  { key: 'EARTHQUAKE', label: 'Gempa', color: '#0133CC' },
  { key: 'TSUNAMI', label: 'Tsunami', color: '#0891B2' },
  { key: 'VOLCANIC', label: 'Vulkanik', color: '#ED7B03' },
  { key: 'WILDFIRE', label: 'Kebakaran', color: '#B91C1C' },
  { key: 'COASTAL', label: 'Pantai', color: '#15803D' },
];

const STEP_MS = { hour: 3600 * 1000, day: 24 * 3600 * 1000, week: 7 * 24 * 3600 * 1000 };

function bucketStart(ms, bucket) {
  const date = new Date(ms);
  if (bucket === 'hour') date.setUTCMinutes(0, 0, 0);
  else date.setUTCHours(0, 0, 0, 0);
  if (bucket === 'week') date.setUTCDate(date.getUTCDate() - ((date.getUTCDay() + 6) % 7));
  return date.getTime();
}

// The Aggregator sends only non-zero buckets; the client fills the empty ones.
export function buildSeries(analytics) {
  const step = STEP_MS[analytics.bucket];
  const end = Date.parse(analytics.to);
  const starts = [];
  for (let t = bucketStart(Date.parse(analytics.from), analytics.bucket); t < end; t += step) starts.push(t);
  const index = new Map(starts.map((t, i) => [t, i]));
  const lines = CATEGORIES.map((category) => ({ ...category, values: Array(starts.length).fill(0) }));
  for (const entry of analytics.series) {
    const i = index.get(Date.parse(entry.start));
    const line = lines.find((l) => l.key === entry.category);
    if (i !== undefined && line) line.values[i] += entry.count;
  }
  const labels = starts.map((t) =>
    new Date(t).toLocaleString('id-ID', analytics.bucket === 'hour'
      ? { hour: '2-digit', minute: '2-digit' }
      : { day: 'numeric', month: 'short' })
  );
  return { labels, lines };
}

export function categoryTotals(analytics) {
  const totals = Object.fromEntries(CATEGORIES.map((c) => [c.key, 0]));
  for (const entry of analytics.series) totals[entry.category] = (totals[entry.category] || 0) + entry.count;
  return totals;
}

export function severityEntries(severity) {
  return ['AWAS', 'SIAGA', 'WASPADA', 'NORMAL'].map((level) => [level, severity[level] || 0]).filter(([, n]) => n > 0);
}

export function areaEntries(areas) {
  return [...areas].sort((a, b) => b.count - a.count).map((a) => [a.area_name, a.count]);
}
