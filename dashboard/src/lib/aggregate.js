export function bucketSeries(items, rangeKey, rangeMs) {
  const end = Date.now();
  const start = end - rangeMs;
  const bucketCount =
    rangeKey === '1h' ? 12 : rangeKey === '6h' ? 12 : rangeKey === '24h' ? 24 : 14;
  const bucketMs = (end - start) / bucketCount;
  const seismic = Array(bucketCount).fill(0);
  const volcanic = Array(bucketCount).fill(0);
  const labels = [];

  for (let i = 0; i < bucketCount; i += 1) {
    const t = start + bucketMs * (i + 0.5);
    labels.push(
      new Date(t).toLocaleString('id-ID', {
        hour: '2-digit',
        minute: '2-digit',
        month: rangeKey === '7d' ? 'short' : undefined,
        day: rangeKey === '7d' ? 'numeric' : undefined,
      })
    );
  }

  for (const item of items) {
    const ts = Date.parse(item.occurred_at);
    if (Number.isNaN(ts) || ts < start || ts > end) continue;
    const idx = Math.min(bucketCount - 1, Math.floor((ts - start) / bucketMs));
    if (item.hazard_type === 'SEISMIC') seismic[idx] += 1;
    else if (item.hazard_type === 'VOLCANIC') volcanic[idx] += 1;
  }
  return { labels, seismic, volcanic };
}

export function countBy(items, key) {
  const map = new Map();
  for (const item of items) {
    const k = item[key] || '(tidak diketahui)';
    map.set(k, (map.get(k) || 0) + 1);
  }
  return [...map.entries()].sort((a, b) => b[1] - a[1]);
}