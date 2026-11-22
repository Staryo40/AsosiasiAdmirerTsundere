// P1 dashboard data checks drafted with OpenAI Codex assistance.
import { bucketSeries, countBy } from '../../dashboard/src/lib/aggregate.js';
import { loadAllHazards } from '../../dashboard/src/lib/api.js';

function check(condition, label, details) {
  console.log(`${condition ? 'PASS' : 'FAIL'} ${label} (${details})`);
  if (!condition) process.exitCode = 1;
}

const occurredAt = new Date(Date.now() - 60_000).toISOString();
const rangeMs = 60 * 60 * 1000;
const hazards = [
  { source: 'BMKG', hazard_type: 'SEISMIC', severity: 'NORMAL', area_name: 'Area A' },
  {
    source: 'BMKG', hazard_type: 'SEISMIC', source_ref_id: 'fixture-earthquake-with-warning',
    severity: 'SIAGA', area_name: 'Area A',
    attributes: {
      potential_tsunami: true,
      tsunami_warnings: [{ related_event_id: 'fixture-earthquake-with-warning', threat_level: 'Siaga' }],
    },
  },
  { source: 'PVMBG', hazard_type: 'VOLCANIC', severity: 'WASPADA', area_name: 'Area B' },
  { source: 'SATELIT', hazard_type: 'WILDFIRE', severity: 'SIAGA', area_name: 'Area C' },
  { source: 'SENSOR_PANTAI', hazard_type: 'COASTAL', severity: 'NORMAL', area_name: 'Area A' },
].map((hazard) => ({ occurred_at: occurredAt, ...hazard }));
const { labels, ...series } = bucketSeries(hazards, '1h', rangeMs);
const totals = Object.fromEntries(Object.entries(series).map(([category, buckets]) => [
  category, buckets.reduce((sum, count) => sum + count, 0),
]));
check(Object.keys(totals).length === 5 && Object.values(totals).every((count) => count === 1),
  'one plotted event in each of five hazard categories', JSON.stringify(totals));

const severity = Object.fromEntries(countBy(hazards, 'severity'));
const areas = Object.fromEntries(countBy(hazards, 'area_name'));
check(Object.keys(severity).length === 3 && severity.NORMAL === 2 && severity.SIAGA === 2 && severity.WASPADA === 1
    && Object.keys(areas).length === 3 && areas['Area A'] === 3 && areas['Area B'] === 1 && areas['Area C'] === 1,
  'severity and area totals agree with the fixture', `severity=${JSON.stringify(severity)} areas=${JSON.stringify(areas)}`);

globalThis.fetch = async () => ({
  ok: true,
  json: async () => ({
    items: [], next_cursor: null, partial: true, unavailable_units: ['fixture-shard'],
  }),
});
const data = await loadAllHazards('fixture-token', '1h', rangeMs);
check(data.partial === true, 'a partial API result remains marked incomplete', `partial=${data.partial}`);
