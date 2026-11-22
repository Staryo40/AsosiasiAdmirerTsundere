import { useState } from 'react';
import { RANGE_LABEL, RANGE_OPTIONS } from '../lib/constants.js';
import { Banner } from './Banner.jsx';
import { LineChart, SeriesTable } from './LineChart.jsx';
import { BarChart } from './BarChart.jsx';
import { SummaryCard } from './SummaryCard.jsx';
import { CATEGORIES } from '../lib/aggregate.js';

// Shown on every card while any storage unit is missing, even when a count is zero.
function PartialMarker({ data }) {
  if (!data?.partial) return null;
  return <p className="partial-marker">Data belum lengkap: {data.unavailableUnits.join(', ')} tidak terjangkau</p>;
}

export function DashboardMain({
  clientId,
  accessToken,
  activeRange,
  onRangeChange,
  onRefresh,
  loading,
  banner,
  data,
}) {
  const [tableOpen, setTableOpen] = useState(false);

  const statsMeta = data
    ? `${RANGE_LABEL[activeRange]} • diperbarui ${
        data.asOf ? new Date(data.asOf).toLocaleString('id-ID') : '—'
      }`
    : '—';

  return (
    <main className="page">
      <div className="page-head">
        <h1>Kejadian bahaya</h1>
        <div className="page-head-right">
          <div className="tabs" role="tablist" aria-label="Rentang waktu">
            {RANGE_OPTIONS.map((key) => (
              <button
                key={key}
                type="button"
                role="tab"
                aria-selected={activeRange === key}
                className={`tab${activeRange === key ? ' is-active' : ''}`}
                onClick={() => onRangeChange(key)}
              >
                {key === '1h' ? '1 jam' : key === '6h' ? '6 jam' : key === '24h' ? '24 jam' : '7 hari'}
              </button>
            ))}
          </div>
          <button type="button" className="link-action" onClick={onRefresh} disabled={loading}>
            Segarkan
          </button>
        </div>
      </div>

      <Banner banner={banner} />

      <SummaryCard accessToken={accessToken} />

      <section className="card stats-card" aria-labelledby="stats-title">
        <h2 id="stats-title">Jumlah kejadian</h2>
        <p className="card-meta">{statsMeta}</p>
        <PartialMarker data={data} />
        <div className="stat-grid">
          {[
            ['Total', data?.total ?? 0],
            ...CATEGORIES.map((category) => [category.label, data?.byCategory[category.key] ?? 0]),
          ].map(([label, value]) => (
            <div className="stat-box" key={label}>
              <span>{label}</span>
              <strong>{value}</strong>
            </div>
          ))}
        </div>
      </section>

      <section className="card chart-card">
        <h2>Kejadian per jenis bahaya</h2>
        <p className="card-meta">Garis menunjukkan jumlah per interval waktu dalam rentang yang dipilih.</p>
        <PartialMarker data={data} />
        <div className="chart-wrap">
          {loading ? <div className="skeleton" /> : <LineChart series={data?.series} />}
        </div>
        <button type="button" className="btn-secondary" onClick={() => setTableOpen((v) => !v)}>
          {tableOpen ? 'Sembunyikan tabel' : 'Tampilkan sebagai tabel'}
        </button>
        <SeriesTable series={data?.series} open={tableOpen} />
      </section>

      <div className="two-col">
        <section className="card chart-card">
          <h2>Per severity</h2>
          <PartialMarker data={data} />
          <div className="chart-wrap bar-chart">
            {loading ? (
              <div className="skeleton" />
            ) : (
              <BarChart entries={data?.bySeverity} color="#0133CC" />
            )}
          </div>
        </section>
        <section className="card chart-card">
          <h2>Per wilayah</h2>
          <PartialMarker data={data} />
          <div className="chart-wrap bar-chart">
            {loading ? (
              <div className="skeleton" />
            ) : (
              <BarChart entries={data?.byArea} color="#56657B" />
            )}
          </div>
        </section>
      </div>

      <p className="footnote">
        Sumber: GET /v1/analytics, dihitung Aggregator dari seluruh unit penyimpanan
        {clientId ? ` (klien: ${clientId})` : ''}. Gempa dengan peringatan tsunami dihitung sebagai Tsunami.
      </p>
    </main>
  );
}