import { useCallback, useEffect, useMemo, useState } from 'react';
import { RANGE_MS } from './lib/constants.js';
import { loadAllHazards, login as apiLogin } from './lib/api.js';
import { bucketSeries, countBy } from './lib/aggregate.js';
import { LoginPanel } from './components/LoginPanel.jsx';
import { DashboardMain } from './components/DashboardMain.jsx';

function formatTopbarDate() {
  const now = new Date();
  return new Intl.DateTimeFormat('id-ID', {
    weekday: 'long',
    day: 'numeric',
    month: 'long',
    year: 'numeric',
  }).format(now);
}

export default function App() {
  const [topbarDate, setTopbarDate] = useState(formatTopbarDate);
  const [session, setSession] = useState(null);
  const [activeRange, setActiveRange] = useState('24h');
  const [loading, setLoading] = useState(false);
  const [banner, setBanner] = useState(null);
  const [data, setData] = useState(null);

  useEffect(() => {
    const id = setInterval(() => setTopbarDate(formatTopbarDate()), 60_000);
    return () => clearInterval(id);
  }, []);

  const refresh = useCallback(async () => {
    if (!session?.accessToken) return;
    setLoading(true);
    setBanner(null);
    try {
      const result = await loadAllHazards(session.accessToken, activeRange, RANGE_MS[activeRange]);
      const series = bucketSeries(result.items, activeRange, RANGE_MS[activeRange]);
      setData({
        items: result.items,
        asOf: result.asOf,
        truncated: result.truncated,
        series,
        bySeverity: countBy(result.items, 'severity'),
        byArea: countBy(result.items, 'area_name'),
      });
      if (result.truncated) {
        setBanner({
          kind: 'warn',
          title: 'Data belum lengkap',
          text: 'Hanya sebagian halaman yang dimuat. Setelah API agregasi Aggregator (A1-08) tersedia, rentang panjang akan lebih akurat.',
        });
      }
    } catch (err) {
      setBanner({ kind: 'error', title: 'Gagal memuat', text: err.message || 'Terjadi kesalahan' });
    } finally {
      setLoading(false);
    }
  }, [session, activeRange]);

  useEffect(() => {
    if (session) refresh();
  }, [session, activeRange, refresh]);

  const handleLogin = useCallback(async (clientId, clientSecret) => {
    const tokens = await apiLogin(clientId, clientSecret);
    setSession({ clientId, accessToken: tokens.access_token });
  }, []);

  const logout = useCallback(() => {
    setSession(null);
    setData(null);
    setBanner(null);
  }, []);

  const header = useMemo(
    () => (
      <header className="site-header">
        <div>
          <p className="brand">Sistem Informasi Kebencanaan BNPB</p>
          <p className="brand-sub">Dashboard analitik kejadian</p>
        </div>
        {session ? (
          <div className="header-actions">
            <span className="status-pill">Masuk sebagai {session.clientId}</span>
            <button type="button" className="link-action" onClick={logout}>
              Keluar
            </button>
          </div>
        ) : null}
      </header>
    ),
    [session, logout]
  );

  return (
    <>
      <div className="topbar">{topbarDate}</div>
      {header}
      {session ? (
        <DashboardMain
          clientId={session.clientId}
          accessToken={session.accessToken}
          activeRange={activeRange}
          onRangeChange={setActiveRange}
          onRefresh={refresh}
          loading={loading}
          banner={banner}
          data={data}
        />
      ) : (
        <LoginPanel onLogin={handleLogin} />
      )}
      <footer className="site-footer">
        <div>
          <h3>Dokumentasi</h3>
          <p>Data bersumber dari Client-Facing API (GET /v1/hazards). UI React + Vite.</p>
        </div>
        <div>
          <p>IF4031 — Arsitektur Aplikasi Terdistribusi</p>
        </div>
      </footer>
    </>
  );
}