import { useState } from 'react';

export function LoginPanel({ onLogin }) {
  const [error, setError] = useState('');
  const [busy, setBusy] = useState(false);

  async function handleSubmit(ev) {
    ev.preventDefault();
    setError('');
    setBusy(true);
    const fd = new FormData(ev.target);
    try {
      await onLogin(String(fd.get('client_id')).trim(), String(fd.get('client_secret')));
    } catch (err) {
      setError(err.message || 'Gagal masuk');
    } finally {
      setBusy(false);
    }
  }

  return (
    <section className="login-panel">
      <div className="card login-card">
        <h1>Masuk untuk melihat data</h1>
        <p className="card-meta">
          Gunakan kredensial klien BNPB (Field Team atau Internal Ops). Secret tidak disimpan di
          peramban setelah tab ditutup.
        </p>
        <form onSubmit={handleSubmit}>
          <label className="field">
            <span>Client ID</span>
            <input name="client_id" type="text" autoComplete="username" required placeholder="field-team" />
          </label>
          <label className="field">
            <span>Client secret</span>
            <input name="client_secret" type="password" autoComplete="current-password" required />
          </label>
          {error ? <p className="form-error">{error}</p> : null}
          <button type="submit" className="btn-primary" disabled={busy}>
            {busy ? 'Memproses…' : 'Masuk'}
          </button>
        </form>
      </div>
    </section>
  );
}