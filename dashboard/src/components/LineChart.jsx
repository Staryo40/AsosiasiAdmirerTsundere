export function LineChart({ series }) {
  if (!series) return null;
  const width = 720;
  const height = 220;
  const pad = { top: 16, right: 16, bottom: 36, left: 40 };
  const innerW = width - pad.left - pad.right;
  const innerH = height - pad.top - pad.bottom;
  const maxY = Math.max(1, ...series.seismic, ...series.volcanic);
  const stepX = innerW / Math.max(1, series.labels.length - 1);

  const path = (values) =>
    values
      .map((v, i) => {
        const x = pad.left + i * stepX;
        const y = pad.top + innerH - (v / maxY) * innerH;
        return `${i === 0 ? 'M' : 'L'}${x.toFixed(1)},${y.toFixed(1)}`;
      })
      .join(' ');

  const gridLines = [0, 0.25, 0.5, 0.75, 1].map((f) => {
    const y = pad.top + innerH * (1 - f);
    return (
      <line
        key={f}
        x1={pad.left}
        y1={y}
        x2={width - pad.right}
        y2={y}
        stroke="#e2e8f0"
        strokeWidth="1"
      />
    );
  });

  return (
    <svg viewBox={`0 0 ${width} ${height}`} role="img" aria-label="Grafik garis kejadian per jenis">
      {gridLines}
      <path d={path(series.seismic)} fill="none" stroke="#0133CC" strokeWidth="2" />
      <path d={path(series.volcanic)} fill="none" stroke="#ED7B03" strokeWidth="2" />
      <text x={pad.left} y={height - 8} fill="#56657B" fontSize="11">
        Seismik
      </text>
      <text x={pad.left + 64} y={height - 8} fill="#56657B" fontSize="11">
        Vulkanik
      </text>
    </svg>
  );
}

export function SeriesTable({ series, open }) {
  if (!open || !series) return null;
  return (
    <div className="data-table-wrap">
      <table className="data-table">
        <thead>
          <tr>
            <th>Waktu</th>
            <th>Seismik</th>
            <th>Vulkanik</th>
          </tr>
        </thead>
        <tbody>
          {series.labels.map((label, i) => (
            <tr key={`${label}-${i}`}>
              <td>{label}</td>
              <td>{series.seismic[i]}</td>
              <td>{series.volcanic[i]}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}