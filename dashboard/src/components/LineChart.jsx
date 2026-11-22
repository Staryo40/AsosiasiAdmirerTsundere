export function LineChart({ series }) {
  if (!series) return null;
  const width = 720;
  const height = 220;
  const pad = { top: 16, right: 16, bottom: 36, left: 40 };
  const innerW = width - pad.left - pad.right;
  const innerH = height - pad.top - pad.bottom;
  const maxY = Math.max(1, ...series.lines.flatMap((line) => line.values));
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
      <line key={f} x1={pad.left} y1={y} x2={width - pad.right} y2={y} stroke="#e2e8f0" strokeWidth="1" />
    );
  });

  return (
    <svg viewBox={`0 0 ${width} ${height}`} role="img" aria-label="Grafik garis kejadian per kategori">
      {gridLines}
      <text x={pad.left - 6} y={pad.top + 4} fill="#56657B" fontSize="10" textAnchor="end">
        {maxY}
      </text>
      {series.lines.map((line) => (
        <path key={line.key} d={path(line.values)} fill="none" stroke={line.color} strokeWidth="2" />
      ))}
      {series.lines.map((line, i) => (
        <text key={line.key} x={pad.left + i * 96} y={height - 8} fill={line.color} fontSize="11" fontWeight="700">
          {line.label}
        </text>
      ))}
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
            {series.lines.map((line) => (
              <th key={line.key}>{line.label}</th>
            ))}
          </tr>
        </thead>
        <tbody>
          {series.labels.map((label, i) => (
            <tr key={`${label}-${i}`}>
              <td>{label}</td>
              {series.lines.map((line) => (
                <td key={line.key}>{line.values[i]}</td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
