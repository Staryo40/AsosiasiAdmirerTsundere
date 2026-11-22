export function BarChart({ entries, color }) {
  if (!entries?.length) {
    return <p className="empty-state">Belum ada kejadian pada rentang ini</p>;
  }
  const top = entries.slice(0, 8);
  const max = Math.max(1, ...top.map(([, count]) => count));
  const rowH = 28;
  const height = 40 + top.length * rowH;
  const width = 360;

  return (
    <svg viewBox={`0 0 ${width} ${height}`} width="100%">
      {top.map(([label, count], i) => {
        const y = 20 + i * rowH;
        const w = (count / max) * (width - 140);
        const short = label.length > 28 ? `${label.slice(0, 25)}…` : label;
        return (
          <g key={label}>
            <text x="0" y={y + 14} fill="#56657B" fontSize="11">
              {short}
            </text>
            <rect x="130" y={y} width={w} height="18" rx="4" fill={color} />
            <text x={130 + w + 6} y={y + 13} fill="#0F172A" fontSize="11" fontWeight="700">
              {count}
            </text>
          </g>
        );
      })}
    </svg>
  );
}