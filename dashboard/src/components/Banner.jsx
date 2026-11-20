export function Banner({ banner }) {
  if (!banner) return null;
  const cls = banner.kind === 'error' ? 'banner-error' : 'banner-warn';
  return (
    <div className={cls}>
      <h3>{banner.title}</h3>
      <p>{banner.text}</p>
    </div>
  );
}