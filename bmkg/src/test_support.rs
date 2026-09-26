//! Shared test-only helpers for capturing structured log output, analogous to PVMBG's `JsonCapture`.
use std::io;
use std::sync::{Arc, Mutex};
use tracing::subscriber::DefaultGuard;

#[derive(Clone, Default)]
struct SharedBuffer(Arc<Mutex<Vec<u8>>>);

impl io::Write for SharedBuffer {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(buf);
        Ok(buf.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for SharedBuffer {
    type Writer = Self;
    fn make_writer(&'a self) -> Self::Writer {
        self.clone()
    }
}

/// Installs a JSON subscriber as the *thread-local* default for as long as the returned guard is
/// alive. Relies on `#[tokio::test]`'s default single-threaded runtime: a spawned server task
/// (`tokio::spawn`) is cooperatively scheduled on the same OS thread as the test body, so it is
/// covered by this scope too, exactly as real BMKG output would render for its own configured level.
pub struct LogCapture {
    buffer: SharedBuffer,
    _guard: DefaultGuard,
}

impl LogCapture {
    pub fn new() -> Self {
        let buffer = SharedBuffer::default();
        let subscriber = tracing_subscriber::fmt()
            .json()
            .flatten_event(true)
            .with_current_span(false)
            .with_span_list(false)
            .with_writer(buffer.clone())
            .with_max_level(tracing::Level::DEBUG)
            .finish();
        let guard = tracing::subscriber::set_default(subscriber);
        Self { buffer, _guard: guard }
    }

    /// Every captured line, parsed as JSON, in the order they were logged.
    pub fn lines(&self) -> Vec<serde_json::Value> {
        let raw = String::from_utf8(self.buffer.0.lock().unwrap().clone()).expect("log output was not valid UTF-8");
        raw.lines().filter(|l| !l.is_empty()).map(|l| serde_json::from_str(l).unwrap_or_else(|e| panic!("not valid JSON: {e}\nline: {l}"))).collect()
    }

    pub fn events_named(&self, name: &str) -> Vec<serde_json::Value> {
        self.lines().into_iter().filter(|l| l.get("event").and_then(|e| e.as_str()) == Some(name)).collect()
    }
}
