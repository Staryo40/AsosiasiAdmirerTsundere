use axum::extract::{Request, State};
use axum::middleware::Next;
use axum::response::Response;
use rand::RngExt;
use std::time::Duration;

/// BMKG's defining trait, unlike PVMBG's: a low and *consistent* delay, with no outage mode and
/// no admin route to change it at runtime. It applies to every response, including `/health` and
/// unauthorized ones, so a caller cannot tell a wrong key from a right one by timing alone.
#[derive(Debug, Clone, Copy)]
pub struct DelayRange {
    pub min_ms: u64,
    pub max_ms: u64,
}

/// Tagged onto the response so the (outer) access log middleware can report how much of the total
/// duration was this deliberate delay versus BMKG's own processing time.
#[derive(Debug, Clone, Copy)]
pub struct DelayMs(pub u64);

pub async fn add_response_delay(State(range): State<DelayRange>, request: Request, next: Next) -> Response {
    let delay_ms = pick_delay_ms(range, &mut rand::rng());
    if delay_ms > 0 {
        tokio::time::sleep(Duration::from_millis(delay_ms)).await;
    }
    let mut response = next.run(request).await;
    response.extensions_mut().insert(DelayMs(delay_ms));
    response
}

/// Takes the RNG as a parameter so the distribution can be tested deterministically without
/// touching the ambient thread-local generator the middleware itself uses.
fn pick_delay_ms(range: DelayRange, rng: &mut impl RngExt) -> u64 {
    rng.random_range(range.min_ms..=range.max_ms)
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::routing::get;
    use axum::{middleware, Router};
    use rand::rngs::StdRng;
    use rand::SeedableRng;
    use tokio::net::TcpListener;

    #[test]
    fn picked_delays_stay_within_the_range_and_reach_both_ends() {
        let mut rng = StdRng::seed_from_u64(1);
        let range = DelayRange { min_ms: 50, max_ms: 150 };
        let picks: Vec<u64> = (0..2000).map(|_| pick_delay_ms(range, &mut rng)).collect();

        assert!(picks.iter().all(|&ms| (50..=150).contains(&ms)));
        assert!(picks.contains(&50), "never picked the minimum in 2000 tries");
        assert!(picks.contains(&150), "never picked the maximum in 2000 tries");
    }

    #[test]
    fn equal_bounds_give_a_fixed_delay() {
        let mut rng = StdRng::seed_from_u64(1);
        let range = DelayRange { min_ms: 100, max_ms: 100 };

        for _ in 0..20 {
            assert_eq!(pick_delay_ms(range, &mut rng), 100);
        }
    }

    #[test]
    fn a_zero_range_never_delays() {
        let mut rng = StdRng::seed_from_u64(1);
        assert_eq!(pick_delay_ms(DelayRange { min_ms: 0, max_ms: 0 }, &mut rng), 0);
    }

    async fn spawn(range: DelayRange) -> String {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let app = Router::new()
            .route("/fast", get(|| async { "ok" }))
            .layer(middleware::from_fn_with_state(range, add_response_delay));
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        format!("http://{addr}")
    }

    async fn elapsed_ms(url: &str) -> u128 {
        let start = std::time::Instant::now();
        reqwest::get(url).await.unwrap();
        start.elapsed().as_millis()
    }

    #[tokio::test]
    async fn a_request_waits_at_least_the_minimum_delay() {
        let base = spawn(DelayRange { min_ms: 100, max_ms: 100 }).await;

        assert!(elapsed_ms(&format!("{base}/fast")).await >= 90, "took less than the configured 100ms delay");
    }

    #[tokio::test]
    async fn a_zero_delay_adds_essentially_nothing() {
        let base = spawn(DelayRange { min_ms: 0, max_ms: 0 }).await;

        assert!(elapsed_ms(&format!("{base}/fast")).await < 200, "a zero-delay request took suspiciously long");
    }

    #[tokio::test]
    async fn an_unmatched_route_is_delayed_too_since_the_layer_wraps_the_whole_router() {
        let base = spawn(DelayRange { min_ms: 100, max_ms: 100 }).await;

        assert!(elapsed_ms(&format!("{base}/no-such-route")).await >= 90);
    }

    // `#[tokio::test]` defaults to a single-threaded runtime; production runs under `#[tokio::main]`'s
    // multi-threaded one. Matching that here, plus reusing one `Client` instead of building 30 (each
    // of which does its own non-trivial setup), isolates the middleware's own behaviour from that overhead.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn many_delayed_requests_wait_in_parallel_without_blocking_each_other() {
        let base = spawn(DelayRange { min_ms: 200, max_ms: 200 }).await;
        let client = reqwest::Client::new();
        let start = std::time::Instant::now();

        let handles: Vec<_> = (0..30)
            .map(|_| {
                let client = client.clone();
                let url = format!("{base}/fast");
                tokio::spawn(async move { client.get(url).send().await.unwrap() })
            })
            .collect();
        for handle in handles {
            handle.await.unwrap();
        }

        let elapsed = start.elapsed().as_millis();
        assert!(elapsed < 1000, "30 requests at 200ms each took {elapsed}ms; they seem to block each other");
    }
}
