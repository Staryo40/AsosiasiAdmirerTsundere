use std::fmt;
use std::str::FromStr;

const LOG_LEVELS: [&str; 6] = ["TRACE", "DEBUG", "INFO", "WARN", "ERROR", "OFF"];
const LOG_FORMATS: [&str; 2] = ["json", "text"];

#[derive(Clone)]
pub struct AppConfig {
    pub port: u16,
    pub api_key: String,
    pub response_delay_min_ms: u64,
    pub response_delay_max_ms: u64,
    pub generator_interval_ms: u64,
    pub log_level: String,
    pub log_format: String,
}

// Never derived: a derived Debug would print `api_key` in plain text in any startup log or panic message.
impl fmt::Debug for AppConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AppConfig")
            .field("port", &self.port)
            .field("api_key", &"***")
            .field("response_delay_min_ms", &self.response_delay_min_ms)
            .field("response_delay_max_ms", &self.response_delay_max_ms)
            .field("generator_interval_ms", &self.generator_interval_ms)
            .field("log_level", &self.log_level)
            .field("log_format", &self.log_format)
            .finish()
    }
}

#[derive(Debug)]
pub struct ConfigError(pub String);

impl fmt::Display for ConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::error::Error for ConfigError {}

impl AppConfig {
    /// Loads `.env` (if present) and then reads from the real process environment.
    pub fn load() -> Result<Self, ConfigError> {
        dotenvy::dotenv().ok();
        Self::from_env(|key| std::env::var(key).ok())
    }

    /// Pure version of `load`, driven by any key lookup function. Kept separate so tests can supply
    /// an in-memory map instead of mutating real process environment variables (which cargo test runs in parallel).
    pub fn from_env(get: impl Fn(&str) -> Option<String>) -> Result<Self, ConfigError> {
        let api_key = require(&get, "BMKG_API_KEY")?;
        let port = parse_or_default(&get, "BMKG_PORT", 8081u16)?;

        let min_ms = parse_or_default(&get, "BMKG_RESPONSE_DELAY_MIN_MS", 50u64)?;
        let max_ms = parse_or_default(&get, "BMKG_RESPONSE_DELAY_MAX_MS", 150u64)?;
        if min_ms > max_ms {
            return Err(ConfigError(format!(
                "Invalid response delay: BMKG_RESPONSE_DELAY_MIN_MS={min_ms}, BMKG_RESPONSE_DELAY_MAX_MS={max_ms}. \
                 minimum delay must not be greater than the maximum."
            )));
        }

        let generator_interval_seconds: f64 = parse_or_default(&get, "BMKG_GENERATOR_INTERVAL_SECONDS", 10.0)?;
        if !generator_interval_seconds.is_finite() || generator_interval_seconds < 0.001 {
            return Err(ConfigError(format!(
                "Invalid generator interval: BMKG_GENERATOR_INTERVAL_SECONDS={generator_interval_seconds}. \
                 It must be a number of seconds, at least 0.001."
            )));
        }
        let generator_interval_ms = (generator_interval_seconds * 1000.0) as u64;

        let log_level = get("BMKG_LOG_LEVEL").unwrap_or_else(|| "INFO".to_string()).to_uppercase();
        if !LOG_LEVELS.contains(&log_level.as_str()) {
            return Err(ConfigError(format!(
                "Invalid log level BMKG_LOG_LEVEL={log_level}. Use one of: {}",
                LOG_LEVELS.join(", ")
            )));
        }

        let log_format = get("BMKG_LOG_FORMAT").unwrap_or_else(|| "json".to_string());
        if !LOG_FORMATS.contains(&log_format.as_str()) {
            return Err(ConfigError(format!(
                "Invalid log format BMKG_LOG_FORMAT={log_format}. Use one of (lowercase): {}",
                LOG_FORMATS.join(", ")
            )));
        }

        Ok(AppConfig {
            port,
            api_key,
            response_delay_min_ms: min_ms,
            response_delay_max_ms: max_ms,
            generator_interval_ms,
            log_level,
            log_format,
        })
    }
}

fn require(get: &impl Fn(&str) -> Option<String>, key: &str) -> Result<String, ConfigError> {
    match get(key) {
        Some(v) if !v.is_empty() => Ok(v),
        _ => Err(ConfigError(format!(
            "Required environment variable \"{key}\" not found and no default value is present"
        ))),
    }
}

fn parse_or_default<T: FromStr>(get: &impl Fn(&str) -> Option<String>, key: &str, default: T) -> Result<T, ConfigError> {
    match get(key) {
        None => Ok(default),
        Some(v) => v
            .parse::<T>()
            .map_err(|_| ConfigError(format!("Invalid value for {key}: '{v}'"))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn env_of(pairs: &[(&str, &str)]) -> impl Fn(&str) -> Option<String> {
        let map: HashMap<String, String> = pairs.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect();
        move |key: &str| map.get(key).cloned()
    }

    const VALID: &[(&str, &str)] = &[("BMKG_API_KEY", "test-key")];

    #[test]
    fn missing_api_key_is_rejected() {
        let err = AppConfig::from_env(env_of(&[])).unwrap_err();
        assert!(err.0.contains("BMKG_API_KEY"), "{}", err.0);
    }

    #[test]
    fn defaults_are_applied_when_nothing_else_is_set() {
        let config = AppConfig::from_env(env_of(VALID)).unwrap();
        assert_eq!(config.port, 8081);
        assert_eq!(config.response_delay_min_ms, 50);
        assert_eq!(config.response_delay_max_ms, 150);
        assert_eq!(config.generator_interval_ms, 10_000);
        assert_eq!(config.log_level, "INFO");
        assert_eq!(config.log_format, "json");
    }

    #[test]
    fn every_value_can_be_overridden() {
        let config = AppConfig::from_env(env_of(&[
            ("BMKG_API_KEY", "test-key"),
            ("BMKG_PORT", "9091"),
            ("BMKG_RESPONSE_DELAY_MIN_MS", "10"),
            ("BMKG_RESPONSE_DELAY_MAX_MS", "20"),
            ("BMKG_GENERATOR_INTERVAL_SECONDS", "0.5"),
            ("BMKG_LOG_LEVEL", "debug"),
            ("BMKG_LOG_FORMAT", "text"),
        ]))
        .unwrap();
        assert_eq!(config.port, 9091);
        assert_eq!(config.response_delay_min_ms, 10);
        assert_eq!(config.response_delay_max_ms, 20);
        assert_eq!(config.generator_interval_ms, 500);
        assert_eq!(config.log_level, "DEBUG", "log level must be normalised to uppercase");
        assert_eq!(config.log_format, "text");
    }

    #[test]
    fn a_delay_minimum_above_the_maximum_is_rejected() {
        let err = AppConfig::from_env(env_of(&[
            ("BMKG_API_KEY", "test-key"),
            ("BMKG_RESPONSE_DELAY_MIN_MS", "200"),
            ("BMKG_RESPONSE_DELAY_MAX_MS", "100"),
        ]))
        .unwrap_err();
        assert!(err.0.contains("BMKG_RESPONSE_DELAY_MIN_MS") && err.0.contains("BMKG_RESPONSE_DELAY_MAX_MS"), "{}", err.0);
    }

    #[test]
    fn equal_delay_bounds_are_accepted() {
        AppConfig::from_env(env_of(&[
            ("BMKG_API_KEY", "test-key"),
            ("BMKG_RESPONSE_DELAY_MIN_MS", "100"),
            ("BMKG_RESPONSE_DELAY_MAX_MS", "100"),
        ]))
        .unwrap();
    }

    #[test]
    fn a_zero_or_negative_or_non_numeric_generator_interval_is_rejected() {
        for bad in ["0", "-1", "0.0001", "abc", "", "NaN", "inf"] {
            let err = AppConfig::from_env(env_of(&[("BMKG_API_KEY", "test-key"), ("BMKG_GENERATOR_INTERVAL_SECONDS", bad)]))
                .unwrap_err();
            assert!(err.0.contains("BMKG_GENERATOR_INTERVAL_SECONDS"), "'{bad}' gave: {}", err.0);
        }
    }

    #[test]
    fn the_smallest_allowed_generator_interval_is_accepted() {
        AppConfig::from_env(env_of(&[("BMKG_API_KEY", "test-key"), ("BMKG_GENERATOR_INTERVAL_SECONDS", "0.001")])).unwrap();
    }

    #[test]
    fn an_unknown_log_level_is_rejected() {
        let err = AppConfig::from_env(env_of(&[("BMKG_API_KEY", "test-key"), ("BMKG_LOG_LEVEL", "LOUD")])).unwrap_err();
        assert!(err.0.contains("BMKG_LOG_LEVEL"), "{}", err.0);
    }

    #[test]
    fn an_unknown_or_uppercase_log_format_is_rejected() {
        assert!(AppConfig::from_env(env_of(&[("BMKG_API_KEY", "test-key"), ("BMKG_LOG_FORMAT", "xml")])).is_err());
        assert!(AppConfig::from_env(env_of(&[("BMKG_API_KEY", "test-key"), ("BMKG_LOG_FORMAT", "JSON")])).is_err());
    }

    #[test]
    fn an_empty_api_key_is_treated_as_missing() {
        let err = AppConfig::from_env(env_of(&[("BMKG_API_KEY", "")])).unwrap_err();
        assert!(err.0.contains("BMKG_API_KEY"), "{}", err.0);
    }

    #[test]
    fn debug_output_never_contains_the_api_key() {
        let config = AppConfig::from_env(env_of(&[("BMKG_API_KEY", "super-secret-value")])).unwrap();
        let debug = format!("{config:?}");
        assert!(!debug.contains("super-secret-value"), "the api key leaked into Debug output: {debug}");
    }
}
