use std::collections::HashMap;
use std::env;
use std::fs::File;
use std::io::{self, Read, Write};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use byteorder::{BigEndian, WriteBytesExt};
use chrono::DateTime;
use futures_util::StreamExt;
use once_cell::sync::Lazy;
use regex::Regex;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::time::interval;
use tokio::spawn;
use tokio::runtime::Runtime;
use tokio_tungstenite::{connect_async, tungstenite::Message};
use tracing::{debug, error, info, warn};

/// Configuration for batch sizes
#[derive(Debug, Clone)]
struct Config {
    state_batch_size: usize,
    sensor_batch_size: usize,
    position_batch_size: usize,
    position_sample_rate: u64,
    sensor_sample_rate: u64,
    state_sample_rate: u64,
    websocket_url: String,
}

impl Config {
    fn from_env() -> Self {
        Self {
            state_batch_size: env::var("STATE_BATCH_SIZE")
              .unwrap_or_else(|_| "100".to_string())
              .parse()
              .unwrap_or(100),
            sensor_batch_size: env::var("SENSOR_BATCH_SIZE")
              .unwrap_or_else(|_| "500".to_string())
              .parse()
              .unwrap_or(500),
            position_batch_size: env::var("POSITION_BATCH_SIZE")
              .unwrap_or_else(|_| "250".to_string())
              .parse()
              .unwrap_or(250),
            position_sample_rate: env::var("POSITION_SAMPLE_RATE")
              .unwrap_or_else(|_| "1000".to_string())
              .parse()
              .unwrap_or(1000),
            sensor_sample_rate: env::var("SENSOR_SAMPLE_RATE")
              .unwrap_or_else(|_| "2000".to_string())
              .parse()
              .unwrap_or(2000),
            state_sample_rate: env::var("STATE_SAMPLE_RATE")
              .unwrap_or_else(|_| "5000".to_string())
              .parse()
              .unwrap_or(500),
            websocket_url: env::var("SIGNALK_WEBSOCKET_URL")
              .unwrap_or_else(|_| "ws://10.101.0.3:3000/signalk/v1/stream".to_string()),
        }
    }
}

/// Signal K delta message structure
#[derive(Debug, Deserialize)]
struct SignalKDelta {
    updates: Option<Vec<SignalKUpdate>>,
}

#[derive(Debug, Deserialize)]
struct SignalKUpdate {
    timestamp: Option<String>,
    source: Option<SignalKSource>,
    values: Vec<SignalKValue>,
}

#[derive(Debug, Deserialize)]
struct SignalKSource {
    label: Option<String>,
}

#[derive(Debug, Deserialize)]
struct SignalKValue {
    path: String,
    value: Value,
    timestamp: Option<String>,
}

/// Processed measurement data
#[derive(Debug, Serialize, Clone)]
struct MeasurementData {
    path: String,
    time: u64,
    value: Value,
    source: String,
}

/// Data categories
#[derive(Debug, Clone, Copy, PartialEq)]
enum Category {
    Dump,
    State,
    Position,
    Sensor,
}

impl Category {
    fn stream_id(&self) -> u8 {
        match self {
            Category::State => b't',
            Category::Sensor => b's',
            Category::Position => b'p',
            Category::Dump => unreachable!("Dump category should never be processed"),
        }
    }

    fn sample_rate(&self, config: &Config) -> u64 {
        match self {
            Category::Position => config.position_sample_rate,
            Category::Sensor => config.sensor_sample_rate,
            Category::State => config.state_sample_rate,
            Category::Dump => unreachable!("Dump category should never be processed"),
        }
    }

    fn batch_size(&self, config: &Config) -> usize {
        match self {
            Category::State => config.state_batch_size,
            Category::Sensor => config.sensor_batch_size,
            Category::Position => config.position_batch_size,
            Category::Dump => unreachable!("Dump category should never be processed"),
        }
    }
}

/// Categorization patterns - compiled regexes for performance
static CATEGORY_PATTERNS: Lazy<CategoryPatterns> = Lazy::new(|| CategoryPatterns::new());

struct CategoryPatterns {
    dump: Vec<Regex>,
    state: Vec<Regex>,
    position: Vec<Regex>,
    sensor: Vec<Regex>,
}

impl CategoryPatterns {
    fn new() -> Self {
        Self {
            dump: vec![
                Regex::new(r"^(.*)(totalPanel)(.*)$").unwrap(),
                Regex::new(r"^(.*)(ModeNumber)(.*)$").unwrap(),
                Regex::new(r"^(.*)(consumedCharge)(.*)$").unwrap(),
                Regex::new(r"^(electrical\.batteries.279)(.*)$").unwrap(),
            ],
            state: vec![
                Regex::new(r"^(.*)((m|M)ode)$").unwrap(),
                Regex::new(r"^(.*)\.(name)$").unwrap(),
                Regex::new(r"^(.*)\.(state)$").unwrap(),
                Regex::new(r"^(electrical\.batteries\.)([0-9]+)(\.capacity\.dischargedEnergy)$").unwrap(),
                Regex::new(r"^(electrical\.batteries\.)([0-9]+)(\.capacity\.stateOfCharge)$").unwrap(),
                Regex::new(r"^(electrical\.batteries\.)([0-9]+)(\.lifetimeDischarge)$").unwrap(),
                Regex::new(r"^(electrical\.solar\.)([0-9]+)(\.systemYield)$").unwrap(),
                Regex::new(r"^(electrical\.solar\.)([0-9]+)(\.yieldToday)$").unwrap(),
                Regex::new(r"^(electrical\.solar\.)([0-9]+)(\.yieldYesterday)$").unwrap(),
                Regex::new(r"^(navigation\.courseRhumbline\.nextPoint\.position)$").unwrap(),
                Regex::new(r"^(navigation\.currentRoute\.)(.*)$").unwrap(),
                Regex::new(r"^(navigation\.gns)(.*)$").unwrap(),
                Regex::new(r"^(navigation\.magneticVariation)$").unwrap(),
                Regex::new(r"^(navigation\.trip\.log)$").unwrap(),
                Regex::new(r"^(notifications)(.*)$").unwrap(),
                Regex::new(r"^(sensors\.ais)(.*)$").unwrap(),
                Regex::new(r"^(steering)(.*)$").unwrap(),
            ],
            position: vec![
                Regex::new(r"^(navigation\.position)$").unwrap(),
            ],
            sensor: vec![
                Regex::new(r"^(.*)([cC]urrent)$").unwrap(),
                Regex::new(r"^(.*)([pP]ower)$").unwrap(),
                Regex::new(r"^(.*)([vV]oltage)$").unwrap(),
                Regex::new(r"^(.*)([tT]emperature)$").unwrap(),
                Regex::new(r"^(environment)(.*)$").unwrap(),
                Regex::new(r"^(navigation)(.*)$").unwrap(),
            ],
        }
    }
}

/// Categorize data based on Signal K path - replicates JavaScript logic exactly
fn categorize_data(path: &str) -> Category {
    // Test dump patterns first (highest priority)
    for pattern in &CATEGORY_PATTERNS.dump {
        if pattern.is_match(path) {
            return Category::Dump;
        }
    }

    // Test state patterns
    for pattern in &CATEGORY_PATTERNS.state {
        if pattern.is_match(path) {
            return Category::State;
        }
    }

    // Test position patterns
    for pattern in &CATEGORY_PATTERNS.position {
        if pattern.is_match(path) {
            return Category::Position;
        }
    }

    // Test sensor patterns
    for pattern in &CATEGORY_PATTERNS.sensor {
        if pattern.is_match(path) {
            return Category::Sensor;
        }
    }

    // Default to dump if no pattern matches
    Category::Dump
}

/// Rate limiter for per-path throttling
struct RateLimiter {
    last_transmission_times: HashMap<String, u64>,
}

impl RateLimiter {
    fn new() -> Self {
        Self {
            last_transmission_times: HashMap::new(),
        }
    }

    fn should_transmit(&mut self, path: &str, category: Category, config: &Config) -> bool {
        let now = SystemTime::now()
          .duration_since(UNIX_EPOCH)
          .unwrap()
          .as_millis() as u64;

        let last_time = self.last_transmission_times.get(path).copied().unwrap_or(0);
        let sample_rate = category.sample_rate(config);

        if (now - last_time) >= sample_rate {
            self.last_transmission_times.insert(path.to_string(), now);
            true
        } else {
            false
        }
    }
}

/// Batch processor for collecting and compressing data
struct BatchProcessor {
    state_batch: Vec<MeasurementData>,
    sensor_batch: Vec<MeasurementData>,
    position_batch: Vec<MeasurementData>,
    dictionary: Option<Vec<u8>>,
}

impl BatchProcessor {
    fn new(dictionary: Option<Vec<u8>>) -> io::Result<Self> {
        Ok(Self {
            state_batch: Vec::new(),
            sensor_batch: Vec::new(),
            position_batch: Vec::new(),
            dictionary,
        })
    }

    fn add_data(&mut self, data: MeasurementData, category: Category, config: &Config) -> io::Result<()> {
        match category {
            Category::State => {
                self.state_batch.push(data);
                if self.state_batch.len() >= category.batch_size(config) {
                    self.flush_batch(Category::State)?;
                }
            }
            Category::Sensor => {
                self.sensor_batch.push(data);
                if self.sensor_batch.len() >= category.batch_size(config) {
                    self.flush_batch(Category::Sensor)?;
                }
            }
            Category::Position => {
                self.position_batch.push(data);
                if self.position_batch.len() >= category.batch_size(config) {
                    self.flush_batch(Category::Position)?;
                }
            }
            Category::Dump => {} // Ignored
        }
        Ok(())
    }

    fn flush_batch(&mut self, category: Category) -> io::Result<()> {
        let batch = match category {
            Category::State => std::mem::take(&mut self.state_batch),
            Category::Sensor => std::mem::take(&mut self.sensor_batch),
            Category::Position => std::mem::take(&mut self.position_batch),
            Category::Dump => return Ok(()),
        };

        if batch.is_empty() {
            return Ok(());
        }

        // Serialize batch to JSON
        let json_data = serde_json::to_string(&batch)?;

        // Compress the data using zstd
        let compressed_data = if let Some(ref dict) = self.dictionary {
            let mut encoder = zstd::stream::Encoder::with_dictionary(Vec::new(), 3, dict)
              .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
            encoder.write_all(json_data.as_bytes())?;
            encoder.finish()?.as_slice().to_vec()
        } else {
            debug!("No zstd dictionary loaded, using default compression");
            zstd::encode_all(json_data.as_bytes(), 3)
              .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?
        };

        // Write framed output to stdout
        write_framed_output(category, &compressed_data)?;

        debug!("Flushed {} {} messages", batch.len(), match category {
            Category::State => "state",
            Category::Sensor => "sensor",
            Category::Position => "position",
            Category::Dump => "dump",
        });

        Ok(())
    }

    fn flush_all(&mut self) -> io::Result<()> {
        self.flush_batch(Category::State)?;
        self.flush_batch(Category::Sensor)?;
        self.flush_batch(Category::Position)?;
        Ok(())
    }
}

static STDOUT_MUTEX: Lazy<Mutex<()>> = Lazy::new(|| Mutex::new(()));
/// Write framed output according to the custom binary protocol
fn write_framed_output(category: Category, compressed_data: &[u8]) -> io::Result<()> {
    let _lock = STDOUT_MUTEX.lock().unwrap(); // Ensure atomic writes
    let mut stdout = io::stdout();
    let stream_id = category.stream_id();
    let payload_size = 1 + compressed_data.len(); // 1 byte for stream ID + payload

    // Write 4-byte length prefix (big-endian)
    // stdout.write_u32::<BigEndian>(payload_size as u32)?;
    //
    // // Write 1-byte stream identifier
    // stdout.write_all(&[stream_id])?;
    //
    // // Write compressed payload
    // stdout.write_all(compressed_data)?;
    // stdout.flush()?;
    println!("First 16 bytes: {:02x?}", compressed_data);

    Ok(())
}

/// Parse timestamp from various formats to milliseconds
fn parse_timestamp(timestamp: Option<&str>) -> u64 {
    if let Some(ts_str) = timestamp {
        if let Ok(dt) = DateTime::parse_from_rfc3339(ts_str) {
            return dt.timestamp_millis() as u64;
        }
        // Try parsing as number
        if let Ok(ts_num) = ts_str.parse::<f64>() {
            return if ts_num > 1e12 {
                ts_num as u64
            } else {
                (ts_num * 1000.0) as u64
            };
        }
    }

    // Fallback to current time
    SystemTime::now()
      .duration_since(UNIX_EPOCH)
      .unwrap()
      .as_millis() as u64
}

/// Check if position has zero lat/lon (matches JavaScript logic)
fn is_zero_position(value: &Value) -> bool {
    if let Some(obj) = value.as_object() {
        if let (Some(lat), Some(lon)) = (obj.get("latitude"), obj.get("longitude")) {
            if let (Some(lat_num), Some(lon_num)) = (lat.as_f64(), lon.as_f64()) {
                return lat_num == 0.0 || lon_num == 0.0;
            }
        }
    }
    false
}

/// Load zstd dictionary from file
fn load_dictionary() -> Option<Vec<u8>> {
    let dict_path = env::var("SIGNALK_ZSTD_DICT_PATH").unwrap_or_else(|_| "signalk.zstddict".to_string());
    match File::open(&dict_path) {
        Ok(mut file) => {
            let mut dict = Vec::new();
            match file.read_to_end(&mut dict) {
                Ok(_) => {
                    info!("Loaded zstd dictionary from {} with {} bytes", dict_path, dict.len());
                    Some(dict)
                }
                Err(e) => {
                    warn!("Failed to read dictionary file {}: {}", dict_path, e);
                    None
                }
            }
        }
        Err(e) => {
            warn!("Dictionary file not found at {}: {}", dict_path, e);
            None
        }
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Create the runtime
    let rt = tokio::runtime::Builder::new_multi_thread()
      .enable_all()
      .build()?;

    // Run the async main function
    rt.block_on(async_main())
}

async fn async_main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize logging
    tracing_subscriber::fmt::init();

    // Load environment variables
    dotenvy::dotenv().ok();

    // Load configuration
    let config = Config::from_env();
    info!("Configuration: {:?}", config);

    // Load zstd dictionary
    let dictionary = load_dictionary();

    // Initialize components
    let rate_limiter: Arc<Mutex<RateLimiter>> = Arc::new(Mutex::new(RateLimiter::new()));
    let batch_processor: Arc<Mutex<BatchProcessor>> = Arc::new(Mutex::new(BatchProcessor::new(dictionary)?));

    // Connect to WebSocket
    info!("Connecting to SignalK WebSocket: {}", config.websocket_url);

    let (ws_stream, _) = connect_async(&config.websocket_url).await?;
    info!("Connected to SignalK WebSocket");

    let (mut _write, mut read) = ws_stream.split();

    // Set up periodic flush timer
    let batch_processor_clone = Arc::clone(&batch_processor);
    spawn(async move {
        let mut flush_interval = interval(Duration::from_secs(5));
        loop {
            flush_interval.tick().await;
            if let Ok(mut processor) = batch_processor_clone.lock() {
                if let Err(e) = processor.flush_all() {
                    error!("Error during periodic flush: {}", e);
                }
            }
        }
    });

    // Main message processing loop
    while let Some(message) = read.next().await {
        match message {
            Ok(Message::Text(text)) => {
                match serde_json::from_str::<SignalKDelta>(&text) {
                    Ok(delta) => {
                        if let Some(updates) = delta.updates {
                            for update in updates {
                                let source = update.source
                                  .and_then(|s| s.label)
                                  .unwrap_or_else(|| "unknown".to_string());

                                let update_timestamp = update.timestamp.as_deref();

                                for value in update.values {
                                    // Skip if no value
                                    if value.value.is_null() {
                                        continue;
                                    }

                                    // Categorize the data
                                    let category = categorize_data(&value.path);

                                    // Skip dump category
                                    if category == Category::Dump {
                                        // debug!("Discarding dump category data for path: {}", value.path);
                                        continue;
                                    }

                                    // Skip zero positions (matches JavaScript logic)
                                    if category == Category::Position && is_zero_position(&value.value) {
                                        debug!("Discarding position with zero lat/lon");
                                        continue;
                                    }

                                    // Check rate limiting
                                    {
                                        let mut limiter = rate_limiter.lock().unwrap();
                                        if !limiter.should_transmit(&value.path, category, &config) {
                                            continue;
                                        }
                                    }

                                    // Parse timestamp
                                    let timestamp = parse_timestamp(
                                        value.timestamp.as_deref().or(update_timestamp)
                                    );

                                    // Create measurement data
                                    let measurement = MeasurementData {
                                        path: value.path.clone(),
                                        time: timestamp,
                                        value: value.value,
                                        source: source.clone(),
                                    };

                                    // Add to batch
                                    {
                                        let mut processor = batch_processor.lock().unwrap();
                                        if let Err(e) = processor.add_data(measurement, category, &config) {
                                            error!("Error adding data to batch: {}", e);
                                        }
                                    }
                                }
                            }
                        }
                    }
                    Err(e) => {
                        // debug!("Raw SignalK delta text: {}", text);
                        error!("Failed to parse SignalK delta: {}", e);
                    }
                }
            }
            Ok(Message::Binary(_)) => {
                debug!("Received binary message (ignored)");
            }
            Ok(Message::Close(_)) => {
                info!("WebSocket connection closed");
                break;
            }
            Ok(Message::Ping(_)) | Ok(Message::Pong(_)) => {
                debug!("Received ping/pong");
            }
            Ok(Message::Frame(_)) => { /* ignore Frame variant */ }
            Err(e) => {
                error!("WebSocket error: {}", e);
                break;
            }
        }
    }

    // Final flush before shutdown
    {
        let mut processor = batch_processor.lock().unwrap();
        if let Err(e) = processor.flush_all() {
            error!("Error during final flush: {}", e);
        }
    }

    info!("Application shutdown complete");
    Ok(())
}