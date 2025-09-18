// use crate::serialize;
// use chrono::naive::serde::ts_microseconds::serialize;
// use bincode::{serialize, deserialize};
use std::collections::HashMap;
use std::env;
use std::fs::{File, OpenOptions};
use std::io::{self, Read, Write, Seek, SeekFrom, BufReader, BufWriter};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use chrono::DateTime;
use futures_util::StreamExt;
use once_cell::sync::Lazy;
use regex::Regex;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::sync::mpsc;
use tokio::time::{interval, timeout};
use tokio::{spawn, task};
use tokio_tungstenite::{connect_async, tungstenite::Message};
use tracing::{debug, error, info, warn};
use uuid::Uuid;

/// Configuration for the application
#[derive(Debug, Clone)]
struct Config {
    // Existing batch configuration
    state_batch_size: usize,
    sensor_batch_size: usize,
    position_batch_size: usize,
    position_sample_rate: u64,
    sensor_sample_rate: u64,
    state_sample_rate: u64,
    websocket_url: String,

    // New HTTP sink configuration
    sensor_endpoint: String,
    position_endpoint: String,
    state_endpoint: String,
    auth_token: String,

    // Batching and buffer configuration
    max_batch_size_bytes: usize,
    batch_timeout_secs: u64,
    max_memory_buffer_bytes: usize,
    buffer_directory: PathBuf,

    // HTTP client configuration
    request_timeout_secs: u64,
    max_concurrent_requests: usize,
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
              .unwrap_or(5000),
            websocket_url: env::var("SIGNALK_WEBSOCKET_URL")
              .unwrap_or_else(|_| "ws://10.101.0.3:3000/signalk/v1/stream".to_string()),

            // HTTP endpoints
            sensor_endpoint: env::var("SENSOR_ENDPOINT")
              .unwrap_or_else(|_| "https://actix.serengwen.thescareys.co.uk/sensor_data".to_string()),
            position_endpoint: env::var("POSITION_ENDPOINT")
              .unwrap_or_else(|_| "https://actix.serengwen.thescareys.co.uk/position_data".to_string()),
            state_endpoint: env::var("STATE_ENDPOINT")
              .unwrap_or_else(|_| "https://actix.serengwen.thescareys.co.uk/state_data".to_string()),
            auth_token: env::var("AUTH_TOKEN")
              .unwrap_or_else(|_| "NO_TOKEN".to_string()),

            // Batching configuration
            max_batch_size_bytes: env::var("MAX_BATCH_SIZE_BYTES")
              .unwrap_or_else(|_| "1048576".to_string()) // 1MB default
              .parse()
              .unwrap_or(1048576),
            batch_timeout_secs: env::var("BATCH_TIMEOUT_SECS")
              .unwrap_or_else(|_| "5".to_string())
              .parse()
              .unwrap_or(5),
            max_memory_buffer_bytes: env::var("MAX_MEMORY_BUFFER_BYTES")
              .unwrap_or_else(|_| "1073741824".to_string()) // 1GB default
              .parse()
              .unwrap_or(1073741824),
            buffer_directory: PathBuf::from(env::var("BUFFER_DIRECTORY")
              .unwrap_or_else(|_| "/var/lib/signalk_buffer".to_string())),

            // HTTP client configuration
            request_timeout_secs: env::var("REQUEST_TIMEOUT_SECS")
              .unwrap_or_else(|_| "30".to_string())
              .parse()
              .unwrap_or(30),
            max_concurrent_requests: env::var("MAX_CONCURRENT_REQUESTS")
              .unwrap_or_else(|_| "15".to_string())
              .parse()
              .unwrap_or(15),
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
#[derive(Debug, Serialize, Deserialize, Clone)]
struct MeasurementData {
    path: String,
    time: u64,
    value: Value,
    source: String,
}

/// Data categories with endpoints
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum Category {
    Dump,
    State,
    Position,
    Sensor,
}

impl Category {
    fn endpoint<'a>(&self, config: &'a Config) -> Option<&'a str> {
        match self {
            Category::State => Some(&config.state_endpoint),
            Category::Sensor => Some(&config.sensor_endpoint),
            Category::Position => Some(&config.position_endpoint),
            Category::Dump => None,
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
                Regex::new(r"^(propulsion\.)([^.]+)(\..*(diagnostic|status|raw|error|fault).*)$").unwrap()
            ],
            state: vec![
                Regex::new(r"^(.*)((m|M)ode)$").unwrap(),
                Regex::new(r"^(.*)\.(name)$").unwrap(),
                Regex::new(r"^(.*)\.(state)$").unwrap(),
                Regex::new(r"^(propulsion\.)([^.]+)(\.(gear|tiltTrim|runTime))$").unwrap()
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
                Regex::new(r"^(propulsion)(.*)$").unwrap(),                
            ],
        }
    }
}

/// Categorize data based on Signal K path
fn categorize_data(path: &str) -> Category {
    for pattern in &CATEGORY_PATTERNS.dump {
        if pattern.is_match(path) {
            return Category::Dump;
        }
    }

    for pattern in &CATEGORY_PATTERNS.state {
        if pattern.is_match(path) {
            return Category::State;
        }
    }

    for pattern in &CATEGORY_PATTERNS.position {
        if pattern.is_match(path) {
            return Category::Position;
        }
    }

    for pattern in &CATEGORY_PATTERNS.sensor {
        if pattern.is_match(path) {
            return Category::Sensor;
        }
    }

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

/// Buffered data item with metadata
#[derive(Debug, Serialize, Deserialize, Clone)]
struct BufferedItem {
    id: Uuid,
    category: String,
    data: Vec<MeasurementData>,
    timestamp: u64,
    size_bytes: usize,
}

/// Durable buffer implementation with memory + disk storage
struct DurableBuffer {
    memory_buffer: HashMap<Category, Vec<BufferedItem>>,
    memory_usage: usize,
    max_memory_bytes: usize,
    buffer_dir: PathBuf,
    disk_files: HashMap<Category, BufWriter<File>>,
    pending_acks: HashMap<Uuid, BufferedItem>,
}

impl DurableBuffer {
    fn new(config: &Config) -> io::Result<Self> {
        // Create buffer directory
        std::fs::create_dir_all(&config.buffer_directory)?;

        let mut disk_files = HashMap::new();

        // Create disk buffer files for each category
        for category in [Category::State, Category::Sensor, Category::Position] {
            let file_path = config.buffer_directory.join(format!("{:?}_buffer.bin", category));
            let file = OpenOptions::new()
              .create(true)
              .append(true)
              .open(file_path)?;
            disk_files.insert(category, BufWriter::new(file));
        }

        Ok(Self {
            memory_buffer: HashMap::new(),
            memory_usage: 0,
            max_memory_bytes: config.max_memory_buffer_bytes,
            buffer_dir: config.buffer_directory.clone(),
            disk_files,
            pending_acks: HashMap::new(),
        })
    }

    /// Add data to the buffer, spilling to disk if memory limit exceeded
    fn add_batch(&mut self, category: Category, data: Vec<MeasurementData>) -> io::Result<()> {
        if category == Category::Dump {
            return Ok(());
        }

        let json_data = serde_json::to_string(&data)?;
        let item = BufferedItem {
            id: Uuid::new_v4(),
            category: format!("{:?}", category),
            data,
            timestamp: SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64,
            size_bytes: json_data.len(),
        };

        // Check if we need to spill to disk
        if self.memory_usage + item.size_bytes > self.max_memory_bytes {
            self.spill_to_disk(category)?;
        }

        // Add to memory buffer
        self.memory_buffer.entry(category).or_default().push(item.clone());
        self.memory_usage += item.size_bytes;

        debug!("Added batch of {} items to buffer (category: {:?}, memory usage: {} bytes)",
               item.data.len(), category, self.memory_usage);

        Ok(())
    }

    /// Spill oldest items to disk to free memory
    fn spill_to_disk(&mut self, category: Category) -> io::Result<()> {
        if let Some(items) = self.memory_buffer.get_mut(&category) {
            if let Some(writer) = self.disk_files.get_mut(&category) {
                // Take half the items and write to disk
                let split_at = items.len() / 2;
                let to_disk: Vec<BufferedItem> = items.drain(..split_at).collect();

                for item in &to_disk {
                    let serialized = serde_json::to_vec(&item)
                      .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
                    writer.write_u32::<BigEndian>(serialized.len() as u32)?;
                    writer.write_all(&serialized)?;

                    self.memory_usage = self.memory_usage.saturating_sub(item.size_bytes);
                }

                writer.flush()?;
                info!("Spilled {} items to disk for category {:?}", to_disk.len(), category);
            }
        }
        Ok(())
    }

    /// Get next batch to send, preferring memory over disk
    fn get_batch(&mut self, category: Category, max_size_bytes: usize) -> io::Result<Option<BufferedItem>> {
        // First try memory buffer
        if let Some(items) = self.memory_buffer.get_mut(&category) {
            if let Some(item) = items.pop() {
                self.memory_usage = self.memory_usage.saturating_sub(item.size_bytes);
                self.pending_acks.insert(item.id, item.clone());
                return Ok(Some(item));
            }
        }

        // Then try disk buffer
        self.load_from_disk(category, max_size_bytes)
    }

    /// Load items from disk buffer
    fn load_from_disk(&mut self, category: Category, _max_size_bytes: usize) -> io::Result<Option<BufferedItem>> {
        let file_path = self.buffer_dir.join(format!("{:?}_buffer.bin", category));

        if !file_path.exists() || file_path.metadata()?.len() == 0 {
            return Ok(None);
        }

        let mut reader = BufReader::new(File::open(&file_path)?);
        let mut temp_items = Vec::new();
        let mut found_item = None;

        // Read all items, taking the first one and keeping the rest
        loop {
            match reader.read_u32::<BigEndian>() {
                Ok(len) => {
                    let mut buffer = vec![0u8; len as usize];
                    reader.read_exact(&mut buffer)?;

                    let item: BufferedItem = serde_json::from_slice(&buffer)
                      .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;

                    if found_item.is_none() {
                        self.pending_acks.insert(item.id, item.clone());
                        found_item = Some(item);
                    } else {
                        temp_items.push(item);
                    }
                }
                Err(ref e) if e.kind() == io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(e),
            }
        }

        // Rewrite file with remaining items
        let mut writer = BufWriter::new(OpenOptions::new()
          .write(true)
          .truncate(true)
          .open(&file_path)?);

        for item in temp_items {
            let serialized = serde_json::to_vec(&item)
              .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
            writer.write_u32::<BigEndian>(serialized.len() as u32)?;
            writer.write_all(&serialized)?;
        }
        writer.flush()?;

        Ok(found_item)
    }

    /// Acknowledge successful transmission
    fn ack(&mut self, id: Uuid) {
        if let Some(item) = self.pending_acks.remove(&id) {
            debug!("Acknowledged successful transmission of batch {}", id);
        }
    }

    /// Handle failed transmission by putting item back in buffer
    fn nack(&mut self, id: Uuid) {
        if let Some(item) = self.pending_acks.remove(&id) {
            warn!("Failed transmission of batch {}, returning to buffer", id);
            if let Ok(category) = item.category.as_str().try_into() {
                // Put back at front of memory buffer
                let category: Category = match category {
                    "State" => Category::State,
                    "Sensor" => Category::Sensor,
                    "Position" => Category::Position,
                    _ => return,
                };

                self.memory_buffer.entry(category).or_default().insert(0, item.clone());
                self.memory_usage += item.size_bytes;
            }
        }
    }

    fn flush_all(&mut self) -> io::Result<()> {
        for writer in self.disk_files.values_mut() {
            writer.flush()?;
        }
        Ok(())
    }
}

/// HTTP sink for sending batched data
struct HttpSink {
    client: Client,
    config: Config,
    buffer: Arc<Mutex<DurableBuffer>>,
}

impl HttpSink {
    fn new(config: Config, buffer: Arc<Mutex<DurableBuffer>>) -> Self {
        let client = Client::builder()
          .timeout(Duration::from_secs(config.request_timeout_secs))
          .build()
          .expect("Failed to create HTTP client");

        Self { client, config, buffer }
    }

    /// Start the HTTP sink task
    async fn start(self: Arc<Self>) {
        let mut interval = interval(Duration::from_secs(self.config.batch_timeout_secs));

        loop {
            interval.tick().await;

            // Process each category
            for category in [Category::State, Category::Sensor, Category::Position] {
                if let Some(endpoint) = category.endpoint(&self.config) {
                    self.process_category(category, endpoint).await;
                }
            }
        }
    }

    async fn process_category(&self, category: Category, endpoint: &str) {
        let batch = {
            let mut buffer = self.buffer.lock().unwrap();
            match buffer.get_batch(category, self.config.max_batch_size_bytes) {
                Ok(batch) => batch,
                Err(e) => {
                    error!("Error getting batch from buffer: {}", e);
                    return;
                }
            }
        };

        if let Some(batch_item) = batch {
            match self.send_batch(endpoint, &batch_item).await {
                Ok(_) => {
                    let mut buffer = self.buffer.lock().unwrap();
                    buffer.ack(batch_item.id);
                }
                Err(e) => {
                    error!("Failed to send batch: {}", e);
                    let mut buffer = self.buffer.lock().unwrap();
                    buffer.nack(batch_item.id);
                }
            }
        }
    }

    async fn send_batch(&self, endpoint: &str, batch: &BufferedItem) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        // Serialize and compress the batch data
        let json_data = serde_json::to_string(&batch.data)?;
        let compressed_data = zstd::encode_all(json_data.as_bytes(), 3)?;

        // Send HTTP request with raw compressed data
        let response = timeout(
            Duration::from_secs(self.config.request_timeout_secs),
            self.client
              .post(endpoint)
              .header("Authorization", format!("Bearer {}", self.config.auth_token))
              .header("Content-Type", "application/zstd")
              .body(compressed_data.clone())
              .send()
        ).await??;

        if response.status().is_success() {
            info!("Successfully sent batch of {} items ({} bytes compressed) to {}",
                  batch.data.len(), compressed_data.len(), endpoint);
            Ok(())
        } else {
            Err(format!("HTTP error {}: {}", response.status(), response.text().await?).into())
        }
    }
}

/// Parse timestamp from various formats to milliseconds
fn parse_timestamp(timestamp: Option<&str>) -> u64 {
    if let Some(ts_str) = timestamp {
        if let Ok(dt) = DateTime::parse_from_rfc3339(ts_str) {
            return dt.timestamp_millis() as u64;
        }
        if let Ok(ts_num) = ts_str.parse::<f64>() {
            return if ts_num > 1e12 {
                ts_num as u64
            } else {
                (ts_num * 1000.0) as u64
            };
        }
    }

    SystemTime::now()
      .duration_since(UNIX_EPOCH)
      .unwrap()
      .as_millis() as u64
}

/// Check if position has zero lat/lon
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

/// Batch processor for collecting data before sending to buffer
struct BatchProcessor {
    state_batch: Vec<MeasurementData>,
    sensor_batch: Vec<MeasurementData>,
    position_batch: Vec<MeasurementData>,
    buffer: Arc<Mutex<DurableBuffer>>,
}

impl BatchProcessor {
    fn new(buffer: Arc<Mutex<DurableBuffer>>) -> Self {
        Self {
            state_batch: Vec::new(),
            sensor_batch: Vec::new(),
            position_batch: Vec::new(),
            buffer,
        }
    }

    fn add_data(&mut self, data: MeasurementData, category: Category, config: &Config) -> io::Result<()> {
        match category {
            Category::State => {
                self.state_batch.push(data);
                if self.state_batch.len() >= category.batch_size(config) {
                    self.flush_batch(Category::State, config)?;
                }
            }
            Category::Sensor => {
                self.sensor_batch.push(data);
                if self.sensor_batch.len() >= category.batch_size(config) {
                    self.flush_batch(Category::Sensor, config)?;
                }
            }
            Category::Position => {
                self.position_batch.push(data);
                if self.position_batch.len() >= category.batch_size(config) {
                    self.flush_batch(Category::Position, config)?;
                }
            }
            Category::Dump => {} // Ignored
        }
        Ok(())
    }

    fn flush_batch(&mut self, category: Category, _config: &Config) -> io::Result<()> {
        let batch = match category {
            Category::State => std::mem::take(&mut self.state_batch),
            Category::Sensor => std::mem::take(&mut self.sensor_batch),
            Category::Position => std::mem::take(&mut self.position_batch),
            Category::Dump => return Ok(()),
        };

        if batch.is_empty() {
            return Ok(());
        }

        // Send to durable buffer
        {
            let mut buffer = self.buffer.lock().unwrap();
            buffer.add_batch(category, batch.clone())?;
        }

        debug!("Flushed {} {} messages to buffer", batch.len(), match category {
            Category::State => "state",
            Category::Sensor => "sensor",
            Category::Position => "position",
            Category::Dump => "dump",
        });

        Ok(())
    }

    fn flush_all(&mut self, config: &Config) -> io::Result<()> {
        self.flush_batch(Category::State, config)?;
        self.flush_batch(Category::Sensor, config)?;
        self.flush_batch(Category::Position, config)?;
        Ok(())
    }
}

fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();
    let rt = tokio::runtime::Runtime::new()?;
    rt.block_on(async_main())
}
async fn async_main() -> anyhow::Result<()> {

    // Load configuration
    let config = Config::from_env();
    info!("Loaded config: {:?}", config);

    // Load zstd dictionary (optional, not used in current code)
    let _zstd_dict = load_dictionary();

    // Create durable buffer
    let buffer = Arc::new(Mutex::new(DurableBuffer::new(&config)?));

    // Create batch processor
    let mut batch_processor = BatchProcessor::new(buffer.clone());

    // Create HTTP sink and spawn its task
    let http_sink = Arc::new(HttpSink::new(config.clone(), buffer.clone()));
    tokio::spawn(http_sink.clone().start());

    // Connect to Signal K websocket
    let (ws_stream, _) = connect_async(&config.websocket_url).await?;
    info!("Connected to Signal K websocket at {}", config.websocket_url);

    let mut rate_limiter = RateLimiter::new();
    let (mut write, mut read) = ws_stream.split();

    // Process incoming websocket messages
    while let Some(msg) = read.next().await {
        match msg {
            Ok(Message::Text(text)) => {
                match serde_json::from_str::<SignalKDelta>(&text) {
                    Ok(delta) => {
                        if let Some(updates) = delta.updates {
                            for update in updates {
                                let source = update.source.as_ref().and_then(|s| s.label.clone()).unwrap_or_default();
                                for value in update.values {
                                    let category = categorize_data(&value.path);
                                    if category == Category::Dump {
                                        continue;
                                    }
                                    if !rate_limiter.should_transmit(&value.path, category, &config) {
                                        continue;
                                    }
                                    if category == Category::Position && is_zero_position(&value.value) {
                                        continue;
                                    }
                                    let time = parse_timestamp(value.timestamp.as_deref().or(update.timestamp.as_deref()));
                                    let data = MeasurementData {
                                        path: value.path.clone(),
                                        time,
                                        value: value.value.clone(),
                                        source: source.clone(),
                                    };
                                    if let Err(e) = batch_processor.add_data(data, category, &config) {
                                        error!("Failed to add data to batch: {}", e);
                                    }
                                }
                            }
                        }
                    }
                    Err(e) => warn!("Failed to parse Signal K delta: {}", e),
                }
            }
            Ok(Message::Ping(_)) => {
                // Optionally handle ping
            }
            Ok(Message::Close(_)) => {
                info!("Websocket closed");
                break;
            }
            Err(e) => {
                error!("Websocket error: {}", e);
                break;
            }
            _ => {}
        }
    }

    // Flush remaining batches and buffer
    batch_processor.flush_all(&config)?;
    buffer.lock().unwrap().flush_all()?;

    Ok(())
}
