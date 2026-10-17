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
#[derive(Clone)]
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

/// Hand-written so that `auth_token` is never printed. The config is logged at startup, and a
/// bearer token in the journal is a credential leak - the API that receives it had the same bug.
impl std::fmt::Debug for Config {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Config")
            .field("websocket_url", &self.websocket_url)
            .field("sensor_endpoint", &self.sensor_endpoint)
            .field("position_endpoint", &self.position_endpoint)
            .field("state_endpoint", &self.state_endpoint)
            .field("auth_token", &format_args!("<redacted, {} bytes>", self.auth_token.len()))
            .field("state_batch_size", &self.state_batch_size)
            .field("sensor_batch_size", &self.sensor_batch_size)
            .field("position_batch_size", &self.position_batch_size)
            .field("state_sample_rate", &self.state_sample_rate)
            .field("sensor_sample_rate", &self.sensor_sample_rate)
            .field("position_sample_rate", &self.position_sample_rate)
            .field("max_batch_size_bytes", &self.max_batch_size_bytes)
            .field("batch_timeout_secs", &self.batch_timeout_secs)
            .field("max_memory_buffer_bytes", &self.max_memory_buffer_bytes)
            .field("buffer_directory", &self.buffer_directory)
            .field("request_timeout_secs", &self.request_timeout_secs)
            .field("max_concurrent_requests", &self.max_concurrent_requests)
            .finish()
    }
}

impl Config {
    /// Reads configuration from the process environment.
    ///
    /// The three ingest endpoints are **required**: there is no sensible default for "where this
    /// boat's data goes", and a placeholder would mean batches piling up in the buffer against a
    /// host that will never answer. Everything else has a working default.
    fn from_env() -> anyhow::Result<Self> {
        fn require(name: &str) -> anyhow::Result<String> {
            env::var(name).map_err(|_| {
                anyhow::anyhow!(
                    "{name} is not set - signalkCompressor needs SENSOR_ENDPOINT, \
                     POSITION_ENDPOINT and STATE_ENDPOINT pointing at the ingest API"
                )
            })
        }

        Ok(Self {
            state_batch_size: env::var("STATE_BATCH_SIZE")
              .unwrap_or_else(|_| "20".to_string())
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
            sensor_endpoint: require("SENSOR_ENDPOINT")?,
            position_endpoint: require("POSITION_ENDPOINT")?,
            state_endpoint: require("STATE_ENDPOINT")?,
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
        })
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
                Regex::new(r"^(propulsion\.)([^.]+)(\..*(diagnostic|status|raw|error|fault).*)$").unwrap(),
                // Per-satellite SNR/azimuth/elevation for the whole constellation: ~1.9 KB of
                // JSON, 30x the average row, and receiver diagnostics nothing in SerenSite reads.
                // It matches the `navigation.gns` State pattern below, but State's write-on-change
                // dedupe never fires on it because the SNRs move every sample - so it shipped
                // ~2560 rows/day. It is also what broke TimescaleDB compression for two months
                // (jsonb too large for a btree minmax index); see ops/compression-fix.md.
                Regex::new(r"^(navigation\.gnss\.satellitesInView)$").unwrap(),
            ],
            state: vec![
                Regex::new(r"^(.*)(([mM])ode)$").unwrap(),
                Regex::new(r"^(.*)\.(name)$").unwrap(),
                Regex::new(r"^(.*)\.(state)$").unwrap(),
                Regex::new(r"^(propulsion\.)([^.]+)(\.(gear|tiltTrim|runTime))$").unwrap(),
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

    /// Remove all buffered state data (memory and disk)
    fn clear_state_data(&mut self) -> io::Result<()> {
        // Clear in-memory state buffer
        self.memory_buffer.remove(&Category::State);

        // Remove state disk file
        let file_path = self.buffer_dir.join("State_buffer.bin");
        if file_path.exists() {
            std::fs::remove_file(&file_path)?;
            // Recreate empty file for future use
            let file = OpenOptions::new()
                .create(true)
                .append(true)
                .open(&file_path)?;
            self.disk_files.insert(Category::State, BufWriter::new(file));
        }
        Ok(())
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

    /// Spill the oldest items to disk to free memory
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

    /// Get next batch to send, preferring memory to disk
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

    /// Largest plausible spilled record. Without this bound a corrupt length
    /// prefix drives a multi-gigabyte `vec![0u8; len]` allocation.
    const MAX_RECORD_BYTES: u32 = 64 * 1024 * 1024;

    /// Read one length-prefixed record's raw bytes.
    ///
    /// `Ok(None)` is a clean end of file. `Err` means the framing is damaged -
    /// a truncated record, or a length that cannot be real - and the caller
    /// should treat everything from that point on as unrecoverable. This is
    /// framing-level only: the payload is not parsed, so compacting a large
    /// spill file does not deserialize it.
    fn read_frame(reader: &mut impl Read) -> io::Result<Option<Vec<u8>>> {
        let len = match reader.read_u32::<BigEndian>() {
            Ok(len) => len,
            // Also covers a 1-3 byte trailing fragment, which byteorder reports
            // as UnexpectedEof. Those bytes cannot be a record, so drop them.
            Err(ref e) if e.kind() == io::ErrorKind::UnexpectedEof => return Ok(None),
            Err(e) => return Err(e),
        };

        if len == 0 || len > Self::MAX_RECORD_BYTES {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("implausible spill record length {len}"),
            ));
        }

        let mut buf = vec![0u8; len as usize];
        match reader.read_exact(&mut buf) {
            Ok(()) => Ok(Some(buf)),
            // The bug this guards: read_exact answers "failed to fill whole
            // buffer" on a half-written record, and the old code propagated it,
            // returning before the compaction below could drop the bad tail. The
            // file stayed damaged, so every poll hit it again - one truncated
            // record wedged 484 MB of good data for ten months.
            Err(ref e) if e.kind() == io::ErrorKind::UnexpectedEof => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("truncated spill record: wanted {len} bytes, file ended first"),
            )),
            Err(e) => Err(e),
        }
    }

    /// Take the next item from the disk buffer, compacting the file as it goes.
    ///
    /// Damaged framing is survivable: the records before it are kept, the
    /// remainder is discarded, and the file is rewritten so the fault clears
    /// itself on the first read rather than recurring forever.
    fn load_from_disk(&mut self, category: Category, _max_size_bytes: usize) -> io::Result<Option<BufferedItem>> {
        let file_path = self.buffer_dir.join(format!("{:?}_buffer.bin", category));

        if !file_path.exists() || file_path.metadata()?.len() == 0 {
            return Ok(None);
        }

        let mut reader = BufReader::new(File::open(&file_path)?);
        let mut corrupt: Option<String> = None;

        // The item being returned to the caller.
        let found_item = match Self::read_frame(&mut reader) {
            Ok(Some(bytes)) => match serde_json::from_slice::<BufferedItem>(&bytes) {
                Ok(item) => Some(item),
                Err(e) => {
                    // Framing is intact but this is not a BufferedItem - e.g. a
                    // file written by an older build. Nothing after it can be
                    // trusted either, so drop the lot instead of looping.
                    corrupt = Some(format!("undecodable record: {e}"));
                    None
                }
            },
            Ok(None) => None,
            Err(e) => {
                corrupt = Some(e.to_string());
                None
            }
        };

        // Copy whatever still frames correctly to a sibling file, then swap it
        // in. Raw byte copy, one record at a time: constant memory regardless of
        // how large the spill has grown.
        let tmp_path = file_path.with_extension("bin.compacting");
        let mut kept = 0usize;
        let mut kept_bytes = 0u64;

        if corrupt.is_none() {
            let mut writer = BufWriter::new(File::create(&tmp_path)?);
            loop {
                match Self::read_frame(&mut reader) {
                    Ok(Some(bytes)) => {
                        writer.write_u32::<BigEndian>(bytes.len() as u32)?;
                        writer.write_all(&bytes)?;
                        kept += 1;
                        kept_bytes += 4 + bytes.len() as u64;
                    }
                    Ok(None) => break,
                    Err(e) => {
                        corrupt = Some(e.to_string());
                        break;
                    }
                }
            }
            writer.flush()?;
        } else {
            File::create(&tmp_path)?;
        }

        if let Some(reason) = &corrupt {
            warn!(
                "Discarding damaged tail of {:?} spill file ({}); kept {} intact record(s)",
                category, reason, kept
            );
        }

        std::fs::rename(&tmp_path, &file_path)?;

        // The retained append handle points at the inode that rename() just
        // replaced, so further spills would vanish into an unlinked file.
        let file = OpenOptions::new().create(true).append(true).open(&file_path)?;
        self.disk_files.insert(category, BufWriter::new(file));

        if let Some(item) = &found_item {
            self.pending_acks.insert(item.id, item.clone());
        }

        debug!(
            "Loaded batch from {:?} spill file; {} record(s) / {} bytes remain",
            category, kept, kept_bytes
        );

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
    state_latest: HashMap<String, MeasurementData>,
    last_sent_state: HashMap<String, Value>,
    sensor_batch: Vec<MeasurementData>,
    position_batch: Vec<MeasurementData>,
    buffer: Arc<Mutex<DurableBuffer>>,
}

impl BatchProcessor {
    fn new(buffer: Arc<Mutex<DurableBuffer>>) -> Self {
        Self {
            state_batch: Vec::new(),
            state_latest: HashMap::new(),
            last_sent_state: HashMap::new(),
            sensor_batch: Vec::new(),
            position_batch: Vec::new(),
            buffer,
        }
    }

    fn add_data(&mut self, data: MeasurementData, category: Category, config: &Config) -> io::Result<()> {
        match category {
            Category::State => {
                self.state_latest.insert(data.path.clone(), data.clone());
                self.state_batch.push(data);
                if self.state_batch.len() >= category.batch_size(config) {
                    self.flush_batch(Category::State, config)?;
                }
            }
            Category::Sensor => {
                self.sensor_batch.push(data);
                if self.sensor_batch.len() >= category.batch_size(config) {
                    self.flush_batch(Category::Sensor, config)?;
                    self.flush_batch(Category::State, config)?;
                }
            }
            Category::Position => {
                self.position_batch.push(data);
                if self.position_batch.len() >= category.batch_size(config) {
                    self.flush_batch(Category::Position, config)?;
                }
            }
            Category::Dump => {}
        }
        Ok(())
    }

    fn flush_batch(&mut self, category: Category, _config: &Config) -> io::Result<()> {
        match category {
            Category::State => {
                let mut changed_states = Vec::new();
                for (path, data) in &self.state_latest {
                    let last = self.last_sent_state.get(path);
                    if last != Some(&data.value) {
                        changed_states.push(data.clone());
                    }
                }
                if !changed_states.is_empty() {
                    {
                        let mut buffer = self.buffer.lock().unwrap();
                        buffer.add_batch(Category::State, changed_states.clone())?;
                    }
                    for data in changed_states {
                        self.last_sent_state.insert(data.path.clone(), data.value.clone());
                    }
                }
                self.state_latest.clear();
            }
            Category::Sensor => {
                let batch = std::mem::take(&mut self.sensor_batch);
                if !batch.is_empty() {
                    let mut buffer = self.buffer.lock().unwrap();
                    buffer.add_batch(Category::Sensor, batch)?;
                }
            }
            Category::Position => {
                let batch = std::mem::take(&mut self.position_batch);
                if !batch.is_empty() {
                    let mut buffer = self.buffer.lock().unwrap();
                    buffer.add_batch(Category::Position, batch)?;
                }
            }
            Category::Dump => {}
        }
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
    let config = Config::from_env()?;
    info!("Loaded config: {:?}", config);

    // Load zstd dictionary (optional, not used in current code)
    let _zstd_dict = load_dictionary();

    // Create durable buffer
    let buffer = Arc::new(Mutex::new(DurableBuffer::new(&config)?));

    // Clear any existing state data
    buffer.lock().unwrap().clear_state_data()?;
    // Create batch processor
    let mut batch_processor = BatchProcessor::new(buffer.clone());

    // Create HTTP sink and spawn its task
    let http_sink = Arc::new(HttpSink::new(config.clone(), buffer.clone()));
    tokio::spawn(http_sink.clone().start());

    let mut rate_limiter = RateLimiter::new();
    let mut backoff = RECONNECT_BACKOFF_MIN;

    // Signal K going away (server restart, Pi network blip) must not end the process:
    // the buffer is RAM-only, so exiting would throw away everything not yet shipped.
    loop {
        match run_session(&config, &mut batch_processor, &mut rate_limiter).await {
            Ok(()) => backoff = RECONNECT_BACKOFF_MIN,
            Err(e) => error!("Signal K session failed: {}", e),
        }

        // Hand any partial batches to the buffer so the sink ships them while we are offline.
        if let Err(e) = batch_processor.flush_all(&config) {
            error!("Failed to flush batches after disconnect: {}", e);
        }
        if let Err(e) = buffer.lock().unwrap().flush_all() {
            error!("Failed to flush buffer after disconnect: {}", e);
        }

        warn!("Reconnecting to Signal K in {:?}", backoff);
        tokio::time::sleep(backoff).await;
        backoff = (backoff * 2).min(RECONNECT_BACKOFF_MAX);
    }
}

const RECONNECT_BACKOFF_MIN: Duration = Duration::from_secs(1);
const RECONNECT_BACKOFF_MAX: Duration = Duration::from_secs(30);
/// Signal K streams deltas continuously; this long without one means the socket is dead
/// even if no close or error ever arrived (e.g. the peer vanished without a FIN).
const WEBSOCKET_IDLE_TIMEOUT: Duration = Duration::from_secs(60);

/// Connect to Signal K and process deltas until the connection ends.
/// Returns `Ok` once a connection was established and then ended, so the caller can reset
/// its backoff; a failed connect is an `Err`.
async fn run_session(
    config: &Config,
    batch_processor: &mut BatchProcessor,
    rate_limiter: &mut RateLimiter,
) -> anyhow::Result<()> {
    let (ws_stream, _) = connect_async(&config.websocket_url).await?;
    info!("Connected to Signal K websocket at {}", config.websocket_url);

    let (_write, mut read) = ws_stream.split();

    loop {
        let msg = match timeout(WEBSOCKET_IDLE_TIMEOUT, read.next()).await {
            Ok(Some(msg)) => msg,
            Ok(None) => {
                info!("Websocket stream ended");
                break;
            }
            Err(_) => {
                warn!("No Signal K data for {:?}, dropping connection", WEBSOCKET_IDLE_TIMEOUT);
                break;
            }
        };
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
                                    if !rate_limiter.should_transmit(&value.path, category, config) {
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
                                    if let Err(e) = batch_processor.add_data(data, category, config) {
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

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Every pattern in `CategoryPatterns` is a `Regex::new(..).unwrap()` built at runtime, so a
    /// malformed one panics on startup rather than failing the build. Touching the lists forces
    /// them all to compile.
    #[test]
    fn all_category_patterns_compile() {
        let p = &*CATEGORY_PATTERNS;
        assert!(!p.dump.is_empty() && !p.state.is_empty() && !p.sensor.is_empty());
    }

    #[test]
    fn satellites_in_view_is_dumped() {
        // Dumped despite matching the `navigation.gns` State pattern: dump is tested first.
        assert_eq!(
            categorize_data("navigation.gnss.satellitesInView"),
            Category::Dump
        );
    }

    #[test]
    fn other_gnss_paths_are_still_state() {
        for path in [
            "navigation.gnss.satellites",
            "navigation.gnss.type",
            "navigation.gnss.methodQuality",
            "navigation.gnss.horizontalDilution",
            "navigation.gnss.geoidalSeparation",
        ] {
            assert_eq!(categorize_data(path), Category::State, "{path}");
        }
    }

    #[test]
    fn position_still_routes_to_position() {
        assert_eq!(categorize_data("navigation.position"), Category::Position);
    }
    // ---- DurableBuffer disk recovery ----
    //
    // A half-written record in a spill file used to make `load_from_disk`
    // return `failed to fill whole buffer` before it could repair the file, so
    // the same bad bytes were re-read forever. These cover the recovery.

    fn scratch_dir(tag: &str) -> PathBuf {
        let dir = env::temp_dir().join(format!("slk-buf-test-{}-{}", tag, Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn test_buffer(dir: &PathBuf) -> DurableBuffer {
        let mut disk_files = HashMap::new();
        for category in [Category::State, Category::Sensor, Category::Position] {
            let file = OpenOptions::new()
                .create(true)
                .append(true)
                .open(dir.join(format!("{:?}_buffer.bin", category)))
                .unwrap();
            disk_files.insert(category, BufWriter::new(file));
        }
        DurableBuffer {
            memory_buffer: HashMap::new(),
            memory_usage: 0,
            max_memory_bytes: 1 << 30,
            buffer_dir: dir.clone(),
            disk_files,
            pending_acks: HashMap::new(),
        }
    }

    fn item(timestamp: u64) -> BufferedItem {
        BufferedItem {
            id: Uuid::new_v4(),
            category: "Sensor".to_string(),
            data: Vec::new(),
            timestamp,
            size_bytes: 1,
        }
    }

    /// Write raw length-prefixed frames, optionally cutting the last one short
    /// the way a kill mid-write does.
    fn write_spill(path: &PathBuf, items: &[BufferedItem], truncate_last_to: Option<usize>) {
        let mut f = BufWriter::new(File::create(path).unwrap());
        for (i, it) in items.iter().enumerate() {
            let bytes = serde_json::to_vec(it).unwrap();
            let last = i + 1 == items.len();
            match truncate_last_to {
                Some(n) if last => {
                    // Full length prefix, short payload.
                    f.write_u32::<BigEndian>(bytes.len() as u32).unwrap();
                    f.write_all(&bytes[..n]).unwrap();
                }
                _ => {
                    f.write_u32::<BigEndian>(bytes.len() as u32).unwrap();
                    f.write_all(&bytes).unwrap();
                }
            }
        }
        f.flush().unwrap();
    }

    #[test]
    fn truncated_tail_does_not_wedge_the_spill_file() {
        let dir = scratch_dir("trunc");
        let path = dir.join("Sensor_buffer.bin");
        let items = vec![item(1), item(2), item(3), item(4)];
        write_spill(&path, &items, Some(10));

        let mut buf = test_buffer(&dir);

        // The three intact records come back in order, no error at any point.
        for expected in &items[..3] {
            let got = buf
                .load_from_disk(Category::Sensor, 1 << 20)
                .expect("damaged tail must not surface as an error");
            assert_eq!(got.expect("record should be present").timestamp, expected.timestamp);
        }

        // The truncated fourth is gone, and the file is empty rather than damaged.
        assert!(buf.load_from_disk(Category::Sensor, 1 << 20).unwrap().is_none());
        assert_eq!(std::fs::metadata(&path).unwrap().len(), 0);

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn truncated_first_record_clears_the_file() {
        let dir = scratch_dir("trunc-first");
        let path = dir.join("Sensor_buffer.bin");
        write_spill(&path, &[item(1)], Some(5));

        let mut buf = test_buffer(&dir);
        assert!(buf.load_from_disk(Category::Sensor, 1 << 20).unwrap().is_none());
        assert_eq!(std::fs::metadata(&path).unwrap().len(), 0);

        std::fs::remove_dir_all(&dir).ok();
    }

    /// A length prefix that cannot be real must be rejected on sight - the old
    /// code fed it straight to `vec![0u8; len]`.
    #[test]
    fn implausible_length_prefix_is_rejected_without_allocating() {
        let dir = scratch_dir("huge-len");
        let path = dir.join("Sensor_buffer.bin");
        {
            let mut f = BufWriter::new(File::create(&path).unwrap());
            f.write_u32::<BigEndian>(u32::MAX).unwrap();
            f.write_all(b"nowhere near that long").unwrap();
            f.flush().unwrap();
        }

        let mut buf = test_buffer(&dir);
        assert!(buf.load_from_disk(Category::Sensor, 1 << 20).unwrap().is_none());
        assert_eq!(std::fs::metadata(&path).unwrap().len(), 0);

        std::fs::remove_dir_all(&dir).ok();
    }

    /// Correct framing around something that is not a `BufferedItem` - e.g. a
    /// file left by an older build.
    #[test]
    fn undecodable_record_discards_the_file() {
        let dir = scratch_dir("undecodable");
        let path = dir.join("Sensor_buffer.bin");
        {
            let mut f = BufWriter::new(File::create(&path).unwrap());
            let junk = br#"{"not":"a buffered item"}"#;
            f.write_u32::<BigEndian>(junk.len() as u32).unwrap();
            f.write_all(junk).unwrap();
            f.flush().unwrap();
        }

        let mut buf = test_buffer(&dir);
        assert!(buf.load_from_disk(Category::Sensor, 1 << 20).unwrap().is_none());
        assert_eq!(std::fs::metadata(&path).unwrap().len(), 0);

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn intact_spill_file_round_trips_and_compacts() {
        let dir = scratch_dir("clean");
        let path = dir.join("Sensor_buffer.bin");
        let items = vec![item(10), item(20)];
        write_spill(&path, &items, None);
        let original_len = std::fs::metadata(&path).unwrap().len();

        let mut buf = test_buffer(&dir);

        let first = buf.load_from_disk(Category::Sensor, 1 << 20).unwrap().unwrap();
        assert_eq!(first.timestamp, 10);
        assert!(std::fs::metadata(&path).unwrap().len() < original_len);
        assert!(buf.pending_acks.contains_key(&first.id));

        let second = buf.load_from_disk(Category::Sensor, 1 << 20).unwrap().unwrap();
        assert_eq!(second.timestamp, 20);
        assert!(buf.load_from_disk(Category::Sensor, 1 << 20).unwrap().is_none());

        std::fs::remove_dir_all(&dir).ok();
    }

    /// After compaction the retained append handle must point at the live file,
    /// not the inode `rename` replaced, or later spills vanish silently.
    #[test]
    fn spilling_after_compaction_still_reaches_the_file() {
        let dir = scratch_dir("rename");
        let path = dir.join("Sensor_buffer.bin");
        write_spill(&path, &[item(1), item(2)], None);

        let mut buf = test_buffer(&dir);
        buf.load_from_disk(Category::Sensor, 1 << 20).unwrap().unwrap();

        // Force a spill through the handle held in `disk_files`.
        buf.memory_buffer
            .entry(Category::Sensor)
            .or_default()
            .extend([item(30), item(31)]);
        buf.spill_to_disk(Category::Sensor).unwrap();

        let on_disk = std::fs::metadata(&path).unwrap().len();
        assert!(on_disk > 0, "spill after compaction was written to an unlinked inode");

        // And it is readable back.
        let next = buf.load_from_disk(Category::Sensor, 1 << 20).unwrap().unwrap();
        assert!([2u64, 30].contains(&next.timestamp));

        std::fs::remove_dir_all(&dir).ok();
    }

}
