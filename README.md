# signalkCompressor

Reads a [Signal K](https://signalk.org/) delta stream from a boat's onboard server, throttles and
categorises it, and ships it to an HTTP ingest API as zstd-compressed batches — with a buffer that
holds data when the link drops and hands it back on failure rather than losing it.

It runs on a Raspberry Pi aboard a sailing yacht, on an intermittent 4G/satellite link, feeding a
vessel-tracking site. The interesting constraint is the link, not the volume: a marine connection is
offline more often than it is online, so the design is about surviving that without either dropping
measurements or filling the Pi's disk.

```
Signal K server ──WebSocket──> signalkCompressor ──HTTPS (zstd)──> ingest API ─> NATS ─> TimescaleDB
   (onboard)                      (this repo, on the Pi)
```

This is one of three components; the API and database writer live in separate repositories.

## What it does

**Categorises every path.** Each incoming Signal K path is matched against ordered regex lists into
one of four categories, which then decides its endpoint, batch size and sample rate:

| category | meaning | default sample rate | default batch |
| --- | --- | --- | --- |
| `Position` | `navigation.position` | 1000 ms | 250 |
| `Sensor` | continuously varying measurements | 2000 ms | 500 |
| `State` | discrete/slow values — modes, names, battery SoC, notifications | 5000 ms | 20 |
| `Dump` | explicitly discarded | — | — |

`Dump` is both a blocklist **and the fallback for anything unmatched**, so a Signal K path that no
`sensor`/`state`/`position` pattern matches is silently discarded. That is deliberate — a marine bus
emits a great deal of diagnostic chatter nobody wants to pay to transmit — but it means adding a new
measurement means adding a pattern.

**Throttles per path.** A `RateLimiter` keeps the last transmission time per path and drops samples
arriving inside the category's sample rate. This is what makes the data volume a function of path
count rather than of bus traffic.

**Sends only changes, for `State`.** `State` batches are deduplicated against the last value sent
for each path, so a value that hasn't moved isn't retransmitted. `Sensor` and `Position` are
rate-limited but not deduplicated — they emit one sample per path per interval whether the value
moved or not.

**Batches and compresses.** Batches flush on size, byte budget or timeout, and go out as **zstd
level 3 over a JSON array** with `Content-Type: application/zstd` and a bearer token:

```json
[{"path": "environment.depth.belowTransducer", "time": 1760126604376, "value": 12.4, "source": "canbus"}]
```

`time` is epoch milliseconds. Compression typically pays for itself many times over on this data —
the paths repeat on every sample, which is exactly what zstd's dictionary matching eats.

**Buffers with ack/nack.** `DurableBuffer` holds pending batches in memory and returns a failed
batch to the *front* of its queue, so an outage doesn't reorder or lose data. Past
`MAX_MEMORY_BUFFER_BYTES` it spills to length-prefixed files under `BUFFER_DIRECTORY`.

A real limitation, stated plainly: **the buffer does not survive a restart.** Spill-to-disk only
triggers past the memory ceiling (1 GB by default, and days of data is only megabytes), there is no
signal handler, and startup clears the existing state buffer file. Restarting the process discards
whatever it was holding. If you are operating this, drain it before restarting, or raise the
spill threshold so the on-disk path is actually exercised.

## Configuration

Everything is environment-driven, read once at startup, straight from the process environment —
there is no config file and no `.env` loading.

| variable | default | notes |
| --- | --- | --- |
| `SENSOR_ENDPOINT` | **required** | startup fails if unset |
| `POSITION_ENDPOINT` | **required** | startup fails if unset |
| `STATE_ENDPOINT` | **required** | startup fails if unset |
| `SIGNALK_WEBSOCKET_URL` | `ws://10.101.0.3:3000/signalk/v1/stream` | onboard Signal K server |
| `AUTH_TOKEN` | `NO_TOKEN` | HS256 JWT, sent as `Authorization: Bearer` |
| `SENSOR_SAMPLE_RATE` / `POSITION_SAMPLE_RATE` / `STATE_SAMPLE_RATE` | 2000 / 1000 / 5000 ms | per-path throttle |
| `SENSOR_BATCH_SIZE` / `POSITION_BATCH_SIZE` / `STATE_BATCH_SIZE` | 500 / 250 / 20 | measurements per POST |
| `MAX_BATCH_SIZE_BYTES` | 1 MiB | byte budget per batch |
| `BATCH_TIMEOUT_SECS` | 5 | flush a partial batch after this |
| `MAX_MEMORY_BUFFER_BYTES` | 1 GiB | spill-to-disk threshold |
| `BUFFER_DIRECTORY` | `/var/lib/signalk_buffer` | spill location |
| `REQUEST_TIMEOUT_SECS` | 30 | per POST |

The three endpoints are required on purpose: there is no sensible default for "where this boat's
data goes", and a placeholder host would mean batches silently accumulating in the buffer against
something that will never answer. `AUTH_TOKEN`, by contrast, still defaults to `NO_TOKEN`, so a
missing token produces 401s rather than a startup error — check the logs, not the exit code.

The startup log prints the resolved config with `auth_token` redacted, so it is safe to paste.

## Building

Nix is the supported path; `shell.nix` provides the toolchain (`rustup` plus clang/bindgen for
`zstd-sys`), pinned by `rust-toolchain.toml`.

```bash
nix-shell --run 'cargo build --release'
nix-shell --run 'cargo test'
```

`flake.nix` and `default.nix` package it for `aarch64-linux` only — the Pi — so `nix build` on an
x86_64 host fails by design; use the shell. `.cargo/config.toml` sets an
`aarch64-unknown-linux-musl` linker for cross-compiling to the Pi from a workstation.

Plain `cargo build --release` works too if you have Rust ≥ 1.85 (the crate is edition 2024) and a C
toolchain for `zstd-sys`.

## Running

Long-running process; restart it under a supervisor. Note the buffer caveat above before configuring
automatic restarts — `Restart=always` on a bad config will loop and throw away buffered data each
time.

```bash
SENSOR_ENDPOINT=https://example.invalid/sensor_data \
POSITION_ENDPOINT=https://example.invalid/position_data \
STATE_ENDPOINT=https://example.invalid/state_data \
AUTH_TOKEN=<jwt> \
  ./target/release/signalkCompressor
```

Supply the token out of band rather than inline — a systemd `Environment=` value is readable via
`systemctl show`, and on NixOS it also lands world-readable in the Nix store. Use an
`EnvironmentFile` outside the store, or a secrets manager such as sops-nix or agenix.

```bash
```

Logging is `tracing`; set `RUST_LOG` to taste.

## Status

Works, and has been in continuous use aboard one boat for over a year. It is not a general-purpose
tool: the category regexes encode one vessel's instrument set, the receiving API is assumed to
accept this specific wire format, and there is no reconnect/backoff tuning beyond what is in the
code. Read it as a worked example of the problem rather than a library.
