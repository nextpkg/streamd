# Streamd

Streamd is a pure-Go streaming service with RTMP publishing and playback, HTTP-FLV, HLS, recording, and live configuration reload.

## Features

- RTMP origin pulling, static push/pull, and Redis-backed stream discovery.
- HTTP-FLV delivery and HLS playlists with configurable segment duration, window size, and signed URLs.
- FLV and MPEG-TS recording to the filesystem, S3-compatible storage, or COS.
- YAML or JSON configuration with file watching and SIGHUP reload.
- Management endpoints, lifecycle callbacks, health checks, and Prometheus metrics.

## Quick start

Build with Go 1.26 or newer:

```sh
make build
./bin/streamd -check-config -config streamd.yaml
./bin/streamd -config streamd.yaml
```

The example configuration binds RTMP to `127.0.0.1:1935` and HTTP to `127.0.0.1:8080`, enables HLS, and leaves recording disabled.

Publish to `rtmp://127.0.0.1:1935/live/sample`, then play:

- RTMP: `rtmp://127.0.0.1:1935/live/sample`
- HTTP-FLV: `http://127.0.0.1:8080/hfs/live/sample.flv`
- HLS: `http://127.0.0.1:8080/hls/live/sample.m3u8`

Health and readiness are available at `/healthz` and `/readyz`; metrics are available at `/metrics`.

## Design

See the [design guide](docs/README.md) for architecture diagrams, memory ownership, and measured performance comparisons.

Each RTMP connection uses a slab allocator for incoming media. Codec headers and GOP caches provide playback startup data, while consumers share media payloads and maintain their own delivery state. Bounded queues isolate slow consumers. Publisher takeover can retain connected viewers, and origin connections are reused for concurrent playback requests.

HLS stores completed segments as immutable byte snapshots and retains recently issued segments for a bounded grace period. Recording batches writes at keyframes and finalizes detached segments asynchronously with bounded concurrency. Storage inspection uses delayed scheduling and bounded workers to keep maintenance separate from live delivery.

Configuration reload validates candidates before committing them. Invalid candidates and listener-address changes leave the active configuration intact. HLS and recording updates apply at video keyframes; `/admin/config` reports the configuration revision and per-stream component state. `STREAMD_HTTP_ADDR`, `STREAMD_RTMP_ADDR`, and `STREAMD_LOG_LEVEL` override file values.

Redis discovery uses `/streamd/{app}/{stream}` keys with TTL renewal, notifications, and polling. All participating nodes and external registry writers must use the same namespace; `finder.separator` can change the path separator.

Lifecycle callbacks are asynchronous, best-effort HTTP requests without a persistent delivery queue. Recording recovery requires valid persisted data and checkpoints; the service does not provide automatic repair of ambiguous storage state or distributed writer ownership.

## Development

```sh
make test
make race
make check
```

Production builds use `CGO_ENABLED=0`; the race detector requires a C toolchain. FFmpeg and ffprobe are independent test tools, not runtime dependencies.

Install FFmpeg/ffprobe, then run `make verify` for unit tests, static checks, and the core media suite (streaming, reload, republish, recording resume). `make e2e-core` runs only that suite; it stops on failure and writes a summary with per-scenario evidence paths. Fixtures are generated automatically.

Use `make e2e-list` to list all scenarios, and `make e2e-<scenario>` for targeted checks. Docker scenarios accept `STREAMD_DOCKER_CONTEXT` (for example, `orbstack`); object-storage and release scenarios may require additional provider/image settings. Existing scenario commands remain available. Generated media and evidence are retained in the ignored `testdata/generated` and `artifacts` directories for inspection.

## License

MIT. See [LICENSE](LICENSE).
