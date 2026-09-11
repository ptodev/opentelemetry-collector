# trace-debug

A local-only setup for inspecting unusually large traces on a developer
laptop — the ones a production trace backend would normally reject or
truncate.

It's a purpose-built Collector, not a generic one: OTLP receiver → tail
sampling that keeps only traces at least `min_bytes` large (exact, not
approximate) → local Tempo.

## Why tail sampling instead of custom aggregation code

Tail sampling already does the hard part (buffering a trace until it's
complete, then deciding whether to keep it) and stays open to combining
this size check with other policies (`composite`, `and`) later, e.g. only
keep large traces for a specific service. `tailsamplingprocessor` doesn't
ship a "minimum total trace byte size" policy, but it does have a
supported extension point for custom policies
(`pkg/samplingpolicy.Extension`) — see
[extension/tracesizepolicy](extension/tracesizepolicy). It reads the exact
`TraceData.SizeBytes` the processor already tracks as spans arrive, so
there's no approximation via span count and no need to marshal traces
ourselves.

## One-time build

Requires Go and the OpenTelemetry Collector Builder:

```console
cd dev/trace-debug
go run go.opentelemetry.io/collector/cmd/builder@v0.158.0 --config=builder-config.yaml
```

This produces `./dist/trace-debug-collector`.

## Run it

1. Start Tempo (and a Grafana with Tempo pre-provisioned as a datasource):

   ```console
   docker compose up -d
   ```

2. Start the collector:

   ```console
   ./dist/trace-debug-collector --config=collector-config.yaml
   ```

3. Point your app's OTLP exporter at `localhost:4317` (gRPC) or
   `localhost:4318` (HTTP) instead of your usual backend.

4. Browse traces at <http://localhost:3000> (Grafana, Tempo datasource
   pre-provisioned, anonymous admin login).

Only traces whose total encoded size reaches `min_bytes` (default 5 MB, set
in `collector-config.yaml`) are forwarded to Tempo — everything smaller is
dropped after the sampling decision. `debugexporter` also logs a summary of
what was sampled, useful for confirming the filter is doing what you
expect.

## Adjusting the threshold or decision window

Edit `collector-config.yaml`:

- `tail_sampling.policies[0].tracesizepolicy.min_bytes` — the size floor.
- `tail_sampling.decision_wait` — how long to buffer a trace before
  deciding. Raise this if your large traces take longer than 10s to fully
  arrive; traces still being received when this expires are decided on
  whatever data has arrived so far.

## Layout

- `extension/tracesizepolicy/` — the custom `tracesizepolicy` sampling
  policy, its own Go module (its own tests: `go test ./...` from that
  directory).
- `builder-config.yaml` — OCB manifest wiring the extension into a minimal
  Collector build alongside `otlpreceiver`, `tailsamplingprocessor`, and
  `otlpexporter`.
- `collector-config.yaml` — the Collector's runtime config.
- `docker-compose.yaml`, `tempo.yaml`, `grafana/` — local Tempo + Grafana.
