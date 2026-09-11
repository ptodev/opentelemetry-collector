# Trace tap extension

The `trace_tap` extension exposes an HTTP API to stream live trace data from
a running Collector's trace-facing pipeline boundaries, for local debugging.
It does not aggregate or filter data itself; it observes whatever traffic
reaches the boundary you tap, so use ordinary pipeline components (e.g.
`tail_sampling`) upstream of that boundary to decide what's worth tapping.

Every receiver/processor/exporter/connector automatically gets a tap at each
trace-facing input/output boundary, at near-zero cost when nothing is
streaming: with no active stream, observing a boundary costs one atomic
load and nothing else.

```yaml
extensions:
  trace_tap:
    endpoint: localhost:1777

service:
  extensions: [trace_tap]
```

The endpoint binds to loopback by default. Keep it on loopback and use a
port forward, or configure the HTTP server's authentication and TLS
settings before exposing it on a network.

## Discover tap points

```console
kubectl port-forward pod/my-collector 1777:1777
curl --fail --silent http://localhost:1777/debug/tracetap/v1/taps
```

Receivers expose an output tap, processors expose input and output taps,
exporters expose an input tap, and connectors expose whichever trace-facing
sides exist. A response resembles:

```json
{
  "taps": [
    {
      "id": "Processor/batch:input@traces/application",
      "component_id": "batch",
      "component_kind": "Processor",
      "pipeline_id": "traces/application",
      "position": "input"
    }
  ]
}
```

## Stream a tap

```console
curl --fail --silent \
  'http://localhost:1777/debug/tracetap/v1/stream?tap_id=Processor/batch:input@traces/application&duration=30s' \
  --output traces.tap
```

The response is a sequence of frames, each a 4-byte big-endian length
prefix followed by that many bytes of OTLP `ExportTraceServiceRequest`
proto — the real trace data observed at that boundary, not a summary or a
lossy text dump. The stream ends when `duration` elapses (capped by
`max_stream_duration`), or when the client disconnects.

`cmd/tracetapclient` is a small companion that reads this stream and
re-exports the traces as a normal OTLP gRPC export, e.g. into another
Collector for further processing:

```console
go run ./cmd/tracetapclient \
  --stream-url='http://localhost:1777/debug/tracetap/v1/stream?tap_id=Processor/batch:input@traces/application&duration=30s' \
  --otlp-endpoint=localhost:4317
```

## Bounding cost

Two things guard against an unfiltered tap point running away with
resources on the collector being tapped:

- `max_concurrent_streams` (default `1`) bounds how many stream requests
  can be active at once; a request beyond that is rejected with `409`.
- `max_spans_per_second` (default `1000`) is a hard, per-stream safety cap
  independent of whatever filtering the pipeline applies upstream of the
  tapped boundary — spans beyond that budget are dropped, not queued.

`stream_buffer_size` (default `256`) bounds how many trace batches can be
buffered between the pipeline's hot path and the (comparatively slow) HTTP
writer; once full, further batches are dropped rather than blocking the
pipeline.

Notifying an observer while a stream is active costs one `ptrace.Traces`
clone on the hot path, at exactly the tapped boundary — this is necessary
because a downstream component may mutate the same data in place after
the tap observes it, and the stream needs its own, undisturbed copy. With
no active stream, there's no clone and no cost beyond the atomic load.
