// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package tracetapextension // import "go.opentelemetry.io/collector/extension/tracetapextension"

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"net/http"
	"sync"
	"time"

	"go.uber.org/zap"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/component/componentstatus"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/collector/service/hostcapabilities"
)

const (
	pathTaps   = "/debug/tracetap/v1/taps"
	pathStream = "/debug/tracetap/v1/stream"
)

type traceTapExtension struct {
	config    *Config
	telemetry component.TelemetrySettings

	hostTaps hostcapabilities.TraceTaps

	server *http.Server
	stopCh chan struct{}

	streamSlots chan struct{}
}

func newTraceTapExtension(cfg *Config, telemetry component.TelemetrySettings) *traceTapExtension {
	return &traceTapExtension{
		config:      cfg,
		telemetry:   telemetry,
		streamSlots: make(chan struct{}, cfg.MaxConcurrentStreams),
	}
}

func (e *traceTapExtension) Start(ctx context.Context, host component.Host) error {
	if hostTaps, ok := host.(hostcapabilities.TraceTaps); ok {
		e.hostTaps = hostTaps
	} else {
		e.telemetry.Logger.Warn("Host does not support trace taps; the trace_tap extension will report an empty catalog and refuse streams")
	}

	mux := http.NewServeMux()
	mux.HandleFunc(pathTaps, e.handleTaps)
	mux.HandleFunc(pathStream, e.handleStream)

	ln, err := e.config.ToListener(ctx)
	if err != nil {
		return err
	}

	e.server, err = e.config.ToServer(ctx, host.GetExtensions(), e.telemetry, mux)
	if err != nil {
		return err
	}

	e.stopCh = make(chan struct{})
	go func() {
		defer close(e.stopCh)
		if errHTTP := e.server.Serve(ln); errHTTP != nil && !errors.Is(errHTTP, http.ErrServerClosed) {
			componentstatus.ReportStatus(host, componentstatus.NewFatalErrorEvent(errHTTP))
		}
	}()
	return nil
}

func (e *traceTapExtension) Shutdown(context.Context) error {
	if e.server == nil {
		return nil
	}
	err := e.server.Close()
	if e.stopCh != nil {
		<-e.stopCh
	}
	return err
}

func (e *traceTapExtension) handleTaps(w http.ResponseWriter, _ *http.Request) {
	if e.hostTaps == nil {
		http.Error(w, "trace taps not available", http.StatusServiceUnavailable)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(struct {
		Taps []hostcapabilities.TraceTap `json:"taps"`
	}{Taps: e.hostTaps.TraceTaps()})
}

func (e *traceTapExtension) handleStream(w http.ResponseWriter, r *http.Request) {
	if e.hostTaps == nil {
		http.Error(w, "trace taps not available", http.StatusServiceUnavailable)
		return
	}

	tapID := r.URL.Query().Get("tap_id")
	if tapID == "" {
		http.Error(w, "\"tap_id\" query parameter is required", http.StatusBadRequest)
		return
	}

	duration := e.config.DefaultStreamDuration
	if raw := r.URL.Query().Get("duration"); raw != "" {
		parsed, err := time.ParseDuration(raw)
		if err != nil {
			http.Error(w, "invalid \"duration\": "+err.Error(), http.StatusBadRequest)
			return
		}
		duration = parsed
	}
	if duration > e.config.MaxStreamDuration {
		duration = e.config.MaxStreamDuration
	}

	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming not supported", http.StatusInternalServerError)
		return
	}

	select {
	case e.streamSlots <- struct{}{}:
		defer func() { <-e.streamSlots }()
	default:
		http.Error(w, "max_concurrent_streams reached", http.StatusConflict)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), duration)
	defer cancel()

	batches := make(chan ptrace.Traces, e.config.StreamBufferSize)
	limiter := newRateLimiter(e.config.MaxSpansPerSecond)
	var dropped int64

	unregister := e.hostTaps.RegisterTraceObserver(traceObserverFunc(func(tap hostcapabilities.TraceTap, traces ptrace.Traces) {
		if tap.ID != tapID {
			return
		}
		spanCount := traces.SpanCount()
		if !limiter.allow(spanCount) {
			dropped += int64(spanCount)
			return
		}
		clone := ptrace.NewTraces()
		traces.CopyTo(clone)
		select {
		case batches <- clone:
		default:
			dropped += int64(spanCount)
		}
	}))
	defer unregister()

	w.Header().Set("Content-Type", "application/vnd.opentelemetry.tracetap.v1+octet-stream")
	w.WriteHeader(http.StatusOK)
	flusher.Flush()

	marshaler := ptrace.ProtoMarshaler{}
	for {
		select {
		case <-ctx.Done():
			return
		case tr := <-batches:
			if err := writeFrame(w, marshaler, tr); err != nil {
				e.telemetry.Logger.Debug("trace_tap stream write failed, stopping stream", zap.Error(err))
				return
			}
			flusher.Flush()
		}
	}
}

// writeFrame writes traces as a 4-byte big-endian length prefix followed by
// its OTLP proto encoding.
func writeFrame(w http.ResponseWriter, marshaler ptrace.ProtoMarshaler, traces ptrace.Traces) error {
	data, err := marshaler.MarshalTraces(traces)
	if err != nil {
		return err
	}
	var header [4]byte
	binary.BigEndian.PutUint32(header[:], uint32(len(data))) //nolint:gosec // frame length always fits uint32 in practice
	if _, err := w.Write(header[:]); err != nil {
		return err
	}
	_, err = w.Write(data)
	return err
}

type traceObserverFunc func(tap hostcapabilities.TraceTap, traces ptrace.Traces)

func (f traceObserverFunc) ObserveTraces(tap hostcapabilities.TraceTap, traces ptrace.Traces) {
	f(tap, traces)
}

// rateLimiter is a simple fixed-window counter: at most max units may be
// consumed per rolling one-second window. It exists purely as a safety
// valve independent of whatever filtering the pipeline applies upstream of
// the tapped boundary, so it favors being cheap and simple over being
// perfectly smooth.
type rateLimiter struct {
	mu          sync.Mutex
	max         int
	windowStart time.Time
	count       int
	now         func() time.Time
}

func newRateLimiter(maxPerSecond int) *rateLimiter {
	return &rateLimiter{max: maxPerSecond, now: time.Now}
}

func (r *rateLimiter) allow(n int) bool {
	if r.max <= 0 {
		return true
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	now := r.now()
	if now.Sub(r.windowStart) >= time.Second {
		r.windowStart = now
		r.count = 0
	}
	if r.count+n > r.max {
		return false
	}
	r.count += n
	return true
}
