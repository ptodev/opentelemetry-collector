// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package tracetapextension

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/collector/service/hostcapabilities"
)

// fakeTraceTaps is a minimal, directly-controllable hostcapabilities.TraceTaps
// for tests: it lets the test fire ObserveTraces calls on demand instead of
// wiring up a real pipeline.
type fakeTraceTaps struct {
	mu        sync.Mutex
	taps      []hostcapabilities.TraceTap
	observers []hostcapabilities.TraceObserver
}

func (f *fakeTraceTaps) TraceTaps() []hostcapabilities.TraceTap {
	return f.taps
}

func (f *fakeTraceTaps) RegisterTraceObserver(o hostcapabilities.TraceObserver) func() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.observers = append(f.observers, o)
	idx := len(f.observers) - 1
	return func() {
		f.mu.Lock()
		defer f.mu.Unlock()
		f.observers[idx] = nil
	}
}

func (f *fakeTraceTaps) fire(tap hostcapabilities.TraceTap, traces ptrace.Traces) {
	f.mu.Lock()
	observers := append([]hostcapabilities.TraceObserver(nil), f.observers...)
	f.mu.Unlock()
	for _, o := range observers {
		if o != nil {
			o.ObserveTraces(tap, traces)
		}
	}
}

func newTestExtension(t *testing.T, cfg *Config) (*traceTapExtension, *fakeTraceTaps) {
	if cfg == nil {
		cfg = createDefaultConfig().(*Config)
	}
	e := newTraceTapExtension(cfg, componenttest.NewNopTelemetrySettings())
	fake := &fakeTraceTaps{taps: []hostcapabilities.TraceTap{
		{ID: "Processor/batch:input@traces/app", ComponentID: "batch", ComponentKind: "Processor"},
	}}
	e.hostTaps = fake
	return e, fake
}

func TestHandleTaps_NoHost(t *testing.T) {
	e, _ := newTestExtension(t, nil)
	e.hostTaps = nil

	rec := httptest.NewRecorder()
	e.handleTaps(rec, httptest.NewRequest(http.MethodGet, pathTaps, nil))
	assert.Equal(t, http.StatusServiceUnavailable, rec.Code)
}

func TestHandleTaps_ListsCatalog(t *testing.T) {
	e, _ := newTestExtension(t, nil)

	rec := httptest.NewRecorder()
	e.handleTaps(rec, httptest.NewRequest(http.MethodGet, pathTaps, nil))
	require.Equal(t, http.StatusOK, rec.Code)

	var body struct {
		Taps []hostcapabilities.TraceTap `json:"taps"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
	require.Len(t, body.Taps, 1)
	assert.Equal(t, "Processor/batch:input@traces/app", body.Taps[0].ID)
}

func TestHandleStream_MissingTapID(t *testing.T) {
	e, _ := newTestExtension(t, nil)

	rec := httptest.NewRecorder()
	e.handleStream(rec, httptest.NewRequest(http.MethodGet, pathStream, nil))
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandleStream_InvalidDuration(t *testing.T) {
	e, _ := newTestExtension(t, nil)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, pathStream+"?tap_id=x&duration=notaduration", nil)
	e.handleStream(rec, req)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandleStream_MaxConcurrentStreamsRejected(t *testing.T) {
	cfg := createDefaultConfig().(*Config)
	cfg.MaxConcurrentStreams = 1
	e, _ := newTestExtension(t, cfg)

	// Occupy the only slot directly, simulating an in-flight stream.
	e.streamSlots <- struct{}{}
	defer func() { <-e.streamSlots }()

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, pathStream+"?tap_id=x&duration=10ms", nil)
	e.handleStream(rec, req)
	assert.Equal(t, http.StatusConflict, rec.Code)
}

func TestHandleStream_StreamsMatchingTapAndFiltersOthers(t *testing.T) {
	e, fake := newTestExtension(t, nil)

	server := httptest.NewServer(http.HandlerFunc(e.handleStream))
	defer server.Close()

	go func() {
		// give the handler time to register its observer before firing
		time.Sleep(20 * time.Millisecond)
		matching := ptrace.NewTraces()
		matching.ResourceSpans().AppendEmpty().ScopeSpans().AppendEmpty().Spans().AppendEmpty()
		fake.fire(hostcapabilities.TraceTap{ID: "Processor/batch:input@traces/app"}, matching)

		other := ptrace.NewTraces()
		other.ResourceSpans().AppendEmpty().ScopeSpans().AppendEmpty().Spans().AppendEmpty()
		fake.fire(hostcapabilities.TraceTap{ID: "different-tap"}, other)
	}()

	req, err := http.NewRequest(http.MethodGet, server.URL+pathStream+"?tap_id=Processor/batch:input@traces/app&duration=150ms", nil)
	require.NoError(t, err)
	resp, err := server.Client().Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	traces := decodeFrames(t, body)
	require.Len(t, traces, 1, "only the matching tap's traffic should be streamed")
	assert.Equal(t, 1, traces[0].SpanCount())
}

func TestHandleStream_RateLimitDropsExcess(t *testing.T) {
	cfg := createDefaultConfig().(*Config)
	cfg.MaxSpansPerSecond = 1
	cfg.DefaultStreamDuration = 100 * time.Millisecond
	e, fake := newTestExtension(t, cfg)

	server := httptest.NewServer(http.HandlerFunc(e.handleStream))
	defer server.Close()

	go func() {
		time.Sleep(20 * time.Millisecond)
		for i := 0; i < 5; i++ {
			tr := ptrace.NewTraces()
			tr.ResourceSpans().AppendEmpty().ScopeSpans().AppendEmpty().Spans().AppendEmpty()
			fake.fire(hostcapabilities.TraceTap{ID: "Processor/batch:input@traces/app"}, tr)
		}
	}()

	req, err := http.NewRequest(http.MethodGet, server.URL+pathStream+"?tap_id=Processor/batch:input@traces/app", nil)
	require.NoError(t, err)
	resp, err := server.Client().Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	traces := decodeFrames(t, body)
	assert.Less(t, len(traces), 5, "rate limiter must drop spans beyond max_spans_per_second")
}

func decodeFrames(t *testing.T, data []byte) []ptrace.Traces {
	t.Helper()
	var out []ptrace.Traces
	unmarshaler := ptrace.ProtoUnmarshaler{}
	for len(data) > 0 {
		require.GreaterOrEqual(t, len(data), 4)
		n := binary.BigEndian.Uint32(data[:4])
		data = data[4:]
		require.GreaterOrEqual(t, len(data), int(n))
		traces, err := unmarshaler.UnmarshalTraces(data[:n])
		require.NoError(t, err)
		out = append(out, traces)
		data = data[n:]
	}
	return out
}

func TestRateLimiter(t *testing.T) {
	now := time.Now()
	r := newRateLimiter(10)
	r.now = func() time.Time { return now }

	assert.True(t, r.allow(5))
	assert.True(t, r.allow(5))
	assert.False(t, r.allow(1), "budget exhausted within the same window")

	now = now.Add(time.Second)
	assert.True(t, r.allow(1), "new window resets the budget")
}

func TestRateLimiter_ZeroMeansUnlimited(t *testing.T) {
	r := newRateLimiter(0)
	assert.True(t, r.allow(1_000_000))
}

func TestExtension_StartWithoutTraceTapsHost(t *testing.T) {
	cfg := createDefaultConfig().(*Config)
	e := newTraceTapExtension(cfg, componenttest.NewNopTelemetrySettings())

	require.NoError(t, e.Start(context.Background(), componenttest.NewNopHost()))
	defer func() { require.NoError(t, e.Shutdown(context.Background())) }()

	rec := httptest.NewRecorder()
	e.handleTaps(rec, httptest.NewRequest(http.MethodGet, pathTaps, nil))
	assert.Equal(t, http.StatusServiceUnavailable, rec.Code)
}
