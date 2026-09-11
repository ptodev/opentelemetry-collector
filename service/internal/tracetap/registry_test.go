// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package tracetap

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/collector/service/hostcapabilities"
)

func TestNewPoint(t *testing.T) {
	id := component.MustNewID("batch")

	p := NewPoint(component.KindProcessor, id, "", hostcapabilities.TraceTapPositionInput)
	assert.Equal(t, "Processor/batch:input", p.ID)
	assert.Equal(t, "batch", p.ComponentID)
	assert.Equal(t, "Processor", p.ComponentKind)
	assert.Empty(t, p.PipelineID)

	p = NewPoint(component.KindProcessor, id, "traces/application", hostcapabilities.TraceTapPositionOutput)
	assert.Equal(t, "Processor/batch:output@traces/application", p.ID)
	assert.Equal(t, "traces/application", p.PipelineID)
}

func TestRegistry_NilIsNoop(t *testing.T) {
	var r *Registry

	sink := new(consumertest.TracesSink)
	wrapped := r.Wrap(sink, hostcapabilities.TraceTap{ID: "x"})
	require.NoError(t, wrapped.ConsumeTraces(context.Background(), ptrace.NewTraces()))
	assert.Len(t, sink.AllTraces(), 1, "forwarded through unchanged")

	assert.Nil(t, r.TraceTaps())

	unregister := r.RegisterTraceObserver(nil)
	unregister() // must not panic
}

func TestRegistry_WrapRegistersTapAndForwards(t *testing.T) {
	r := NewRegistry()
	sink := new(consumertest.TracesSink)
	point := NewPoint(component.KindExporter, component.MustNewID("otlp"), "", hostcapabilities.TraceTapPositionInput)

	wrapped := r.Wrap(sink, point)

	traces := ptrace.NewTraces()
	traces.ResourceSpans().AppendEmpty()
	require.NoError(t, wrapped.ConsumeTraces(context.Background(), traces))

	require.Len(t, sink.AllTraces(), 1)

	taps := r.TraceTaps()
	require.Len(t, taps, 1)
	assert.Equal(t, point.ID, taps[0].ID)
}

type recordingObserver struct {
	observed []ptrace.Traces
}

func (o *recordingObserver) ObserveTraces(_ hostcapabilities.TraceTap, traces ptrace.Traces) {
	o.observed = append(o.observed, traces)
}

func TestRegistry_ObserverSeesTraffic(t *testing.T) {
	r := NewRegistry()
	sink := new(consumertest.TracesSink)
	point := NewPoint(component.KindReceiver, component.MustNewID("otlp"), "", hostcapabilities.TraceTapPositionOutput)
	wrapped := r.Wrap(sink, point)

	obs := &recordingObserver{}
	unregister := r.RegisterTraceObserver(obs)

	traces := ptrace.NewTraces()
	traces.ResourceSpans().AppendEmpty()
	require.NoError(t, wrapped.ConsumeTraces(context.Background(), traces))
	require.Len(t, obs.observed, 1)
	require.Len(t, sink.AllTraces(), 1)

	unregister()

	require.NoError(t, wrapped.ConsumeTraces(context.Background(), ptrace.NewTraces()))
	assert.Len(t, obs.observed, 1, "observer must not be notified after unregistering")
	assert.Len(t, sink.AllTraces(), 2, "forwarding to next must continue after unregistering")
}

func TestRegistry_MultipleObservers(t *testing.T) {
	r := NewRegistry()
	sink := new(consumertest.TracesSink)
	point := NewPoint(component.KindProcessor, component.MustNewID("batch"), "traces/app", hostcapabilities.TraceTapPositionInput)
	wrapped := r.Wrap(sink, point)

	obsA := &recordingObserver{}
	obsB := &recordingObserver{}
	r.RegisterTraceObserver(obsA)
	r.RegisterTraceObserver(obsB)

	require.NoError(t, wrapped.ConsumeTraces(context.Background(), ptrace.NewTraces()))
	assert.Len(t, obsA.observed, 1)
	assert.Len(t, obsB.observed, 1)
}
