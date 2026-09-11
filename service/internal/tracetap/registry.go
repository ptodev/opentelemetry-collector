// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

// Package tracetap provides low-overhead, opt-in observation points at
// trace-facing component boundaries in the Collector's pipelines.
package tracetap // import "go.opentelemetry.io/collector/service/internal/tracetap"

import (
	"context"
	"fmt"
	"slices"
	"sync"
	"sync/atomic"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/collector/service/hostcapabilities"
)

type observerEntry struct {
	id       uint64
	observer hostcapabilities.TraceObserver
}

type observerSet struct {
	entries []observerEntry
}

// Registry owns the trace-tap catalog and the active observer set. The zero
// value is not usable; construct one with NewRegistry. A nil *Registry is
// accepted by Wrap/TraceTaps/RegisterTraceObserver as a no-op, so callers
// that never enable tapping (e.g. validate-only code paths) need not
// construct one.
type Registry struct {
	mu             sync.Mutex
	taps           map[string]hostcapabilities.TraceTap
	nextObserverID uint64
	observers      atomic.Pointer[observerSet]
}

// NewRegistry creates an empty trace-tap registry.
func NewRegistry() *Registry {
	r := &Registry{taps: make(map[string]hostcapabilities.TraceTap)}
	r.observers.Store(&observerSet{})
	return r
}

// NewPoint constructs a trace tap ID for a component boundary. pipelineID
// may be empty for components that can be shared across pipelines
// (receivers, exporters).
func NewPoint(
	kind component.Kind,
	componentID component.ID,
	pipelineID string,
	position hostcapabilities.TraceTapPosition,
) hostcapabilities.TraceTap {
	id := fmt.Sprintf("%s/%s:%s", kind.String(), componentID.String(), position)
	if pipelineID != "" {
		id += "@" + pipelineID
	}
	return hostcapabilities.TraceTap{
		ID:            id,
		ComponentID:   componentID.String(),
		ComponentKind: kind.String(),
		PipelineID:    pipelineID,
		Position:      position,
	}
}

// Wrap registers point in the catalog and returns a consumer that observes
// traces before forwarding them to next. With no observers registered
// anywhere, the returned consumer's ConsumeTraces costs one atomic load and
// nothing else.
func (r *Registry) Wrap(next consumer.Traces, point hostcapabilities.TraceTap) consumer.Traces {
	if r == nil {
		return next
	}
	r.mu.Lock()
	r.taps[point.ID] = point
	r.mu.Unlock()
	return tracesConsumer{next: next, registry: r, point: point}
}

// TraceTaps returns a stable snapshot of the registered tap catalog.
func (r *Registry) TraceTaps() []hostcapabilities.TraceTap {
	if r == nil {
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	taps := make([]hostcapabilities.TraceTap, 0, len(r.taps))
	for _, tap := range r.taps {
		taps = append(taps, tap)
	}
	slices.SortFunc(taps, func(a, b hostcapabilities.TraceTap) int {
		if a.ID < b.ID {
			return -1
		}
		if a.ID > b.ID {
			return 1
		}
		return 0
	})
	return taps
}

// RegisterTraceObserver adds an observer and returns an idempotent
// unregister function. Updates copy the observer set so observation needs
// no lock.
func (r *Registry) RegisterTraceObserver(observer hostcapabilities.TraceObserver) func() {
	if r == nil || observer == nil {
		return func() {}
	}
	r.mu.Lock()
	r.nextObserverID++
	id := r.nextObserverID
	current := r.observers.Load()
	entries := append([]observerEntry(nil), current.entries...)
	entries = append(entries, observerEntry{id: id, observer: observer})
	r.observers.Store(&observerSet{entries: entries})
	r.mu.Unlock()

	var once sync.Once
	return func() {
		once.Do(func() {
			r.mu.Lock()
			current := r.observers.Load()
			entries := make([]observerEntry, 0, len(current.entries))
			for _, entry := range current.entries {
				if entry.id != id {
					entries = append(entries, entry)
				}
			}
			r.observers.Store(&observerSet{entries: entries})
			r.mu.Unlock()
		})
	}
}

type tracesConsumer struct {
	next     consumer.Traces
	registry *Registry
	point    hostcapabilities.TraceTap
}

func (c tracesConsumer) Capabilities() consumer.Capabilities {
	return c.next.Capabilities()
}

// ConsumeTraces notifies every registered observer, then forwards to next.
// Observers are notified synchronously, on the hot path: an observer that
// needs to retain or hand data off asynchronously must clone it first,
// since ownership of traces continues with next after this call returns
// (a downstream component may mutate it in place).
func (c tracesConsumer) ConsumeTraces(ctx context.Context, traces ptrace.Traces) error {
	entries := c.registry.observers.Load().entries
	for _, entry := range entries {
		entry.observer.ObserveTraces(c.point, traces)
	}
	return c.next.ConsumeTraces(ctx, traces)
}
