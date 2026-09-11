// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

// Package hostcapabilities provides interfaces that can be implemented by the host
// to provide additional capabilities.
package hostcapabilities // import "go.opentelemetry.io/collector/service/hostcapabilities"

import (
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/collector/pipeline"
	"go.opentelemetry.io/collector/service/internal/moduleinfo"
)

// ModuleInfo is an interface that may be implemented by the host to provide
// information about modules that were used to build the host.
type ModuleInfo interface {
	// GetModuleInfos returns the module information for the host
	// i.e. Receivers, Processors, Exporters, Extensions, and Connectors
	GetModuleInfos() moduleinfo.ModuleInfos
}

// ExposeExporters is an interface that may be implemented by the host to provide
// access to the exporters that were used to build the host.
//
// Deprecated: [v0.121.0] Will be removed in Service 1.0.
// See: https://github.com/open-telemetry/opentelemetry-collector/issues/7370 for service 1.0
type ExposeExporters interface {
	GetExporters() map[pipeline.Signal]map[component.ID]component.Component
}

// ComponentFactory is an interface that may be implemented by the host to
// provide a component's factory
type ComponentFactory interface {
	// GetFactory returns the component factory for the given
	// component type
	GetFactory(kind component.Kind, componentType component.Type) component.Factory
}

// TraceTapPosition identifies which side of a component a TraceTap observes.
type TraceTapPosition string

const (
	// TraceTapPositionInput observes traces as they enter a component.
	TraceTapPositionInput TraceTapPosition = "input"
	// TraceTapPositionOutput observes traces as they leave a component.
	TraceTapPositionOutput TraceTapPosition = "output"
)

// TraceTap identifies one observable trace-facing boundary between two
// components in a pipeline.
type TraceTap struct {
	// ID uniquely identifies this tap, e.g. "Processor/batch:input@traces/application".
	ID string
	// ComponentID is the ID of the component this tap is attached to.
	ComponentID string
	// ComponentKind is the kind of the component this tap is attached to.
	ComponentKind string
	// PipelineID is the pipeline this tap belongs to. Empty for taps on
	// components that can be shared across pipelines (receivers, exporters),
	// since such a tap observes traffic for all pipelines sharing it.
	PipelineID string
	// Position indicates whether this tap observes the component's input or output.
	Position TraceTapPosition
}

// TraceObserver observes traces flowing through a registered TraceTap. It
// must return promptly: it is invoked synchronously on the pipeline's hot
// path, once per matching tap, for every batch of traces observed.
type TraceObserver interface {
	ObserveTraces(tap TraceTap, traces ptrace.Traces)
}

// TraceTaps is an interface that may be implemented by the host to provide
// access to the catalog of trace-facing pipeline boundaries, and to let a
// caller (typically an extension) observe traffic at those boundaries.
type TraceTaps interface {
	// TraceTaps returns a stable snapshot of the registered tap catalog.
	TraceTaps() []TraceTap
	// RegisterTraceObserver adds an observer and returns an idempotent
	// function to unregister it.
	RegisterTraceObserver(observer TraceObserver) func()
}
