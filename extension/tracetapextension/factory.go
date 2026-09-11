// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package tracetapextension // import "go.opentelemetry.io/collector/extension/tracetapextension"

//go:generate mdatagen metadata.yaml

import (
	"context"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/extension"
	"go.opentelemetry.io/collector/extension/tracetapextension/internal/metadata"
)

// NewFactory returns a new factory for the trace_tap extension.
func NewFactory() extension.Factory {
	return extension.NewFactory(
		metadata.Type,
		createDefaultConfig,
		createExtension,
		metadata.ExtensionStability,
	)
}

func createExtension(_ context.Context, set extension.Settings, cfg component.Config) (extension.Extension, error) {
	return newTraceTapExtension(cfg.(*Config), set.TelemetrySettings), nil
}
