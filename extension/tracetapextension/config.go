// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package tracetapextension // import "go.opentelemetry.io/collector/extension/tracetapextension"

import (
	"errors"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/config/confighttp"
)

// Config configures the trace_tap extension.
type Config struct {
	confighttp.ServerConfig `mapstructure:",squash"`

	// DefaultStreamDuration is used when a stream request doesn't specify
	// its own duration.
	DefaultStreamDuration time.Duration `mapstructure:"default_stream_duration"`
	// MaxStreamDuration bounds how long any single stream may run,
	// regardless of what a request asks for.
	MaxStreamDuration time.Duration `mapstructure:"max_stream_duration"`
	// MaxConcurrentStreams bounds how many stream requests can be active at
	// once. A request beyond this limit is rejected with 409.
	MaxConcurrentStreams int `mapstructure:"max_concurrent_streams"`
	// MaxSpansPerSecond is a hard safety cap on how many spans a single
	// stream will emit per second, independent of whatever filtering the
	// pipeline itself applies upstream of the tapped boundary. Spans beyond
	// this budget are dropped, not queued.
	MaxSpansPerSecond int `mapstructure:"max_spans_per_second"`
	// StreamBufferSize bounds the number of trace batches buffered per
	// stream between the pipeline's hot path and the (slower) HTTP writer.
	// A full buffer causes the batch to be dropped rather than blocking the
	// pipeline.
	StreamBufferSize int `mapstructure:"stream_buffer_size"`

	// prevent unkeyed literal initialization
	_ struct{}
}

var _ component.Config = (*Config)(nil)

func createDefaultConfig() component.Config {
	serverConfig := confighttp.NewDefaultServerConfig()
	serverConfig.NetAddr.Endpoint = "localhost:1777"
	// The stream response is long-lived; disable the write timeout so the
	// server doesn't close a healthy, actively-draining stream once it runs
	// past an otherwise-reasonable deadline.
	serverConfig.WriteTimeout = 0

	return &Config{
		ServerConfig: serverConfig,
		DefaultStreamDuration: 10 * time.Second,
		MaxStreamDuration:     5 * time.Minute,
		MaxConcurrentStreams:  1,
		MaxSpansPerSecond:     1000,
		StreamBufferSize:      256,
	}
}

// Validate checks if the extension configuration is valid.
func (cfg *Config) Validate() error {
	if cfg.ServerConfig.NetAddr.Endpoint == "" {
		return errors.New("\"endpoint\" is required when using the \"trace_tap\" extension")
	}
	if cfg.DefaultStreamDuration <= 0 {
		return errors.New("\"default_stream_duration\" must be positive")
	}
	if cfg.MaxStreamDuration <= 0 {
		return errors.New("\"max_stream_duration\" must be positive")
	}
	if cfg.DefaultStreamDuration > cfg.MaxStreamDuration {
		return errors.New("\"default_stream_duration\" must not exceed \"max_stream_duration\"")
	}
	if cfg.MaxConcurrentStreams <= 0 {
		return errors.New("\"max_concurrent_streams\" must be positive")
	}
	if cfg.MaxSpansPerSecond <= 0 {
		return errors.New("\"max_spans_per_second\" must be positive")
	}
	if cfg.StreamBufferSize <= 0 {
		return errors.New("\"stream_buffer_size\" must be positive")
	}
	return nil
}
