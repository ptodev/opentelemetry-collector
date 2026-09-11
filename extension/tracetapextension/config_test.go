// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package tracetapextension

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConfig_Validate(t *testing.T) {
	valid := func() *Config {
		cfg := createDefaultConfig().(*Config)
		return cfg
	}

	require.NoError(t, valid().Validate())

	tests := []struct {
		name    string
		mutate  func(*Config)
		wantErr string
	}{
		{"empty endpoint", func(c *Config) { c.NetAddr.Endpoint = "" }, "endpoint"},
		{"zero default duration", func(c *Config) { c.DefaultStreamDuration = 0 }, "default_stream_duration"},
		{"zero max duration", func(c *Config) { c.MaxStreamDuration = 0 }, "max_stream_duration"},
		{"default exceeds max", func(c *Config) { c.DefaultStreamDuration = 2 * c.MaxStreamDuration }, "must not exceed"},
		{"zero max concurrent streams", func(c *Config) { c.MaxConcurrentStreams = 0 }, "max_concurrent_streams"},
		{"zero max spans per second", func(c *Config) { c.MaxSpansPerSecond = 0 }, "max_spans_per_second"},
		{"zero stream buffer size", func(c *Config) { c.StreamBufferSize = 0 }, "stream_buffer_size"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := valid()
			tt.mutate(cfg)
			err := cfg.Validate()
			require.Error(t, err)
			assert.ErrorContains(t, err, tt.wantErr)
		})
	}
}

func TestCreateDefaultConfig(t *testing.T) {
	cfg := createDefaultConfig().(*Config)
	assert.Equal(t, 10*time.Second, cfg.DefaultStreamDuration)
	assert.Equal(t, 5*time.Minute, cfg.MaxStreamDuration)
	assert.Equal(t, 1, cfg.MaxConcurrentStreams)
	assert.Equal(t, time.Duration(0), cfg.WriteTimeout)
	require.NoError(t, cfg.Validate())
}
