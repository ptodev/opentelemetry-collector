// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/collector/pdata/ptrace/ptraceotlp"
)

func TestReadFrame(t *testing.T) {
	traces := ptrace.NewTraces()
	traces.ResourceSpans().AppendEmpty().ScopeSpans().AppendEmpty().Spans().AppendEmpty()
	marshaler := ptrace.ProtoMarshaler{}
	data, err := marshaler.MarshalTraces(traces)
	require.NoError(t, err)

	var buf bytes.Buffer
	var header [4]byte
	binary.BigEndian.PutUint32(header[:], uint32(len(data)))
	buf.Write(header[:])
	buf.Write(data)

	got, err := readFrame(&buf)
	require.NoError(t, err)
	assert.Equal(t, 1, got.SpanCount())

	_, err = readFrame(&buf)
	assert.ErrorIs(t, err, io.EOF)
}

func TestReadFrame_TruncatedHeader(t *testing.T) {
	_, err := readFrame(bytes.NewReader([]byte{0x00, 0x01}))
	assert.Error(t, err)
}

type recordingTracesServer struct {
	ptraceotlp.UnimplementedGRPCServer
	mu      sync.Mutex
	batches []ptrace.Traces
}

func (s *recordingTracesServer) Export(_ context.Context, req ptraceotlp.ExportRequest) (ptraceotlp.ExportResponse, error) {
	s.mu.Lock()
	s.batches = append(s.batches, req.Traces())
	s.mu.Unlock()
	return ptraceotlp.NewExportResponse(), nil
}

func (s *recordingTracesServer) received() []ptrace.Traces {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]ptrace.Traces(nil), s.batches...)
}

func TestRun_StreamsToOTLPExport(t *testing.T) {
	// Fake trace_tap stream: two frames, then EOF.
	traces := ptrace.NewTraces()
	traces.ResourceSpans().AppendEmpty().ScopeSpans().AppendEmpty().Spans().AppendEmpty()
	marshaler := ptrace.ProtoMarshaler{}
	frame, err := marshaler.MarshalTraces(traces)
	require.NoError(t, err)

	streamServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		for i := 0; i < 2; i++ {
			var header [4]byte
			binary.BigEndian.PutUint32(header[:], uint32(len(frame)))
			_, _ = w.Write(header[:])
			_, _ = w.Write(frame)
		}
	}))
	defer streamServer.Close()

	// Fake OTLP receiver over an in-memory connection.
	lis := bufconn.Listen(1024 * 1024)
	grpcServer := grpc.NewServer()
	recorder := &recordingTracesServer{}
	ptraceotlp.RegisterGRPCServer(grpcServer, recorder)
	go func() { _ = grpcServer.Serve(lis) }()
	defer grpcServer.Stop()

	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return lis.Dial() }),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	defer conn.Close()

	client := &otlpClient{conn: conn, client: ptraceotlp.NewGRPCClient(conn)}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, streamServer.URL, nil)
	require.NoError(t, err)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	count := 0
	for {
		tr, err := readFrame(resp.Body)
		if errors.Is(err, io.EOF) {
			break
		}
		require.NoError(t, err)
		require.NoError(t, client.export(ctx, tr))
		count++
	}

	assert.Equal(t, 2, count)
	assert.Len(t, recorder.received(), 2)
}
