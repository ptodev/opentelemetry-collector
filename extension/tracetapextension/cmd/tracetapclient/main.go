// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

// Command tracetapclient reads a trace_tap extension's stream (see
// extension/tracetapextension) through a laptop-initiated HTTP connection
// (e.g. a `kubectl port-forward`) and re-exports the traces it carries as a
// normal OTLP gRPC export, so they can be fed into any other Collector for
// further analysis.
package main

import (
	"context"
	"encoding/binary"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/collector/pdata/ptrace/ptraceotlp"
)

func main() {
	streamURL := flag.String("stream-url", "", "trace_tap stream URL, e.g. http://localhost:1777/debug/tracetap/v1/stream?tap_id=...&duration=30s (required)")
	otlpEndpoint := flag.String("otlp-endpoint", "localhost:4317", "OTLP/gRPC endpoint to re-export received traces to")
	insecure := flag.Bool("insecure", true, "use an insecure (non-TLS) connection to --otlp-endpoint")
	flag.Parse()

	if *streamURL == "" {
		log.Fatal("--stream-url is required")
	}

	if err := run(context.Background(), *streamURL, *otlpEndpoint, *insecure); err != nil {
		log.Fatal(err)
	}
}

func run(ctx context.Context, streamURL, otlpEndpoint string, insecureConn bool) error {
	client, err := newOTLPClient(ctx, otlpEndpoint, insecureConn)
	if err != nil {
		return fmt.Errorf("connecting to %s: %w", otlpEndpoint, err)
	}
	defer client.close()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, streamURL, nil)
	if err != nil {
		return err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("connecting to %s: %w", streamURL, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("stream request failed: %s", resp.Status)
	}

	count := 0
	for {
		traces, err := readFrame(resp.Body)
		if err == io.EOF {
			log.Printf("stream ended after re-exporting %d batches", count)
			return nil
		}
		if err != nil {
			return fmt.Errorf("reading frame: %w", err)
		}
		if err := client.export(ctx, traces); err != nil {
			log.Printf("failed to re-export batch: %v", err)
			continue
		}
		count++
	}
}

// readFrame reads one 4-byte-length-prefixed OTLP proto frame, as written
// by the trace_tap extension's stream handler.
func readFrame(r io.Reader) (ptrace.Traces, error) {
	var header [4]byte
	if _, err := io.ReadFull(r, header[:]); err != nil {
		return ptrace.Traces{}, err
	}
	n := binary.BigEndian.Uint32(header[:])
	data := make([]byte, n)
	if _, err := io.ReadFull(r, data); err != nil {
		return ptrace.Traces{}, err
	}
	unmarshaler := ptrace.ProtoUnmarshaler{}
	return unmarshaler.UnmarshalTraces(data)
}

type otlpClient struct {
	conn   *grpc.ClientConn
	client ptraceotlp.GRPCClient
}

func newOTLPClient(ctx context.Context, endpoint string, insecureConn bool) (*otlpClient, error) {
	var opts []grpc.DialOption
	if insecureConn {
		opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	}
	dialCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	conn, err := grpc.DialContext(dialCtx, endpoint, opts...) //nolint:staticcheck // SA1019 grpc.DialContext kept for the wider compatibility this small tool targets
	if err != nil {
		return nil, err
	}
	return &otlpClient{conn: conn, client: ptraceotlp.NewGRPCClient(conn)}, nil
}

func (c *otlpClient) export(ctx context.Context, traces ptrace.Traces) error {
	_, err := c.client.Export(ctx, ptraceotlp.NewExportRequestFromTraces(traces))
	return err
}

func (c *otlpClient) close() error {
	return c.conn.Close()
}
