#!/usr/bin/env python3
"""Sends synthetic OTLP/HTTP traces to the three leaf collectors on a loop.

Alternates small traces (a handful of spans -- should be filtered out by the
gatherer's tracesizepolicy tail-sampling) and big traces (hundreds of spans
with padded attributes -- should cross the 20KB demo threshold and reach
Tempo).
"""
import json
import random
import time
import urllib.request

ENDPOINTS = [
    "http://localhost:5318/v1/traces",
    "http://localhost:5328/v1/traces",
    "http://localhost:5338/v1/traces",
]


def rand_hex(nbytes):
    return "".join(random.choice("0123456789abcdef") for _ in range(nbytes * 2))


def make_trace(span_count, service_name, pad_bytes=0):
    trace_id = rand_hex(16)
    now_ns = int(time.time() * 1e9)
    padding = "x" * pad_bytes
    spans = []
    for i in range(span_count):
        span = {
            "traceId": trace_id,
            "spanId": rand_hex(8),
            "name": f"{service_name}-op-{i}",
            "startTimeUnixNano": str(now_ns + i * 1000),
            "endTimeUnixNano": str(now_ns + i * 1000 + 500),
        }
        if pad_bytes:
            span["attributes"] = [{"key": "payload", "value": {"stringValue": padding}}]
        spans.append(span)
    return {
        "resourceSpans": [{
            "resource": {"attributes": [{"key": "service.name", "value": {"stringValue": service_name}}]},
            "scopeSpans": [{"spans": spans}],
        }]
    }


def send(endpoint, trace):
    data = json.dumps(trace).encode("utf-8")
    req = urllib.request.Request(endpoint, data=data, headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=5) as resp:
            return resp.status
    except Exception as e:  # noqa: BLE001 - loadgen, keep going regardless of transient errors
        return f"error: {e}"


def main():
    i = 0
    while True:
        endpoint = ENDPOINTS[i % len(ENDPOINTS)]
        service = f"svc-{(i % len(ENDPOINTS)) + 1}"
        if i % 3 == 0:
            trace = make_trace(span_count=400, service_name=service, pad_bytes=100)
            kind = "big"
        else:
            trace = make_trace(span_count=5, service_name=service, pad_bytes=0)
            kind = "small"
        status = send(endpoint, trace)
        print(f"[{time.strftime('%H:%M:%S')}] sent {kind} trace to {endpoint} -> {status}", flush=True)
        i += 1
        time.sleep(2)


if __name__ == "__main__":
    main()
