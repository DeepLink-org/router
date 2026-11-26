# OpenTelemetry tracing

Set `OTEL_EXPORTER_OTLP_ENDPOINT` in the deployment environment to the selected Collector's
OTLP/gRPC receiver URL. The address belongs to the deployment, not the Router implementation.
Configure the Router process separately from the backend API process.

```bash
export OTEL_SERVICE_NAME=lmdeploy-router
export OTEL_RESOURCE_ATTRIBUTES='inference.engine=lmdeploy,inference.role=router'
: "${OTEL_EXPORTER_OTLP_ENDPOINT:?Set the Collector OTLP/gRPC endpoint in the deployment environment}"
export OTEL_EXPORTER_OTLP_ENDPOINT
# Add --enable-trace to the existing Router startup command.
```

Keep the existing worker URLs or LMDeploy dynamic worker registration configuration.
Configure the API Server process separately with `OTEL_SERVICE_NAME=lmdeploy-full` and the same
Collector endpoint. The Collector exports traces to the gateway using OTLP/gRPC over TLS.

The Router emits an HTTP SERVER span with the method, route template, and `url.path`.
It inherits the incoming W3C trace context and sampling decision. A missing or invalid parent
starts a root with `trace.context.missing=true`.

Set `service.instance.id` in `OTEL_RESOURCE_ATTRIBUTES` to supply an instance ID. Otherwise the
Router uses its Kubernetes pod identity and PID, or generates a UUID at startup.
SDK and configured resource metadata is restricted to `service.name`, `service.instance.id`,
`deployment.environment`, `k8s.cluster.name`, `k8s.namespace.name`, `k8s.pod.name`,
`k8s.node.name`, `inference.engine` and `inference.role`, with scalar and privacy checks.
Other fields, including raw affinity and request/session IDs injected through resource environment
variables, are omitted before export. Request/session IDs come from accepted request headers.
Additional resource fields require an agreed platform contract update before being allowed here.

Request spans record `request.id` from a safe inbound `x-request-id` (16–64 ASCII letters, digits,
underscores or hyphens). Repeated correlation headers use the first field. Missing or invalid
values leave the attribute absent. The Router's HTTP response ID and P/D coordination IDs
retain their existing protocol semantics.
`session.id` uses the explicit session header, or the OpenClaw session key when affinity is absent.
The gateway can supply a normalized session ID for affinity routing. Health probes and LMDeploy
worker registration endpoints are excluded by default. Span export uses the SDK batch processor.
Completed spans are pushed asynchronously using the standard `OTEL_BSP_*` environment settings.
Defaults are a 5-second scheduled delay (`OTEL_BSP_SCHEDULE_DELAY=5000`), a maximum batch of
512 spans (`OTEL_BSP_MAX_EXPORT_BATCH_SIZE=512`) and a queue of 2,048 spans
(`OTEL_BSP_MAX_QUEUE_SIZE=2048`). A full batch can be sent earlier. These settings are optional;
the defaults align with vLLM's OpenTelemetry SDK configuration. Export delay does not change
request timing or span timestamps. Collector buffering is configured separately.
Five seconds is not an upper bound on delivery time or data loss on forced termination;
queued spans, export retries and shutdown behavior also matter. The Rust SDK reads these
settings and ignores values it cannot parse; LMDeploy validates its Python settings separately.
For excluded routes that still forward an HTTP request, the backend CLIENT span inherits the
incoming parent and sampling decision. Valid `tracestate` is forwarded as whole leftmost entries,
limited to eight vendors and 512 bytes, including for unsampled requests. No local probability
sampling option is added; the gateway owns tail sampling.

### Exported span names

With `OTEL_SERVICE_NAME=lmdeploy-router`, the Router exports inbound inference POSTs as
`model.router` (SERVER). Each carries the matched HTTP route template, method, response status,
`inference.engine=lmdeploy` and `inference.role=router`. The Router injects that span's
context directly into backend inference requests. LMDeploy's HTTP request and measured phase
spans can then reference the exported `model.router` span by ID. HTTP 5xx responses and
transport failures set the Router span's OpenTelemetry status to ERROR, including a backend
response-body read failure after HTTP 200 headers and an initial SSE chunk. The HTTP status
remains 200 because those headers have already been sent. The Router forwards the body error
without buffering or parsing SSE; a clean transport EOF without a protocol terminal event is
not classified by this middleware.
Normally completed 4xx responses do not set ERROR, but a body transport failure does, including
after 4xx headers. With OTel disabled, non-5xx responses skip end-of-stream error classification;
existing HTTP status and service-error logging remain active.
`latency_ms` measures request-to-response-headers time, or time to a service failure before
headers. A body read failure preserves the recorded header latency. The full span duration
also includes body processing; it is distinct from `latency_ms`.

Other Router identities and other HTTP routes retain backend CLIENT spans, which measure
the time to backend response headers.
