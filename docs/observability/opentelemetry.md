# OpenTelemetry tracing

Enable tracing in the Router process and point it at the local Collector's OTLP/gRPC receiver.

```bash
export OTEL_SERVICE_NAME=lmdeploy-router
export OTEL_RESOURCE_ATTRIBUTES='inference.engine=lmdeploy,inference.role=router'
export OTEL_EXPORTER_OTLP_ENDPOINT=http://127.0.0.1:4317
lmdeploy-router --enable-trace --worker-urls http://127.0.0.1:23333
```

For LMDeploy dynamic worker registration, add `--enable-trace` to the existing Router command.
Configure the API Server process separately with `OTEL_SERVICE_NAME=lmdeploy-full` and the same
Collector endpoint. The Collector exports traces to the gateway using OTLP/gRPC over TLS.

The Router emits an HTTP SERVER span with the method, route template, and `url.path`.
It inherits the incoming W3C trace context and sampling decision. A missing or invalid parent
starts a root with `trace.context.missing=true`.

Set `service.instance.id` in `OTEL_RESOURCE_ATTRIBUTES` to supply an instance ID. Otherwise the
Router uses its Kubernetes pod identity and PID, or generates a UUID at startup.

Request spans record `request.id` from a safe inbound `x-request-id` (16–64 ASCII letters, digits,
underscores or hyphens). Repeated correlation headers use the first field. Missing or invalid
values leave the attribute absent. The Router's HTTP response ID and P/D coordination IDs
retain their existing protocol semantics.
`session.id` uses the explicit session header, or the OpenClaw session key when affinity is absent.
The gateway can supply a normalized session ID for affinity routing. Health probes and LMDeploy
worker registration endpoints are excluded by default. Span export uses the batch processor.

### Exported span names

With `OTEL_SERVICE_NAME=lmdeploy-router`, the Router exports inbound inference POSTs as
`model.router` (SERVER). Each carries the matched HTTP route template, method, response status,
`inference.engine=lmdeploy` and `inference.role=router`. The Router injects that span's
context directly into backend inference requests. LMDeploy's HTTP request and measured phase
spans can then reference the exported `model.router` span by ID. HTTP 5xx responses and
transport failures set the Router span's OpenTelemetry status to ERROR.

Other Router identities and other HTTP routes retain backend CLIENT spans, which measure
the time to backend response headers.
