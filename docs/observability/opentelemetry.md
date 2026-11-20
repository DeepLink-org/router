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

The Router emits an HTTP SERVER span and a CLIENT span for each backend call. Both carry the
HTTP method and route template. The SERVER span also records the route template as `url.path`.
It inherits the incoming W3C trace context and sampling decision. A missing or invalid parent
starts a root with `trace.context.missing=true`. A client span measures the time to backend response headers.

Set `service.instance.id` in `OTEL_RESOURCE_ATTRIBUTES` to supply an instance ID. Otherwise the
Router uses its Kubernetes pod identity and PID, or generates a UUID at startup.

Both spans record `request.id` from a safe inbound `x-request-id` (16–64 ASCII letters, digits,
underscores or hyphens). Repeated correlation headers use the first field. Missing or invalid
values leave the attribute absent. The Router's HTTP response ID and P/D coordination IDs
retain their existing protocol semantics.
`session.id` uses the explicit session header, or the OpenClaw session key when affinity is absent.
The gateway can supply a normalized session ID for affinity routing. Health probes and LMDeploy
worker registration endpoints are excluded by default. Span export uses the batch processor.

### Exported span names

With `OTEL_SERVICE_NAME=lmdeploy-router`, the Router exports inbound inference POSTs as
`model.router` (SERVER). Each carries the matched HTTP route template, method, response status,
`inference.engine=lmdeploy` and `inference.role=router`. The boolean `envoy.error` reflects
an HTTP 5xx or a request failure observed by the Router. The gateway can apply its own signals.

Other routes and backend CLIENT spans retain their HTTP names. Keep backend CLIENT spans when
filtering downstream spans that reference them as parents. The local Collector can forward
these spans directly. Register the exported names with the gateway before deployment.
