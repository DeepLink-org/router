use axum::http::HeaderMap;
use tracing::{field::Empty, info_span, instrument::WithSubscriber, Dispatch, Span};
use tracing_opentelemetry::OpenTelemetrySpanExt;
use url::Url;

pub const TRACE_HEADER_NAMES: &[&str] = &["traceparent", "tracestate", "baggage"];

#[derive(Clone, Copy, Debug)]
pub struct ClientRequestOptions<'a> {
    pub method: &'a str,
    pub url: &'a str,
    pub route: Option<&'a str>,
    pub request_phase: Option<&'a str>,
}

pub struct PreparedClientRequest {
    request: reqwest::RequestBuilder,
    span: Span,
}

impl PreparedClientRequest {
    pub async fn send(self) -> Result<reqwest::Response, reqwest::Error> {
        let Self { request, span } = self;

        let response = if crate::otel_trace::is_otel_enabled() {
            // Propagation headers are already set. Hyper's keep-alive tasks can
            // outlive this request and must not retain its client or server span.
            request.send().with_subscriber(Dispatch::none()).await
        } else {
            request.send().await
        };

        match response {
            Ok(response) => {
                if !span.is_disabled() {
                    let status = response.status();
                    span.record("http.response.status_code", i64::from(status.as_u16()));

                    if status.is_server_error() {
                        span.set_status(opentelemetry::trace::Status::error(format!(
                            "HTTP {}",
                            status.as_u16()
                        )));
                    }
                }

                Ok(response)
            }
            Err(error) => {
                if !span.is_disabled() {
                    let message = "backend request failed";
                    span.record("error", true);
                    span.set_status(opentelemetry::trace::Status::error(message));
                }

                Err(error)
            }
        }
    }
}

pub fn prepare_client_request(
    mut request: reqwest::RequestBuilder,
    incoming_headers: Option<&HeaderMap>,
    options: ClientRequestOptions<'_>,
) -> PreparedClientRequest {
    // In LMDeploy inference traffic the worker and its phases share model.router as parent.
    let span = if crate::otel_trace::is_otel_enabled()
        && !(crate::otel_trace::is_lmdeploy_router()
            && crate::otel_trace::is_inference_post(options.method, options.route))
    {
        // Only static inference routes belong in trace metadata. Other backend paths can
        // contain user-supplied response IDs or query parameters.
        let route = options.route.filter(|route| {
            matches!(
                *route,
                "/v1/chat/completions"
                    | "/v1/completions"
                    | "/generate"
                    | "/v1/messages"
                    | "/v1/models"
                    | "/v1/responses"
                    | "/v1/embeddings"
                    | "/rerank"
                    | "/v1/rerank"
            )
        });
        let span = info_span!(
            target: "otel_trace",
            "http_client_request",
            otel.kind = "client",
            otel.name = %format_args!(
                "{} {}",
                options.method,
                route.unwrap_or("backend")
            ),
            http.request.method = %options.method,
            http.route = Empty,
            request.id = Empty,
            session.id = Empty,
            server.address = Empty,
            server.port = Empty,
            vllm.request_phase = Empty,
            http.response.status_code = Empty,
            error = Empty,
        );

        if let Some(route) = route {
            span.record("http.route", route);
        }
        if let Some(request_phase) = options.request_phase {
            span.record("vllm.request_phase", request_phase);
        }
        if let Some(headers) = incoming_headers {
            crate::otel_trace::record_correlation(&span, headers);
        }
        record_server_address(&span, options.url);

        {
            let _enter = span.enter();
            request = propagate_trace_headers(request, incoming_headers);
        }
        span
    } else {
        request = propagate_trace_headers(request, incoming_headers);
        Span::none()
    };

    PreparedClientRequest { request, span }
}

pub async fn send_client_request(
    request: reqwest::RequestBuilder,
    incoming_headers: Option<&HeaderMap>,
    options: ClientRequestOptions<'_>,
) -> Result<reqwest::Response, reqwest::Error> {
    prepare_client_request(request, incoming_headers, options)
        .send()
        .await
}

pub fn propagate_trace_headers(
    mut request: reqwest::RequestBuilder,
    headers: Option<&HeaderMap>,
) -> reqwest::RequestBuilder {
    if crate::otel_trace::is_otel_enabled() {
        let mut trace_headers = HeaderMap::new();
        crate::otel_trace::inject_request_context(&mut trace_headers, headers);
        for (name, value) in trace_headers.iter() {
            request = request.header(name, value);
        }
        return request;
    }

    if let Some(headers) = headers {
        for &name in TRACE_HEADER_NAMES {
            if let Some(value) = headers.get(name) {
                request = request.header(name, value);
            }
        }
    }

    request
}

fn record_server_address(span: &Span, url: &str) {
    let Ok(parsed) = Url::parse(url) else {
        return;
    };

    if let Some(host) = parsed.host_str() {
        span.record("server.address", host);
    }

    if let Some(port) = parsed.port_or_known_default() {
        span.record("server.port", port);
    }
}
