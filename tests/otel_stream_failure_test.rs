//! Response-body errors must fail the Router span after HTTP 200 has been sent.

use axum::{body::Body, http::Request, response::Response, routing::post, Router};
use http_body_util::BodyExt;
use opentelemetry::trace::{SpanId, Status, TraceId, TracerProvider as _};
use opentelemetry_sdk::{testing::trace::InMemorySpanExporter, trace::TracerProvider};
use std::sync::{Arc, Mutex};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    sync::oneshot,
    time::Duration,
};
use tower::{Layer, ServiceExt};
use tower_http::classify::{ClassifiedResponse, ClassifyResponse};
use tracing_subscriber::layer::SubscriberExt;
use vllm_router_rs::{middleware, otel_http, otel_trace};

#[derive(Clone, Default)]
struct LatencyRecords(Arc<Mutex<Vec<f64>>>);

impl tracing::field::Visit for LatencyRecords {
    fn record_f64(&mut self, field: &tracing::field::Field, value: f64) {
        if field.name() == "latency_ms" {
            self.0.lock().unwrap().push(value);
        }
    }

    fn record_debug(&mut self, _: &tracing::field::Field, _: &dyn std::fmt::Debug) {}
}

impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for LatencyRecords {
    fn on_record(
        &self,
        _: &tracing::span::Id,
        record: &tracing::span::Record,
        _: tracing_subscriber::layer::Context<S>,
    ) {
        record.record(&mut self.clone());
    }
}

#[tokio::test(flavor = "current_thread")]
async fn upstream_body_failure_preserves_http_status_and_marks_router_error() {
    // Keep all cases together: the environment and OTel activation are global.
    std::env::set_var("OTEL_SERVICE_NAME", "lmdeploy-router");
    let exporter = InMemorySpanExporter::default();
    let provider = TracerProvider::builder()
        .with_simple_exporter(exporter.clone())
        .build();
    let prepared = otel_trace::PreparedOtel::from_parts(provider.clone(), provider.tracer("test"));
    let latencies = LatencyRecords::default();
    let subscriber = tracing_subscriber::registry()
        .with(prepared.layer())
        .with(latencies.clone());
    let _guard = tracing::subscriber::set_default(subscriber);
    otel_trace::activate_otel(prepared);

    for path in ["/v1/chat/completions", "/v1/messages", "/v1/responses"] {
        for (status, broken) in [
            (200, true),
            (200, false),
            (400, true),
            (400, false),
            (503, false),
        ] {
            exporter.reset();
            latencies.0.lock().unwrap().clear();
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let endpoint = format!("http://{}", listener.local_addr().unwrap());
            let (release, released) = oneshot::channel();
            let first = b"data: {\"content\":\"test\"}\n\n";
            let worker = tokio::spawn(async move {
                let (mut socket, _) = listener.accept().await.unwrap();
                let mut headers = Vec::new();
                while !headers.ends_with(b"\r\n\r\n") {
                    headers.push(socket.read_u8().await.unwrap());
                    assert!(headers.len() <= 8192);
                }
                // Keep the socket open until
                // the downstream reader has consumed the first real SSE chunk.
                socket.write_all(format!("HTTP/1.1 {status} Test\r\nContent-Type: text/event-stream\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n").as_bytes()).await.unwrap();
                socket
                    .write_all(format!("{:x}\r\n", first.len()).as_bytes())
                    .await
                    .unwrap();
                socket.write_all(first).await.unwrap();
                socket.write_all(b"\r\n").await.unwrap();
                tokio::time::timeout(Duration::from_secs(5), released)
                    .await
                    .unwrap()
                    .unwrap();
                if !broken {
                    socket.write_all(b"0\r\n\r\n").await.unwrap();
                }
                socket.shutdown().await.unwrap();
            });
            let client = reqwest::Client::builder().no_proxy().build().unwrap();
            let app = Router::new()
                .route(
                    path,
                    post(move |headers: axum::http::HeaderMap| {
                        let client = client.clone();
                        let endpoint = endpoint.clone();
                        async move {
                            let response = otel_http::send_client_request(
                                client.post(&endpoint),
                                Some(&headers),
                                otel_http::ClientRequestOptions {
                                    method: "POST",
                                    url: &endpoint,
                                    route: Some(path),
                                    request_phase: None,
                                },
                            )
                            .await
                            .unwrap();
                            Response::builder()
                                .status(response.status())
                                .header("content-type", "text/event-stream")
                                .body(Body::from_stream(response.bytes_stream()))
                                .unwrap()
                        }
                    }),
                )
                .layer(middleware::create_logging_layer());
            let response = tokio::time::timeout(
                Duration::from_secs(5),
                app.oneshot(
                    Request::post(path)
                        .header(
                            "traceparent",
                            "00-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-bbbbbbbbbbbbbbbb-01",
                        )
                        .header("x-request-id", "req_stream_failure_1234")
                        .body(Body::empty())
                        .unwrap(),
                ),
            )
            .await
            .unwrap()
            .unwrap();
            assert_eq!(response.status(), status);
            let mut body = response.into_body();
            let mut received = Vec::new();
            tokio::time::timeout(Duration::from_secs(5), async {
                while received.len() < first.len() {
                    let chunk = body.frame().await.unwrap().unwrap().into_data().unwrap();
                    received.extend_from_slice(&chunk);
                }
            })
            .await
            .unwrap();
            assert_eq!(received, first);
            assert!(exporter.get_finished_spans().unwrap().is_empty());
            let header_latencies = latencies.0.lock().unwrap().clone();
            assert_eq!(header_latencies.len(), 1);
            release.send(()).unwrap();
            let next = tokio::time::timeout(Duration::from_secs(5), body.frame())
                .await
                .unwrap();
            assert_eq!(next.as_ref().is_some_and(Result::is_err), broken);
            if !broken {
                assert!(next.is_none());
            }
            drop(body);
            worker.await.unwrap();
            // Body failure latency is a frame interval in tower-http. It must
            // not overwrite the recorded request-to-headers duration.
            assert_eq!(*latencies.0.lock().unwrap(), header_latencies);
            provider.force_flush();
            let spans = exporter.get_finished_spans().unwrap();
            // LMDeploy inference forwarding propagates the Router context
            // directly; no intermediate backend HTTP CLIENT span is exported.
            assert_eq!(spans.len(), 1, "{path}: {spans:?}");
            let span = &spans[0];
            assert_eq!(span.name, "model.router");
            assert_eq!(
                matches!(span.status, Status::Error { .. }),
                broken || status >= 500,
                "{path}"
            );
            assert_eq!(
                span.span_context.trace_id(),
                TraceId::from_hex("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa").unwrap()
            );
            assert_eq!(
                span.parent_span_id,
                SpanId::from_hex("bbbbbbbbbbbbbbbb").unwrap()
            );
            assert!(span
                .attributes
                .iter()
                .any(|kv| kv.key.as_str() == "http.response.status_code"
                    && kv.value.to_string() == status.to_string()));
            assert!(!span
                .attributes
                .iter()
                .any(|kv| kv.key.as_str() == "envoy.error"));
        }
    }

    // A service failure before any response still records request latency.
    exporter.reset();
    latencies.0.lock().unwrap().clear();
    let service =
        middleware::create_logging_layer().layer(tower::service_fn(|_: Request<Body>| async {
            Err::<Response, _>(std::io::Error::other("backend unavailable"))
        }));
    assert!(service
        .oneshot(
            Request::post("/v1/chat/completions")
                .body(Body::empty())
                .unwrap()
        )
        .await
        .is_err());
    assert_eq!(latencies.0.lock().unwrap().len(), 1);
    let spans = exporter.get_finished_spans().unwrap();
    assert_eq!(spans.len(), 1);
    assert!(matches!(spans[0].status, Status::Error { .. }));
    otel_trace::shutdown_otel();
    assert!(matches!(
        middleware::HttpFailureClassifier.classify_response(&Response::new(Body::empty())),
        ClassifiedResponse::Ready(Ok(()))
    ));
}
