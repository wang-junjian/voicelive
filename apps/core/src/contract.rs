use axum::body::{Body, to_bytes};
use axum::extract::Request;
use axum::http::{HeaderValue, StatusCode, header};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use simulmt_api::ErrorInfo;
use uuid::Uuid;

const MAX_ERROR_BODY_BYTES: usize = 1024 * 1024;

/// Assigns a server-generated request ID to every response and normalizes
/// framework-produced failures (JSON rejection, 404, 405, body limits) too.
pub(crate) async fn request_context(request: Request, next: Next) -> Response {
    let request_id = Uuid::new_v4().to_string();
    let response = next.run(request).await;
    let status = response.status();
    let mut response = if status.is_client_error() || status.is_server_error() {
        normalize_error(response, &request_id).await
    } else {
        response
    };
    response.headers_mut().insert(
        "x-request-id",
        HeaderValue::from_str(&request_id).expect("UUID is a valid header"),
    );
    response
}

async fn normalize_error(response: Response, request_id: &str) -> Response {
    let status = response.status();
    let (mut parts, body) = response.into_parts();
    let body = match to_bytes(body, MAX_ERROR_BODY_BYTES).await {
        Ok(body) => body,
        Err(_) => return error_response(StatusCode::INTERNAL_SERVER_ERROR, request_id, None),
    };
    let is_json = parts
        .headers
        .get(header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| value.starts_with("application/json"));
    if is_json
        && let Ok(mut value) = serde_json::from_slice::<serde_json::Value>(&body)
        && let Some(object) = value.as_object_mut()
    {
        object.insert("request_id".into(), request_id.into());
        parts.headers.remove(header::CONTENT_LENGTH);
        parts
            .headers
            .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
        let encoded = serde_json::to_vec(&value).expect("serialize normalized error");
        return Response::from_parts(parts, Body::from(encoded));
    }
    let message = String::from_utf8_lossy(&body).trim().to_owned();
    error_response(status, request_id, (!message.is_empty()).then_some(message))
}

fn error_response(status: StatusCode, request_id: &str, message: Option<String>) -> Response {
    let code = match status {
        StatusCode::BAD_REQUEST => "invalid_request",
        StatusCode::UNAUTHORIZED => "unauthorized",
        StatusCode::FORBIDDEN => "forbidden",
        StatusCode::NOT_FOUND => "not_found",
        StatusCode::METHOD_NOT_ALLOWED => "method_not_allowed",
        StatusCode::CONFLICT => "invalid_state",
        StatusCode::PAYLOAD_TOO_LARGE => "payload_too_large",
        StatusCode::UNPROCESSABLE_ENTITY => "invalid_json",
        StatusCode::TOO_MANY_REQUESTS => "too_many_requests",
        StatusCode::SERVICE_UNAVAILABLE => "service_unavailable",
        StatusCode::GATEWAY_TIMEOUT => "operation_timeout",
        _ if status.is_server_error() => "internal_error",
        _ => "request_failed",
    };
    let retryable = matches!(
        status,
        StatusCode::REQUEST_TIMEOUT
            | StatusCode::TOO_MANY_REQUESTS
            | StatusCode::BAD_GATEWAY
            | StatusCode::SERVICE_UNAVAILABLE
            | StatusCode::GATEWAY_TIMEOUT
    );
    let body = ErrorInfo {
        code: code.into(),
        component_id: "http".into(),
        message: message.unwrap_or_else(|| {
            status
                .canonical_reason()
                .unwrap_or("request failed")
                .to_string()
        }),
        retryable,
        fatal: status.is_server_error(),
        request_id: request_id.into(),
    };
    let mut response = (status, axum::Json(body)).into_response();
    response
        .headers_mut()
        .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    response
}
