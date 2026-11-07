use axum::Json;
use axum::extract::{Request, State};
use axum::http::{HeaderMap, HeaderValue, StatusCode, header};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use simulmt_api::ErrorInfo;
use std::collections::HashSet;
use std::net::SocketAddr;
use std::sync::Arc;

#[derive(Clone)]
pub(crate) struct LocalAuth {
    token: Arc<str>,
    hosts: Arc<HashSet<String>>,
    origins: Arc<HashSet<String>>,
}

impl LocalAuth {
    pub fn new(address: SocketAddr, token: String) -> Self {
        let port = address.port();
        let hosts = ["127.0.0.1", "localhost", "[::1]"]
            .into_iter()
            .map(|host| format!("{host}:{port}"))
            .collect();
        let origins = ["127.0.0.1", "localhost", "[::1]"]
            .into_iter()
            .map(|host| format!("http://{host}:{port}"))
            .collect();
        Self {
            token: Arc::from(token),
            hosts: Arc::new(hosts),
            origins: Arc::new(origins),
        }
    }

    pub fn accepts_token(&self, candidate: &str) -> bool {
        constant_time_equal(self.token.as_bytes(), candidate.as_bytes())
    }

    fn valid_host(&self, headers: &HeaderMap) -> bool {
        single_header(headers, header::HOST)
            .is_some_and(|host| self.hosts.contains(&host.to_ascii_lowercase()))
    }

    fn valid_origin(&self, headers: &HeaderMap) -> bool {
        let mut values = headers.get_all(header::ORIGIN).iter();
        let Some(origin) = values.next() else {
            // Command-line clients do not send Origin. Browser requests and WS
            // upgrades are still bound by their bearer/first-message credential.
            return true;
        };
        values.next().is_none()
            && origin
                .to_str()
                .ok()
                .is_some_and(|origin| self.origins.contains(origin))
    }
}

pub(crate) async fn authorize(
    State(auth): State<LocalAuth>,
    request: Request,
    next: Next,
) -> Response {
    if !auth.valid_host(request.headers()) {
        return security_error(
            StatusCode::FORBIDDEN,
            "invalid_host",
            "untrusted Host header",
        );
    }
    if !auth.valid_origin(request.headers()) {
        return security_error(
            StatusCode::FORBIDDEN,
            "invalid_origin",
            "untrusted Origin header",
        );
    }

    let path = request.uri().path();
    let protected_api = path.starts_with("/api/") && path != "/api/health";
    let websocket = path.starts_with("/api/sessions/") && path.ends_with("/ws");
    if protected_api && !websocket {
        let authorized = single_header(request.headers(), header::AUTHORIZATION)
            .and_then(|value| value.strip_prefix("Bearer "))
            .is_some_and(|token| auth.accepts_token(token));
        if !authorized {
            let mut response = security_error(
                StatusCode::UNAUTHORIZED,
                "unauthorized",
                "a valid startup bearer token is required",
            );
            response
                .headers_mut()
                .insert(header::WWW_AUTHENTICATE, HeaderValue::from_static("Bearer"));
            return response;
        }
    }
    next.run(request).await
}

pub(crate) fn security_error(status: StatusCode, code: &str, message: &str) -> Response {
    let body = ErrorInfo {
        code: code.into(),
        component_id: "local_security".into(),
        message: message.into(),
        retryable: matches!(
            status,
            StatusCode::TOO_MANY_REQUESTS
                | StatusCode::SERVICE_UNAVAILABLE
                | StatusCode::GATEWAY_TIMEOUT
        ),
        fatal: false,
        request_id: String::new(),
    };
    let mut response = (status, Json(body)).into_response();
    response
        .headers_mut()
        .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    response
}

fn single_header(headers: &HeaderMap, name: header::HeaderName) -> Option<&str> {
    let mut values = headers.get_all(name).iter();
    let value = values.next()?.to_str().ok()?;
    if values.next().is_some() {
        return None;
    }
    Some(value)
}

fn constant_time_equal(expected: &[u8], candidate: &[u8]) -> bool {
    let mut difference = expected.len() ^ candidate.len();
    let longest = expected.len().max(candidate.len());
    for index in 0..longest {
        let left = expected.get(index).copied().unwrap_or(0);
        let right = candidate.get(index).copied().unwrap_or(0);
        difference |= usize::from(left ^ right);
    }
    difference == 0
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn token_comparison_handles_different_lengths() {
        assert!(constant_time_equal(b"secret", b"secret"));
        assert!(!constant_time_equal(b"secret", b"secre"));
        assert!(!constant_time_equal(b"secret", b"secrex"));
        assert!(!constant_time_equal(b"", b"x"));
    }
}
