use crate::{
    DirectionSelection, Health, LatencySelection, MediaUploadQuery, SessionTitle,
    SocketAuthentication, SourceSelection, media,
};
use schemars::JsonSchema;
use schemars::generate::SchemaSettings;
use serde_json::{Map, Value, json};
use simulmt_api::*;

#[derive(JsonSchema)]
#[allow(dead_code)]
struct SchemaInventory {
    health: Health,
    default_config: PipelineConfig,
    session: SessionSnapshot,
    sessions: Vec<SessionSnapshot>,
    sources: Vec<SourceInfo>,
    imported_media: media::ImportedMedia,
    title: SessionTitle,
    source_selection: SourceSelection,
    direction: DirectionSelection,
    latency: LatencySelection,
    media_query: MediaUploadQuery,
    history: Vec<SubtitleCue>,
    stream_message: SessionStreamMessage,
    socket_authentication: SocketAuthentication,
    error: ErrorInfo,
}

pub(crate) fn document_yaml() -> String {
    serde_yaml::to_string(&document()).expect("serialize OpenAPI document")
}

fn document() -> Value {
    let settings = SchemaSettings::openapi3().with(|settings| {
        settings.meta_schema = None;
    });
    let root = settings
        .into_generator()
        .into_root_schema_for::<SchemaInventory>();
    let mut schemas = root
        .as_object()
        .and_then(|object| object.get("components"))
        .and_then(Value::as_object)
        .and_then(|components| components.get("schemas"))
        .and_then(Value::as_object)
        .cloned()
        .expect("OpenAPI component schemas");
    schemas.remove("SchemaInventory");

    let snapshot = response("SessionSnapshot", "Current authoritative session snapshot");
    let history = array_response("SubtitleCue", "Complete session subtitle history");
    let session_id = || {
        json!([{
            "name": "id", "in": "path", "required": true,
            "schema": {"type": "string", "format": "uuid"}
        }])
    };
    let command = |method: &str, body: Option<&str>, errors: Vec<u16>| {
        let mut responses = Map::new();
        responses.insert("200".into(), snapshot.clone());
        for status in errors {
            responses.insert(status.to_string(), error_response());
        }
        let mut operation = json!({"responses": Value::Object(responses)});
        if let Some(schema) = body {
            operation["requestBody"] = json!({
                "required": true,
                "content": {"application/json": {"schema": schema_ref(schema)}}
            });
        }
        json!({method: operation})
    };

    let mut paths = Map::new();
    paths.insert(
        "/api/health".into(),
        json!({"get": {
            "security": [],
            "responses": {"200": response("Health", "Core liveness and process identity")}
        }}),
    );
    paths.insert("/api/default-config".into(), json!({"get": {
        "responses": {"200": response("PipelineConfig", "Default public pipeline configuration")}
    }}));
    paths.insert(
        "/api/sources".into(),
        json!({"get": {
            "responses": {
                "200": array_response("SourceInfo", "Available native audio sources"),
                "503": error_response()
            }
        }}),
    );
    paths.insert("/api/media".into(), json!({"post": {
        "parameters": [{
            "name": "name", "in": "query", "required": false,
            "schema": {"type": "string"}
        }],
        "requestBody": {
            "required": true,
            "content": {"application/octet-stream": {"schema": {"type": "string", "format": "binary"}}}
        },
        "responses": {
            "200": response("ImportedMedia", "Imported normalized WAV"),
            "400": error_response(), "413": error_response(), "422": error_response(),
            "429": error_response(), "500": error_response(), "504": error_response(),
            "507": error_response()
        }
    }}));
    paths.insert("/api/sessions".into(), json!({
        "get": {"responses": {"200": array_response("SessionSnapshot", "All sessions")}},
        "post": {
            "requestBody": {"required": true, "content": {"application/json": {"schema": schema_ref("PipelineConfig")}}},
            "responses": {"200": snapshot, "400": error_response(), "422": error_response()}
        }
    }));
    paths.insert("/api/sessions/{id}".into(), json!({
        "parameters": session_id(),
        "get": {"responses": {"200": snapshot, "404": error_response()}},
        "delete": {"responses": {"204": {"description": "Session deleted"}, "404": error_response(), "409": error_response()}}
    }));

    let session_routes = [
        ("/title", "put", Some("SessionTitle")),
        ("/config", "put", Some("PipelineConfig")),
        ("/prepare", "post", None),
        ("/start", "post", None),
        ("/pause", "post", None),
        ("/resume", "post", None),
        ("/stop", "post", None),
        ("/stop-model", "post", None),
        ("/source", "put", Some("SourceSelection")),
        ("/latency-mode", "put", Some("LatencySelection")),
        ("/direction", "put", Some("DirectionSelection")),
    ];
    for (suffix, method, body) in session_routes {
        let mut item = json!({"parameters": session_id()});
        item[method] = command(method, body, vec![400, 404, 409, 422, 500, 503])[method].take();
        paths.insert(format!("/api/sessions/{{id}}{suffix}"), item);
    }
    paths.insert("/api/sessions/{id}/history".into(), json!({
        "parameters": session_id(),
        "get": {"responses": {"200": history, "404": error_response(), "500": error_response()}},
        "delete": {"responses": {"200": history, "404": error_response(), "409": error_response(), "500": error_response()}}
    }));
    for (extension, content_type) in [
        ("srt", "application/x-subrip"),
        ("vtt", "text/vtt"),
        ("wav", "audio/wav"),
    ] {
        paths.insert(
            format!("/api/sessions/{{id}}/export.{extension}"),
            json!({
                "parameters": session_id(),
                "get": {"responses": {
                    "200": {"description": "Strict snapshot export", "content": {
                        content_type: {"schema": {"type": "string", "format": "binary"}}
                    }},
                    "400": error_response(), "404": error_response(), "500": error_response(),
                    "503": error_response()
                }}
            }),
        );
    }
    paths.insert("/api/sessions/{id}/ws".into(), json!({
        "parameters": session_id(),
        "get": {
            "security": [],
            "description": "Authenticate in the first WebSocket message, then receive an atomic baseline and ordered events. The token is never accepted in the URL.",
            "responses": {
                "101": {"description": "WebSocket upgrade; messages use SessionStreamMessage"},
                "403": error_response(), "404": error_response()
            }
        }
    }));

    json!({
        "openapi": "3.0.4",
        "info": {
            "title": "EchoNote Local Control Plane",
            "version": API_VERSION,
            "description": "Loopback-only, bearer-authenticated REST and WebSocket API. Error responses use ErrorInfo and every response carries X-Request-Id."
        },
        "servers": [{"url": "http://127.0.0.1:8780"}],
        "security": [{"bearerAuth": []}],
        "paths": Value::Object(paths),
        "components": {
            "schemas": Value::Object(schemas),
            "securitySchemes": {"bearerAuth": {"type": "http", "scheme": "bearer"}}
        }
    })
}

fn schema_ref(name: &str) -> Value {
    json!({"$ref": format!("#/components/schemas/{name}")})
}

fn response(schema: &str, description: &str) -> Value {
    json!({
        "description": description,
        "headers": {"X-Request-Id": {"schema": {"type": "string", "format": "uuid"}}},
        "content": {"application/json": {"schema": schema_ref(schema)}}
    })
}

fn array_response(item_schema: &str, description: &str) -> Value {
    json!({
        "description": description,
        "headers": {"X-Request-Id": {"schema": {"type": "string", "format": "uuid"}}},
        "content": {"application/json": {"schema": {
            "type": "array", "items": schema_ref(item_schema)
        }}}
    })
}

fn error_response() -> Value {
    response(
        "ErrorInfo",
        "Structured error; retryable is authoritative for retry decisions",
    )
}

#[cfg(test)]
mod tests {
    use super::{Map, Value};

    #[test]
    fn checked_in_openapi_matches_rust_contract() {
        assert_eq!(
            include_str!("../../../api/openapi.yaml"),
            super::document_yaml(),
            "run `cargo run -p simulmt-app --locked -- --print-openapi > api/openapi.yaml`"
        );
    }

    #[test]
    fn all_component_references_resolve() {
        fn visit(value: &Value, schemas: &Map<String, Value>) {
            match value {
                Value::Object(object) => {
                    if let Some(reference) = object.get("$ref").and_then(Value::as_str)
                        && let Some(name) = reference.strip_prefix("#/components/schemas/")
                    {
                        assert!(schemas.contains_key(name), "missing schema: {name}");
                    }
                    for child in object.values() {
                        visit(child, schemas);
                    }
                }
                Value::Array(items) => {
                    for child in items {
                        visit(child, schemas);
                    }
                }
                _ => {}
            }
        }

        let document = super::document();
        let schemas = document["components"]["schemas"]
            .as_object()
            .expect("component schemas");
        visit(&document, schemas);
    }
}
