use std::collections::BTreeMap;

use http::{Method, Request, StatusCode, header};
use http_body_util::BodyExt as _;
use lakekeeper::{
    api::iceberg::v1::tables,
    axum::{Extension, Router, body::Body},
    server::CatalogServer,
    service::authz::AllowAllAuthorizer,
};
use lakekeeper_integration_tests::{
    SetupTestCatalog, create_ns, memory_io_profile, random_request_metadata,
};
use lakekeeper_io::LakekeeperStorage as _;
use serde_json::{Value, json};
use sqlx::PgPool;
use tower::ServiceExt as _;

async fn setup(pool: PgPool) -> (Router, String) {
    let (ctx, warehouse) = SetupTestCatalog::builder()
        .pool(pool)
        .storage_profile(memory_io_profile())
        .authorizer(AllowAllAuthorizer::default())
        .number_of_warehouses(1)
        .build()
        .setup()
        .await;
    let prefix = warehouse.warehouse_id.to_string();
    create_ns(ctx.clone(), prefix.clone(), "geo".to_string()).await;
    let router = tables::router::<CatalogServer<_, _, _>, _>()
        .with_state(ctx)
        .layer(Extension(random_request_metadata()));
    (router, format!("/{prefix}/namespaces/geo"))
}

async fn request(router: &Router, method: Method, path: &str, body: Value) -> Value {
    let (status, body) = response(router, method, path, body).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    body
}

async fn response(router: &Router, method: Method, path: &str, body: Value) -> (StatusCode, Value) {
    let request = Request::builder()
        .method(method)
        .uri(path)
        .header(header::HOST, "localhost")
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(serde_json::to_vec(&body).unwrap()))
        .unwrap();
    let response = router.clone().oneshot(request).await.unwrap();
    let status = response.status();
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    (status, serde_json::from_slice(&bytes).unwrap())
}

fn schema() -> Value {
    json!({"type": "struct", "schema-id": 0, "fields": [
        {"id": 1, "name": "geom", "required": false, "type": "geometry"},
        {"id": 2, "name": "geog", "required": false, "type": "geography"},
        {"id": 3, "name": "projected", "required": false, "type": "geometry(ePsG:3857)"},
        {"id": 4, "name": "ellipsoid", "required": false, "type": "geography(EPSG:4326, karney)"},
        {"id": 5, "name": "paths", "required": false, "type": {
            "type": "list", "element-id": 8, "element-required": false,
            "element": "geometry(EPSG:27700)"
        }},
        {"id": 6, "name": "places", "required": false, "type": {
            "type": "map", "key-id": 9, "key": "string", "value-id": 10,
            "value-required": false, "value": "geography(OGC:CRS27, vincenty)"
        }},
        {"id": 7, "name": "nested", "required": false, "type": {
            "type": "struct", "fields": [
                {"id": 11, "name": "point", "required": false, "type": "geometry(EPSG:4326)"}
            ]
        }}
    ]})
}

fn current_schema(metadata: &Value) -> &Value {
    metadata["schemas"]
        .as_array()
        .unwrap()
        .iter()
        .find(|schema| schema["schema-id"] == metadata["current-schema-id"])
        .unwrap()
}

fn schema_map(metadata: &Value) -> BTreeMap<i64, Value> {
    metadata["schemas"]
        .as_array()
        .unwrap()
        .iter()
        .map(|schema| (schema["schema-id"].as_i64().unwrap(), schema.clone()))
        .collect()
}

#[sqlx::test]
async fn v3_geo_create_load_commit_register_reload(pool: PgPool) {
    let (router, namespace) = setup(pool).await;
    let tables = format!("{namespace}/tables");
    let table = format!("{tables}/geo_table");
    let created = request(
        &router,
        Method::POST,
        &tables,
        json!({
            "name": "geo_table", "schema": schema(), "properties": {"format-version": "3"}
        }),
    )
    .await;
    assert_eq!(created["metadata"]["format-version"], 3);
    let expected =
        serde_json::to_value(serde_json::from_value::<iceberg::spec::Schema>(schema()).unwrap())
            .unwrap();
    assert_eq!(current_schema(&created["metadata"]), &expected);
    assert_eq!(expected["fields"][0]["type"], "geometry(OGC:CRS84)");
    assert_eq!(
        expected["fields"][1]["type"],
        "geography(OGC:CRS84, spherical)"
    );
    assert_eq!(expected["fields"][2]["type"], "geometry(ePsG:3857)");
    let loaded = request(&router, Method::GET, &table, Value::Null).await;
    assert_eq!(current_schema(&loaded["metadata"]), &expected);

    let mut evolved = expected.clone();
    evolved["schema-id"] = json!(1);
    evolved["fields"].as_array_mut().unwrap().push(json!({
        "id": 12, "name": "added", "required": false, "type": "geography(EPSG:4326, thomas)"
    }));
    let committed = request(
        &router,
        Method::POST,
        &table,
        json!({
            "requirements": [], "updates": [
                {"action": "add-schema", "schema": evolved},
                {"action": "set-current-schema", "schema-id": -1}
            ]
        }),
    )
    .await;
    assert_eq!(current_schema(&committed["metadata"]), &evolved);
    let reloaded = request(&router, Method::GET, &table, Value::Null).await;
    assert_eq!(
        schema_map(&reloaded["metadata"]),
        BTreeMap::from([(0, expected.clone()), (1, evolved.clone())])
    );

    let registered = request(&router, Method::POST, &format!("{namespace}/register"), json!({
        "name": "geo_table", "metadata-location": committed["metadata-location"], "overwrite": true
    })).await;
    assert_eq!(current_schema(&registered["metadata"]), &evolved);
    let reloaded = request(&router, Method::GET, &table, Value::Null).await;
    assert_eq!(
        schema_map(&reloaded["metadata"]),
        BTreeMap::from([(0, expected.clone()), (1, evolved.clone())])
    );
}

#[sqlx::test]
async fn v1_v2_geo_create_commit_register_rejected(pool: PgPool) {
    let (router, namespace) = setup(pool).await;
    let tables = format!("{namespace}/tables");
    for version in ["1", "2"] {
        for geo_type in ["geometry(EPSG:3857)", "geography(EPSG:4326, andoyer)"] {
            let geo_schema = json!({"type": "struct", "schema-id": 1, "fields": [
                {"id": 1, "name": "point", "required": false, "type": geo_type}
            ]});
            let (status, _) = response(&router, Method::POST, &tables, json!({
                "name": "invalid", "schema": geo_schema, "properties": {"format-version": version}
            })).await;
            assert_eq!(status, StatusCode::BAD_REQUEST);

            let name = format!("v{version}_{}", geo_type.split('(').next().unwrap());
            let created = request(
                &router,
                Method::POST,
                &tables,
                json!({
                    "name": name, "schema": {"type": "struct", "schema-id": 0, "fields": [
                        {"id": 1, "name": "id", "required": true, "type": "long"}
                    ]}, "properties": {"format-version": version}
                }),
            )
            .await;
            let table = format!("{tables}/{name}");
            let (status, body) = response(
                &router,
                Method::POST,
                &table,
                json!({
                    "requirements": [], "updates": [{"action": "add-schema", "schema": geo_schema}]
                }),
            )
            .await;
            assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
            assert_eq!(body["error"]["type"], "InvalidSchemaFormatVersion");
            let mut invalid_metadata = created["metadata"].clone();
            invalid_metadata["schemas"]
                .as_array_mut()
                .unwrap()
                .push(geo_schema.clone());
            let invalid_location = format!(
                "{}/invalid.metadata.json",
                invalid_metadata["location"].as_str().unwrap()
            );
            memory_io_profile()
                .file_io(None)
                .await
                .unwrap()
                .write(
                    &invalid_location,
                    serde_json::to_vec(&invalid_metadata).unwrap().into(),
                )
                .await
                .unwrap();
            let (status, body) = response(
                &router,
                Method::POST,
                &format!("{namespace}/register"),
                json!({
                    "name": name, "metadata-location": invalid_location, "overwrite": true
                }),
            )
            .await;
            assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
            assert_eq!(body["error"]["type"], "InvalidSchemaFormatVersion");
            let loaded = request(&router, Method::GET, &table, Value::Null).await;
            assert_eq!(
                current_schema(&loaded["metadata"])["fields"][0]["type"],
                "long"
            );
        }
    }
}
