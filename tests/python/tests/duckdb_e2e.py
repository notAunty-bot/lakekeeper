import conftest
import duckdb
import requests


def sql_string(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def connect(warehouse: conftest.Warehouse):
    connection = duckdb.connect()
    connection.execute("INSTALL iceberg; LOAD iceberg; INSTALL spatial; LOAD spatial")
    connection.execute(
        f"CREATE SECRET catalog_token (TYPE iceberg, TOKEN {sql_string(warehouse.access_token)})"
    )
    warehouse_reference = f"{warehouse.project_id}/{warehouse.warehouse_name}"
    connection.execute(
        f"ATTACH {sql_string(warehouse_reference)} AS lakekeeper "
        f"(TYPE iceberg, SECRET catalog_token, "
        f"URI {sql_string(warehouse.server.catalog_url.rstrip('/'))})"
    )
    return connection


def test_v3_geometry_round_trip(namespace: conftest.Namespace):
    warehouse = namespace.warehouse
    schema_name = '"' + namespace.name[0].replace('"', '""') + '"'
    table = f"lakekeeper.{schema_name}.geo_table"
    with connect(warehouse) as connection:
        connection.execute(
            f"CREATE TABLE {table} (id BIGINT, point GEOMETRY('EPSG:3857')) "
            "WITH ('format-version' = '3')"
        )
        connection.execute(
            f"INSERT INTO {table} VALUES (1, 'POINT (1 2)'::GEOMETRY('EPSG:3857'))"
        )
        connection.execute(
            f"ALTER TABLE {table} ADD COLUMN route GEOMETRY('EPSG:4326')"
        )

    response = requests.get(
        f"{warehouse.server.catalog_url.rstrip('/')}/v1/{warehouse.warehouse_id}"
        f"/namespaces/{namespace.url_name}/tables/geo_table",
        headers={"Authorization": f"Bearer {warehouse.access_token}"},
    )
    response.raise_for_status()
    metadata = response.json()["metadata"]
    assert metadata["format-version"] == 3
    schema = next(
        schema
        for schema in metadata["schemas"]
        if schema["schema-id"] == metadata["current-schema-id"]
    )
    assert [(field["name"], field["type"]) for field in schema["fields"]] == [
        ("id", "long"),
        ("point", "geometry(EPSG:3857)"),
        ("route", "geometry(EPSG:4326)"),
    ]

    with connect(warehouse) as connection:
        assert connection.execute(
            f"SELECT id, ST_AsText(point), route IS NULL FROM {table} ORDER BY id"
        ).fetchall() == [(1, "POINT (1 2)", True)]
