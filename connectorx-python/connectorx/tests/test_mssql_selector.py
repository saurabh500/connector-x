"""Runtime routing checks: environment changes are confined to child processes."""
import os
import subprocess
import sys

import pytest


def run_child(code, backend, **variables):
    env = os.environ.copy()
    env.pop("CONNECTORX_MSSQL_BACKEND", None)
    if backend is not None:
        env["CONNECTORX_MSSQL_BACKEND"] = backend
    env.update(variables)
    env["RUST_LOG"] = "connectorx::mssql_backend=debug"
    result = subprocess.run(
        [sys.executable, "-c", code], env=env, capture_output=True, text=True, timeout=120
    )
    # Do not expose the private connection string if a driver reports it.
    error = result.stderr.replace(env.get("CX_SELECTOR_URI", "unused-uri"), "<connection>")
    assert result.returncode == 0, error
    return result.stdout, error


@pytest.mark.parametrize("backend", ["", "invalid", "Tiberius", "mssql-tds "])
@pytest.mark.parametrize("operation", ["pandas", "arrow", "arrow_stream", "meta", "partition"])
def test_bad_selector_before_database_io(backend, operation):
    run_child(
        """
import os
import connectorx as cx
uri = "mssql://127.0.0.1:1/db"
operation = os.environ["CX_SELECTOR_OPERATION"]
try:
    if operation == "meta":
        cx.get_meta(uri, "SELECT 1 AS id")
    elif operation == "partition":
        cx.partition_sql(uri, "SELECT 1 AS id", "id", 2, (1, 2))
    else:
        cx.read_sql(uri, "SELECT 1 AS id", return_type=operation,
                    partition_on="id", partition_num=2)
except RuntimeError as error:
    assert "CONNECTORX_MSSQL_BACKEND" in str(error), str(error)
else:
    raise AssertionError("invalid selector accepted")
""",
        backend,
        CX_SELECTOR_OPERATION=operation,
    )


def test_non_mssql_ignores_invalid_selector(tmp_path):
    run_child(
        """
import os
import sqlite3
import connectorx as cx
path = os.environ["CX_SQLITE_PATH"]
with sqlite3.connect(path) as db:
    db.execute("CREATE TABLE numbers (id INTEGER)")
    db.execute("INSERT INTO numbers VALUES (1)")
for output in ("pandas", "arrow", "arrow_stream"):
    result = cx.read_sql("sqlite://" + path, "SELECT id FROM numbers", return_type=output)
    if output == "arrow_stream":
        result = result.read_all()
    assert len(result) == 1
""",
        "invalid",
        CX_SQLITE_PATH=str(tmp_path / "selector.sqlite"),
    )


def test_bridge_stream_source_configuration_error():
    run_child(
        """
import connectorx as cx
try:
    cx.read_sql("mssql://127.0.0.1:1/db", "SELECT 1 AS id", return_type="arrow_stream")
except RuntimeError as error:
    assert "encrypt" in str(error), str(error)
else:
    raise AssertionError("bridge source configuration error was not returned")
""",
        "mssql-tds",
    )


@pytest.fixture
def selector_uri():
    # Opt-in, read-only live checks; do not create or change any server data.
    uri = os.environ.get("MSSQL_SELECTOR_TEST_URL")
    if not uri:
        pytest.skip("MSSQL_SELECTOR_TEST_URL is not configured")
    return uri


@pytest.mark.parametrize("backend", [None, "tiberius", "mssql-tds"])
@pytest.mark.parametrize("output", ["pandas", "arrow", "arrow_stream", "polars"])
@pytest.mark.parametrize("shape", ["auto", "explicit", "queries", "empty"])
def test_live_selector_consistent_routing(selector_uri, backend, output, shape):
    _, trace = run_child(
        """
import os
import connectorx as cx
uri = os.environ["CX_SELECTOR_URI"]
output = os.environ["CX_SELECTOR_OUTPUT"]
shape = os.environ["CX_SELECTOR_SHAPE"]
initial = os.environ.get("CONNECTORX_MSSQL_BACKEND")
query = "SELECT id FROM (VALUES (1), (2), (3)) AS data(id)"
kwargs = {}
if shape in ("auto", "explicit"):
    kwargs = dict(partition_on="id", partition_num=2)
    if shape == "explicit":
        kwargs["partition_range"] = (1, 3)
elif shape == "queries":
    query = ["SELECT 1 AS id", "SELECT id FROM (VALUES (2), (3)) AS data(id)"]
else:
    query += " WHERE id < 0"
for _ in range(2):
    if initial is None:
        os.environ.pop("CONNECTORX_MSSQL_BACKEND", None)
    else:
        os.environ["CONNECTORX_MSSQL_BACKEND"] = initial
    result = cx.read_sql(uri, query, return_type=output, batch_size=2, **kwargs)
    # No existing stream may re-read the selector, even after EOF.
    os.environ["CONNECTORX_MSSQL_BACKEND"] = "invalid"
    if output == "arrow_stream":
        result_stream = result
        result = result.read_all()
        for _ in range(2):
            try:
                result_stream.read_next_batch()
            except StopIteration:
                pass
            else:
                raise AssertionError("stream returned data after EOF")
    if output in ("arrow", "arrow_stream"):
        values = result.column("id").to_pylist()
    else:
        values = result["id"].to_list() if output == "polars" else result["id"].tolist()
    assert sorted(values) == ([] if shape == "empty" else [1, 2, 3]), values
    try:
        cx.read_sql(uri, "SELECT 1 AS id", return_type=output)
    except RuntimeError as error:
        assert "CONNECTORX_MSSQL_BACKEND" in str(error)
    else:
        raise AssertionError("subsequent operation did not resolve again")
""",
        backend,
        CX_SELECTOR_URI=selector_uri,
        CX_SELECTOR_OUTPUT=output,
        CX_SELECTOR_SHAPE=shape,
    )
    selected = backend or "tiberius"
    other = "mssql-tds" if selected == "tiberius" else "tiberius"
    assert f"backend: {other}" not in trace
    assert trace.count(f"MSSQL backend: {selected}") == 2
    assert trace.count(f"MSSQL metadata backend: {selected}") == 2
    assert trace.count(f"MSSQL range backend: {selected}") == (2 if shape == "auto" else 0)
    partitions = 2 if shape in ("auto", "explicit", "queries") else 1
    assert trace.count(f"MSSQL partition backend: {selected}") == 2 * partitions
    if output == "pandas":
        count_kind = "count" if shape in ("auto", "explicit") else "partition count"
        expected = 2 if count_kind == "count" else 2 * partitions
        assert trace.count(f"MSSQL {count_kind} backend: {selected}") == expected


@pytest.mark.parametrize("backend", ["tiberius", "mssql-tds"])
def test_live_standalone_metadata_range_and_stream_errors(selector_uri, backend):
    _, trace = run_child(
        """
import os
import connectorx as cx
uri = os.environ["CX_SELECTOR_URI"]
query = "SELECT id FROM (VALUES (1), (2), (3)) AS data(id)"
assert list(cx.get_meta(uri, query).columns) == ["id"]
assert len(cx.partition_sql(uri, query, "id", 2)) == 2
try:
    cx.read_sql(uri, "SELECT missing_column FROM (VALUES (1)) AS data(id)",
                return_type="arrow_stream")
except RuntimeError:
    pass
else:
    raise AssertionError("stream metadata error was not returned")
assert cx.read_sql(uri, "SELECT 1 AS id", return_type="arrow").num_rows == 1
""",
        backend,
        CX_SELECTOR_URI=selector_uri,
    )
    assert trace.count(f"MSSQL range backend: {backend}") == 1
    assert trace.count(f"MSSQL metadata backend: {backend}") == 3
