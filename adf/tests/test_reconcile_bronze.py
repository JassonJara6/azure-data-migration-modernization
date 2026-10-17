"""Unit tests for the read-only Bronze reconciler (pytest + pyarrow)."""
import io
import importlib.util
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

spec = importlib.util.spec_from_file_location(
    "reconcile_bronze", Path(__file__).resolve().parents[1] / "tools" / "reconcile_bronze.py"
)
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)


def sample(run_id="run-123", n=2, include_metadata=True):
    fields = {
        "ProductID": [1, 2][:n],
        "_pipeline_run_id": [run_id] * n,
        "_parent_pipeline_run_id": ["parent"] * n,
        "_source_system": ["adventureworks"] * n,
        "_load_type": ["INCREMENTAL"] * n,
        "_watermark_lower": ["1900-01-01"] * n,
        "_watermark_upper": ["2026-01-01"] * n,
        "_watermark_lower_key_json": ["{}"] * n,
        "_watermark_upper_key_json": ["{}"] * n,
        "_ingested_at_utc": ["2026-10-16T00:00:00Z"] * n,
    }
    if not include_metadata:
        del fields["_source_system"]
    sink = io.BytesIO()
    pq.write_table(pa.table(fields), sink)
    return ("part-00000.parquet", sink.getvalue())


def test_success():
    result = module.reconcile([sample()], 2, "run-123")
    assert result["reconciled_rows"] == 2
    assert result["status"] == "RECONCILED_READ_ONLY"


@pytest.mark.parametrize("files,expected,run_id", [
    ([], 0, "run-123"),
    ([sample()], 3, "run-123"),
    ([sample()], 2, "wrong-run"),
    ([sample(include_metadata=False)], 2, "run-123"),
    ([("unexpected.txt", b"bad")], 0, "run-123"),
    ([sample(), sample()], 4, "run-123"),
])
def test_fail_closed(files, expected, run_id):
    with pytest.raises(ValueError):
        module.reconcile(files, expected, run_id)
