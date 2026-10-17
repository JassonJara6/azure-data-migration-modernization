"""Read-only reconciliation of one immutable Bronze child-run folder in ADLS Gen2.

Install: pip install azure-identity azure-storage-file-datalake pyarrow
Usage:
  python adf/tools/reconcile_bronze.py --account stadmmdevdlf6d99ba4 \
    --folder adventureworks/production/product/load_type=incremental/ingestion_date=2026-10-16/run_id=<CHILD_RUN_ID> \
    --expected-rows 504 --run-id <CHILD_RUN_ID>

Authenticates with DefaultAzureCredential. Prints JSON evidence, exits nonzero
on mismatches. Does NOT write SQL audit rows or commit watermarks.
"""
from __future__ import annotations

import argparse
import io
import json
import sys


REQUIRED_METADATA = {
    "_pipeline_run_id",
    "_parent_pipeline_run_id",
    "_source_system",
    "_load_type",
    "_watermark_lower",
    "_watermark_upper",
    "_watermark_lower_key_json",
    "_watermark_upper_key_json",
    "_ingested_at_utc",
}


def reconcile(files: list[tuple[str, bytes]], expected_rows: int, run_id: str) -> dict:
    """Pure validator; bytes must come from the exact run folder, never a wildcard."""
    import pyarrow.parquet as pq

    if expected_rows < 0 or not run_id:
        raise ValueError("expected_rows must be nonnegative and run_id nonempty")
    if not files:
        raise ValueError("No Parquet files found in the exact run folder")
    names = [name for name, _ in files]
    if len(names) != len(set(names)):
        raise ValueError("Duplicate file names in reconciliation input")
    if any(not name.endswith(".parquet") or "/" in name for name in names):
        raise ValueError("Unexpected file name or non-Parquet object in run folder")

    total = 0
    schemas = set()
    for name, data in files:
        parquet = pq.ParquetFile(io.BytesIO(data))
        schema = parquet.schema_arrow
        missing = REQUIRED_METADATA - set(schema.names)
        if missing:
            raise ValueError(f"{name}: missing ingestion metadata: {sorted(missing)}")
        schemas.add(str(schema))
        # Scan actual destination data, not ADF's copy counters or Parquet footer count.
        for batch in parquet.iter_batches(batch_size=65536):
            ids = batch.column(batch.schema.get_field_index("_pipeline_run_id"))
            sources = batch.column(batch.schema.get_field_index("_source_system"))
            if any(value.as_py() != run_id for value in ids):
                raise ValueError(f"{name}: child run ID mismatch")
            if any(value.as_py() != "adventureworks" for value in sources):
                raise ValueError(f"{name}: source system mismatch")
            total += batch.num_rows
    if len(schemas) != 1:
        raise ValueError("Parquet schema mismatch between files")
    if total != expected_rows:
        raise ValueError(f"Destination rows {total} != planned rows {expected_rows}")
    return {"status": "RECONCILED_READ_ONLY", "run_id": run_id,
            "expected_rows": expected_rows, "reconciled_rows": total,
            "files": names, "note": "Not an audited or committed ingestion"}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--account", required=True)
    parser.add_argument("--filesystem", default="bronze")
    parser.add_argument("--folder", required=True)
    parser.add_argument("--expected-rows", type=int, required=True)
    parser.add_argument("--run-id", required=True)
    args = parser.parse_args()
    try:
        from azure.identity import DefaultAzureCredential
        from azure.storage.filedatalake import DataLakeServiceClient

        folder = args.folder.strip("/")
        if not folder.startswith("adventureworks/") or ".." in folder.split("/"):
            raise ValueError("Folder must be under adventureworks/ and contain no '..'")
        if folder.split("/")[-1] != "run_id=" + args.run_id:
            raise ValueError("Folder run_id must match --run-id")
        client = DataLakeServiceClient(
            account_url=f"https://{args.account}.dfs.core.windows.net",
            credential=DefaultAzureCredential(),
        ).get_file_system_client(args.filesystem)
        paths = list(client.get_paths(path=folder, recursive=True))
        if any(p.is_directory for p in paths):
            raise ValueError("Nested directories are not allowed within one run")
        files = []
        for p in paths:
            prefix = folder + "/"
            if not p.name.startswith(prefix):
                raise ValueError("Unexpected path outside run folder")
            relative = p.name[len(prefix):]
            data = client.get_file_client(p.name).download_file().readall()
            files.append((relative, data))
        print(json.dumps(reconcile(files, args.expected_rows, args.run_id), sort_keys=True))
        return 0
    except Exception as exc:
        print(json.dumps({"status": "RECONCILIATION_FAILED", "error": str(exc),
                          "run_id": args.run_id}), file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
