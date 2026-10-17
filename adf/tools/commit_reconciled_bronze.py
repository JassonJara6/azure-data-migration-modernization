"""Run trusted ADLS reconciliation and SQL commit for one COPIED child run.

Separate deployment from ADF; SQL login needs EXECUTE on the attestation and
commit procedures but ADF must NOT receive attestation privileges.

Environment: BRONZE_SQL_CONNECTION_STRING (ODBC connection string), Azure
DefaultAzureCredential, ADLS account via --account. Never print credentials.
"""
from __future__ import annotations

import argparse
import json
import os
import sys

from reconcile_bronze import reconcile


def run(account: str, filesystem: str, run_id: str) -> dict:
    import pyodbc
    from azure.identity import DefaultAzureCredential
    from azure.storage.filedatalake import DataLakeServiceClient

    conn_str = os.environ.get("BRONZE_SQL_CONNECTION_STRING")
    if not conn_str:
        raise ValueError("BRONZE_SQL_CONNECTION_STRING must be set securely")
    if not run_id or len(run_id) > 100 or any(c not in "0123456789abcdefABCDEF-" for c in run_id):
        raise ValueError("Run ID must be a UUID-like identifier")
    with pyodbc.connect(conn_str, autocommit=True, timeout=30) as sql:
        cursor = sql.cursor()
        row = cursor.execute(
            "SELECT bronze_path, expected_rows, copied_rows, status, parent_run_id, load_type "
            "FROM ingestion.bronze_run_audit WHERE child_run_id=?", run_id
        ).fetchone()
        if row is None or row.status != "COPIED":
            raise ValueError("Run not present or not in COPIED state")
        folder = row.bronze_path.strip("/")
        if (not folder.startswith("adventureworks/")
                or folder.split("/")[-1] != "run_id=" + run_id
                or ".." in folder.split("/")):
            raise ValueError("Audit folder is not a valid exact run path")
        if row.expected_rows != row.copied_rows:
            raise ValueError("SQL planned and copied counts differ")
        if row.load_type not in ("FULL", "INCREMENTAL"):
            raise ValueError("Invalid load type in audit")
        client = DataLakeServiceClient(
            account_url=f"https://{account}.dfs.core.windows.net",
            credential=DefaultAzureCredential(),
        ).get_file_system_client(filesystem)
        paths = list(client.get_paths(path=folder, recursive=True))
        prefix = folder + "/"
        if any(p.is_directory or not p.name.startswith(prefix) for p in paths):
            raise ValueError("Nested or unrelated paths in run folder")
        files = [
            (p.name[len(prefix):], client.get_file_client(p.name).download_file().readall())
            for p in paths
        ]
        evidence = reconcile(files, int(row.expected_rows), run_id)
        # Only independently verified data may transition COPIED -> RECONCILED.
        cursor.execute(
            "EXEC ingestion.attest_bronze_reconciliation "
            "@child_run_id=?, @bronze_path=?, @reconciled_rows=?",
            run_id, folder, evidence["reconciled_rows"]
        )
        # Commit is a separate optimistic-locking SQL transaction.
        cursor.execute("EXEC ingestion.commit_bronze_run @child_run_id=?", run_id)
        status = cursor.execute(
            "SELECT status FROM ingestion.bronze_run_audit WHERE child_run_id=?", run_id
        ).fetchval()
        if status != "COMMITTED":
            raise RuntimeError("SQL commit did not reach COMMITTED state")
        return {"run_id": run_id, "status": status, "reconciled_rows": evidence["reconciled_rows"]}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--account", required=True)
    parser.add_argument("--filesystem", default="bronze")
    parser.add_argument("--run-id", required=True)
    args = parser.parse_args()
    try:
        print(json.dumps(run(args.account, args.filesystem, args.run_id), sort_keys=True))
        return 0
    except Exception as exc:
        print(json.dumps({"status": "FAILED", "run_id": args.run_id,
                          "error": str(exc)}), file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
