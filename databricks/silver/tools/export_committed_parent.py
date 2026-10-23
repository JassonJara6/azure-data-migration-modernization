"""Read-only local SQL export; never modify Bronze state or execute ADF.

Run from the repository with its existing Bronze Python/ODBC environment. The
output is trusted evidence: place it in a write-restricted UC input volume.
"""
import argparse
import json
import sys
from pathlib import Path
ROOT=Path(__file__).resolve().parents[3]
sys.path.insert(0,str(ROOT/'adf/tools'))

QUERY="""
SELECT 1 AS format_version,'COMMITTED' AS status,p.parent_run_id,p.committed_children,
p.manifest_sha256,CONVERT(varchar(33),p.committed_at_utc,126) AS committed_at_utc,
JSON_QUERY((SELECT a.config_id,CONCAT(c.source_schema,'.',c.source_table) AS source,
a.child_run_id,a.status,a.load_type,a.primary_key_columns,a.manifest_json,a.manifest_sha256
FROM ingestion.bronze_run_audit a JOIN ingestion.table_config c ON c.config_id=a.config_id
WHERE a.parent_run_id=p.parent_run_id ORDER BY a.config_id FOR JSON PATH)) AS tables
FROM ingestion.bronze_parent_audit p WHERE p.parent_run_id=? FOR JSON PATH,WITHOUT_ARRAY_WRAPPER
"""


def main():
    from bronze_connection import configured_connection
    from bronze_diagnostics import step,report_error,close_resources
    from commit_reconciled_bronze import SqlStore,run_id
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--parent-run-id',required=True);parser.add_argument('--output',required=True)
    parser.add_argument('--sql-server',default='127.0.0.1,1433');parser.add_argument('--sql-user',default='bronze_reconciler')
    parser.add_argument('--trust-local-sql-certificate',action='store_true')
    args=parser.parse_args();conn=None
    try:
        import pyodbc
        run_id(args.parent_run_id)
        value=configured_connection(interactive=True,server=args.sql_server,user=args.sql_user,trust_local_certificate=args.trust_local_sql_certificate)
        with step('sql_export_committed_evidence'):
            conn=pyodbc.connect(value,autocommit=True,timeout=30);store=SqlStore(conn);store.validate_session()
            # SQL Server may chunk FOR JSON output into multiple rows.
            with conn.cursor() as cursor:
                rows=cursor.execute(QUERY,args.parent_run_id).fetchall()
            text=''.join(row[0] for row in rows);bundle=json.loads(text)
            if bundle.get('committed_children')!=11 or len(bundle.get('tables',[]))!=11:raise ValueError('Incomplete parent evidence')
            with Path(args.output).open('x',encoding='utf-8') as stream:json.dump(bundle,stream,ensure_ascii=True,indent=2)
        print(json.dumps({'status':'EXPORTED','parent_run_id':args.parent_run_id,'children':11}));return 0
    except Exception as exc:
        print(json.dumps(report_error(exc),ensure_ascii=True),file=sys.stderr);return 1
    finally:close_resources(conn)

if __name__=='__main__':raise SystemExit(main())
