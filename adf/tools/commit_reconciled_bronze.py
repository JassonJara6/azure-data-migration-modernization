"""Trusted reconciler: read exact ADLS data and persist evidence; never confirm alone.

Default CLI operation is attestation only. Parent coordination commits all eleven
in one SQL transaction. The ADF identity must never receive these permissions.
"""
from __future__ import annotations
import argparse
import json
import os
import re
import sys
from uuid import UUID
from reconcile_bronze import read_and_reconcile, snapshot, strict_json
from bronze_diagnostics import step, report_error, close_resources, fail


def run_id(value):
    if str(UUID(value))!=value: raise ValueError('Expected canonical lowercase UUID')
    return value


class SqlStore:
    def __init__(self,connection):self.connection=connection
    def rows(self,query,*values,stage='sql_query'):
        cursor=None
        with step(stage):
            try:
                cursor=self.connection.cursor()
                cursor.execute(query,*values)
                if cursor.description is None:raise ValueError('Expected SQL result set')
                names=[c[0] for c in cursor.description]
                return [dict(zip(names,row)) for row in cursor.fetchall()]
            finally:close_resources(cursor)
    def execute(self,query,*values,stage='sql_execute'):
        cursor=None
        with step(stage):
            try:
                cursor=self.connection.cursor()
                cursor.execute(query,*values)
                # ODBC can defer a server error until the next TDS result.
                # Do not treat the initial execute acknowledgement as success.
                while cursor.nextset():pass
            finally:close_resources(cursor)
    def validate_session(self):
        rows=self.rows("SELECT CASE WHEN DB_NAME()=N'AdventureWorks' THEN 1 ELSE 0 END AS database_ok",stage='sql_validate_session')
        if len(rows)!=1 or rows[0]['database_ok']!=1:
            fail('sql_validate_session','SQL_WRONG_DATABASE','La conexión SQL no selecciona AdventureWorks.',
                 'Una prueba SELECT 1 no verifica la base. Usa --interactive o especifica DATABASE=AdventureWorks de forma segura.')
    def get_run(self,child):
        rows=self.rows("SELECT config_id,parent_run_id,child_run_id,bronze_path,load_type,expected_rows,copied_rows,status,"
          "primary_key_columns,ISNULL(CONVERT(nvarchar(33),watermark_lower,126),'') AS watermark_lower,"
          "ISNULL(CONVERT(nvarchar(33),watermark_upper,126),'') AS watermark_upper,"
          "ISNULL(watermark_lower_key_json,'') AS lower_key_json,ISNULL(watermark_upper_key_json,'') AS upper_key_json,"
          "manifest_json,manifest_sha256 FROM ingestion.bronze_run_audit WHERE child_run_id=?",child,stage='sql_load_child_receipt')
        if len(rows)!=1:raise ValueError('Missing audit receipt')
        return rows[0]
    def parent_runs(self,parent):
        return self.rows('SELECT config_id,child_run_id,status FROM ingestion.bronze_run_audit WHERE parent_run_id=? ORDER BY config_id',parent,stage='sql_load_parent_children')
    def active_configs(self):
        return self.rows('SELECT config_id,load_type FROM ingestion.table_config WHERE is_active=1 ORDER BY config_id',stage='sql_load_active_configs')
    def parent_receipt(self,parent):
        rows=self.rows('SELECT parent_run_id,committed_children,manifest_sha256 FROM ingestion.bronze_parent_audit WHERE parent_run_id=?',parent,stage='sql_read_parent_receipt')
        return rows[0] if rows else None
    def attest(self,plan,evidence,text,digest):
        self.execute('EXEC ingestion.attest_bronze_reconciliation @child_run_id=?,@bronze_path=?,@reconciled_rows=?,@manifest_json=?,@manifest_sha256=?',
                     plan['child_run_id'],plan['bronze_path'],evidence['reconciled_rows'],text,digest,stage='sql_attest_child')
    def commit_parent(self,parent):self.execute('EXEC ingestion.commit_bronze_parent @parent_run_id=?',parent,stage='sql_commit_parent')


def attest_run(store,client,account,filesystem,child):
    run_id(child)
    plan=store.get_run(child)
    if plan['status'] not in ('COPIED','RECONCILED','COMMITTED') or plan['expected_rows']!=plan['copied_rows']:raise ValueError('Invalid Copy audit state')
    if plan['status']=='COMMITTED':
        if not plan['manifest_sha256']:raise ValueError('Legacy commit has no independent evidence')
        return plan
    with step('adls_reconcile_child'):
        evidence,text,digest=read_and_reconcile(client,plan,account,filesystem)
    store.attest(plan,evidence,text,digest)
    current=store.get_run(child)
    if current['status']!='RECONCILED' or current['manifest_sha256']!=digest:raise RuntimeError('Attestation was not durable')
    return current


def verify_unchanged(client,plans):
    for plan in plans:
        evidence=strict_json(plan['manifest_json'])
        expected={plan['bronze_path']+'/'+f['name']:(f['etag'],f['bytes']) for f in evidence['files']}
        if snapshot(client,plan['bronze_path'],plan['expected_rows'])!=expected:raise ValueError('Destination changed before parent commit')


def connections(account,filesystem,*,connection_string=None,credential=None):
    import pyodbc
    from azure.identity import DefaultAzureCredential
    from azure.storage.filedatalake import DataLakeServiceClient
    from bronze_connection import configured_connection
    if not re.fullmatch(r'[a-z0-9]{3,24}',account) or filesystem!='bronze':raise ValueError('Invalid storage binding')
    connection=connection_string if connection_string is not None else configured_connection()
    sql=service=None
    try:
        with step('sql_connect'):
            sql=pyodbc.connect(connection,autocommit=True,timeout=30)
            sql.timeout=120
        SqlStore(sql).validate_session()
        with step('adls_client'):
            service=DataLakeServiceClient('https://'+account+'.dfs.core.windows.net',credential or DefaultAzureCredential())
        return sql,service
    except Exception:
        close_resources(service,sql)
        raise


def run(account,filesystem,child):
    sql,service=connections(account,filesystem)
    try:
        store=SqlStore(sql);plan=attest_run(store,service.get_file_system_client(filesystem),account,filesystem,child)
        receipt=store.get_run(child)
        return {'run_id':child,'status':receipt['status'],'manifest_sha256':receipt['manifest_sha256']}
    finally:close_resources(service,sql)


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--account',required=True);parser.add_argument('--filesystem',default='bronze')
    parser.add_argument('--run-id',required=True)
    args=parser.parse_args()
    try:print(json.dumps(run(args.account,args.filesystem,args.run_id),sort_keys=True));return 0
    except Exception as exc:
        print(json.dumps(report_error(exc),ensure_ascii=True),file=sys.stderr);return 1

if __name__=='__main__':sys.exit(main())
