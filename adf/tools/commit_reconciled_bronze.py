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


def run_id(value):
    if str(UUID(value))!=value: raise ValueError('Expected canonical lowercase UUID')
    return value


class SqlStore:
    def __init__(self,connection):self.connection=connection
    def rows(self,query,*values):
        cursor=self.connection.cursor()
        try:
            cursor.execute(query,*values)
            names=[c[0] for c in cursor.description]
            return [dict(zip(names,row)) for row in cursor.fetchall()]
        finally:cursor.close()
    def execute(self,query,*values):
        cursor=self.connection.cursor()
        try:cursor.execute(query,*values)
        finally:cursor.close()
    def get_run(self,child):
        rows=self.rows("SELECT config_id,parent_run_id,child_run_id,bronze_path,load_type,expected_rows,copied_rows,status,"
          "primary_key_columns,ISNULL(CONVERT(nvarchar(33),watermark_lower,126),'') AS watermark_lower,"
          "ISNULL(CONVERT(nvarchar(33),watermark_upper,126),'') AS watermark_upper,"
          "ISNULL(watermark_lower_key_json,'') AS lower_key_json,ISNULL(watermark_upper_key_json,'') AS upper_key_json,"
          "manifest_json,manifest_sha256 FROM ingestion.bronze_run_audit WHERE child_run_id=?",child)
        if len(rows)!=1:raise ValueError('Missing audit receipt')
        return rows[0]
    def parent_runs(self,parent):
        return self.rows('SELECT config_id,child_run_id,status FROM ingestion.bronze_run_audit WHERE parent_run_id=? ORDER BY config_id',parent)
    def active_configs(self):
        return self.rows('SELECT config_id,load_type FROM ingestion.table_config WHERE is_active=1 ORDER BY config_id')
    def parent_receipt(self,parent):
        rows=self.rows('SELECT parent_run_id,committed_children,manifest_sha256 FROM ingestion.bronze_parent_audit WHERE parent_run_id=?',parent)
        return rows[0] if rows else None
    def attest(self,plan,evidence,text,digest):
        self.execute('EXEC ingestion.attest_bronze_reconciliation @child_run_id=?,@bronze_path=?,@reconciled_rows=?,@manifest_json=?,@manifest_sha256=?',
                     plan['child_run_id'],plan['bronze_path'],evidence['reconciled_rows'],text,digest)
    def commit_parent(self,parent):self.execute('EXEC ingestion.commit_bronze_parent @parent_run_id=?',parent)


def attest_run(store,client,account,filesystem,child):
    run_id(child)
    plan=store.get_run(child)
    if plan['status'] not in ('COPIED','RECONCILED','COMMITTED') or plan['expected_rows']!=plan['copied_rows']:raise ValueError('Invalid Copy audit state')
    if plan['status']=='COMMITTED':
        if not plan['manifest_sha256']:raise ValueError('Legacy commit has no independent evidence')
        return plan
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


def connections(account,filesystem):
    import pyodbc
    from azure.identity import DefaultAzureCredential
    from azure.storage.filedatalake import DataLakeServiceClient
    if not re.fullmatch(r'[a-z0-9]{3,24}',account) or filesystem!='bronze':raise ValueError('Invalid storage binding')
    connection=os.environ.get('BRONZE_SQL_CONNECTION_STRING')
    if not connection:raise ValueError('Secure SQL connection setting is missing')
    sql=pyodbc.connect(connection,autocommit=True,timeout=30)
    service=DataLakeServiceClient('https://'+account+'.dfs.core.windows.net',DefaultAzureCredential())
    return sql,service


def run(account,filesystem,child):
    sql,service=connections(account,filesystem)
    try:
        store=SqlStore(sql);plan=attest_run(store,service.get_file_system_client(filesystem),account,filesystem,child)
        receipt=store.get_run(child)
        return {'run_id':child,'status':receipt['status'],'manifest_sha256':receipt['manifest_sha256']}
    finally:sql.close();service.close()


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--account',required=True);parser.add_argument('--filesystem',default='bronze')
    parser.add_argument('--run-id',required=True)
    args=parser.parse_args()
    try:print(json.dumps(run(args.account,args.filesystem,args.run_id),sort_keys=True));return 0
    except Exception as exc:
        print(json.dumps({'status':'FAILED','error_type':type(exc).__name__}),file=sys.stderr);return 1

if __name__=='__main__':sys.exit(main())
