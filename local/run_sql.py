"""One command: existing local SQL -> Parquet -> unchanged Silver/Gold -> replay.

No Docker control, Azure clients, cloud Jobs, SQL writes or remote receipts.
"""
import argparse
import getpass
import json
import os
from pathlib import Path
import sys
import warnings

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))
from local.run import execute,single_writer
from local.lake import session
from local.sql_export import Settings,export
from local.sql_manifest import ExportError,require,load,silver_input,ACCOUNT


def password_from_environment(prompt=False):
    value=os.environ.get('ADMM_SQL_PASSWORD')
    if value:return value
    require(prompt,'LOCAL_SQL_PASSWORD_ENV_REQUIRED')
    # Never fall back to echoing password input in Git Bash/non-TTY consoles.
    try:
        with warnings.catch_warnings():
            warnings.simplefilter('error',getpass.GetPassWarning)
            value=getpass.getpass('SQL password (hidden): ')
    except (getpass.GetPassWarning,EOFError):raise ExportError('LOCAL_SQL_HIDDEN_PROMPT_UNAVAILABLE') from None
    require(bool(value),'LOCAL_SQL_PASSWORD_ENV_REQUIRED')
    os.environ['ADMM_SQL_PASSWORD']=value
    return value


def validate_expected_counts(manifest,expected):
    counts={t['source']:t['source_rows'] for t in manifest['tables']}
    for source,n in expected.items():require(counts.get(source)==n,'LOCAL_SQL_EXPECTED_COUNT_MISMATCH')
    return counts


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root',default='work/adventureworks-real')
    parser.add_argument('--prompt-password',action='store_true',help='Read hidden password into environment only when a NEW export needs it')
    parser.add_argument('--chunk-rows',type=int,default=5000)
    parser.add_argument('--expected-product-rows',type=int,default=504)
    parser.add_argument('--expected-detail-rows',type=int,default=121317)
    args=parser.parse_args()
    root=Path(args.root).resolve();root.mkdir(parents=True,exist_ok=True);spark=None
    expected={'Production.Product':args.expected_product_rows,'Sales.SalesOrderDetail':args.expected_detail_rows}
    try:
        require(sys.version_info[:2]==(3,11),'LOCAL_SQL_PYTHON_311_REQUIRED')
        require(all(n>=0 for n in expected.values()),'LOCAL_SQL_EXPECTED_COUNT_INVALID')
        with single_writer(root):
            path=root/'local-sql-export.json';provenance=root/'provenance.json'
            if provenance.exists():
                require(json.loads(provenance.read_text())['data_kind']=='LOCAL_SQL_SERVER_EXPORT','LOCAL_SQL_LAKE_PROVENANCE_CHANGED')
            reused=path.exists()
            if reused:manifest=load(path)
            else:
                require(not any((root/n).exists() for n in ('local_silver','local_gold')) and not provenance.exists(),
                        'LOCAL_SQL_FRESH_LAKE_REQUIRED')
                settings=Settings(user=os.environ.get('ADMM_SQL_USER','admm_ingestion'),
                    password=password_from_environment(args.prompt_password),server=os.environ.get('ADMM_SQL_SERVER','127.0.0.1,1433'))
                manifest=export(root,settings,chunk_rows=args.chunk_rows,expected_counts=expected)
                del settings
            bundle,bronze=silver_input(manifest,root)
            counts=validate_expected_counts(manifest,expected)
            binding={'data_kind':'LOCAL_SQL_SERVER_EXPORT','bronze_root':str(bronze),'account':ACCOUNT,
                     'export_manifest_sha256':manifest['manifest_sha256']}
            if provenance.exists():require(json.loads(provenance.read_text())==binding,'LOCAL_SQL_LAKE_PROVENANCE_CHANGED')
            else:provenance.write_text(json.dumps(binding,indent=2),encoding='utf-8')
            # SQL is closed; never propagate its password to JVM/Python workers.
            os.environ.pop('ADMM_SQL_PASSWORD',None)
            spark=session(root)
            require(spark.sparkContext._jvm.java.lang.System.getProperty('java.version').startswith('17.'),'LOCAL_SQL_JAVA_17_REQUIRED')
            result=execute(spark,bundle,bronze,ACCOUNT,root,data_kind='LOCAL_SQL_SERVER_EXPORT')
            require(len(result['silver']['metrics'])==11,'LOCAL_SQL_SILVER_SOURCE_COUNT_INVALID')
            for name,n in counts.items():
                metric=result['silver']['metrics'][name]
                require(metric['input_rows']==n and metric['target_rows']==n and metric['rejected_rows']==0,
                        'LOCAL_SQL_SILVER_RECONCILIATION_FAILED')
            from admm_gold.model import KEYS
            require(all(name in result['gold']['metrics'] for name in KEYS),'LOCAL_SQL_GOLD_TABLE_COUNT_INVALID')
            result.update(validation_scope='LOCAL_SQL_SNAPSHOT',export_reused=reused,sql_queried_this_invocation=not reused,
                local_export_id=manifest['export_id'],local_export_manifest_sha256=manifest['manifest_sha256'],
                sql_source_counts=counts,remote_commit_confirmed=False,expected_counts=expected)
            from importlib.metadata import version
            result['environment']={'python':sys.version.split()[0],'java':spark.sparkContext._jvm.java.lang.System.getProperty('java.version'),
                'spark':spark.version,'delta_spark':version('delta-spark'),'pyarrow':version('pyarrow')}
            report=root/('sql-acceptance-'+manifest['export_id']+'.json')
            report.write_text(json.dumps(result,indent=2,sort_keys=True),encoding='utf-8')
            (root/'validation-status.json').write_text(json.dumps({'status':'SUCCEEDED','report':report.name,'azure_validated':False}))
            print(json.dumps({'status':'SUCCEEDED','data_kind':'LOCAL_SQL_SERVER_EXPORT','report':str(report),
                'silver_tables':11,'gold_tables':7,'replay_verified':True,'sql_export_reused':reused,'azure_validated':False}))
        return 0
    except Exception as exc:
        from admm_silver.contracts import SilverError
        from admm_gold.model import GoldError
        code=str(exc) if isinstance(exc,(ExportError,SilverError,GoldError)) else 'LOCAL_SQL_RUNTIME_FAILED'
        if isinstance(exc,FileExistsError):code='LOCAL_SQL_WRITER_LOCKED'
        report={'status':'FAILED','code':code,'error_type':type(exc).__name__,'azure_validated':False}
        if isinstance(exc,ExportError):
            if exc.sqlstate:report['sqlstate']=exc.sqlstate
            if exc.stage in {'CONNECT','IDENTITY','SCHEMA','COUNT','EXPECTED_COUNTS','UPPER_CURSOR','READ_ROWS','WRITE_PARQUET','ROLLBACK'}:
                report['stage']=exc.stage
            from admm_silver.contracts import TABLES
            if exc.source_table in {t['source'] for t in TABLES}:report['source_table']=exc.source_table
        # A process which failed to acquire the lock must not overwrite the
        # active writer's report. Other failures get safe local status only.
        if not isinstance(exc,FileExistsError):
            (root/'validation-status.json').write_text(json.dumps(report),encoding='utf-8')
        print(json.dumps(report),file=sys.stderr);return 1
    finally:
        if spark is not None:spark.stop()

if __name__=='__main__':raise SystemExit(main())
