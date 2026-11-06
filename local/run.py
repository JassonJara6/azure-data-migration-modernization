"""One local Bronze -> existing Silver -> Gold -> no-write replay, no cloud clients."""
import argparse
from contextlib import contextmanager
import json
import os
from pathlib import Path
import sys

ROOT=Path(__file__).resolve().parents[1]
for path in (ROOT,ROOT/'databricks/silver/src',ROOT/'databricks/gold/src'):sys.path.insert(0,str(path))
from local.lake import session,register,history
from admm_silver.evidence import load_committed_evidence
from admm_silver.pipeline import run as silver_run
from admm_gold.pipeline import run as gold_run


@contextmanager
def single_writer(root):
    path=root/'.writer.lock'
    descriptor=os.open(path,os.O_CREAT|os.O_EXCL|os.O_WRONLY,0o600)
    try:
        os.write(descriptor,str(os.getpid()).encode());os.close(descriptor)
        yield
    finally:path.unlink(missing_ok=True)


def execute(spark,bundle,bronze_root,account,root,*,data_kind):
    for namespace in ('local_silver','local_gold'):register(spark,root,namespace)
    parent=bundle['parent_run_id']
    silver=silver_run(spark,bundle,account=account,bronze_root=str(bronze_root),namespace='local_silver',execution_id='offline')
    gold=gold_run(spark,silver_namespace='local_silver',gold_namespace='local_gold',parent_run_id=parent)
    before={n:history(spark,n) for n in ('local_silver','local_gold')}
    repeated_silver=silver_run(spark,bundle,account=account,bronze_root=str(bronze_root),namespace='local_silver',execution_id='offline-replay')
    repeated_gold=gold_run(spark,silver_namespace='local_silver',gold_namespace='local_gold',parent_run_id=parent)
    after={n:history(spark,n) for n in ('local_silver','local_gold')}
    if not (repeated_silver['replayed'] and repeated_gold['replayed'] and before==after):
        raise RuntimeError('LOCAL_REPLAY_CREATED_DELTA_VERSIONS')
    return {'status':'SUCCEEDED','data_kind':data_kind,'parent_run_id':parent,'silver':silver,'gold':gold,
        'replay':{'verified':True,'versions_before':before,'versions_after':after},
        'azure_validated':False,'source_provenance':('SYNTHETIC_GENERATOR' if data_kind=='SYNTHETIC_FIXTURES' else 'OPERATOR_PROVIDED_LOCAL_FILES')}


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root',default='work/local-lake')
    parser.add_argument('--fixture',action='store_true',help='Generate explicitly SYNTHETIC Bronze-shaped Parquet and evidence')
    parser.add_argument('--fixture-phase',type=int,choices=(1,2),default=1)
    parser.add_argument('--bronze-root',help='Existing LOCAL mirror of Bronze folders; never downloaded by this tool')
    parser.add_argument('--receipt',help='Existing authoritative local COMMITTED JSON; never fabricated in real-data mode')
    parser.add_argument('--account',help='Storage account binding in the existing receipt (no Azure connection)')
    args=parser.parse_args();root=Path(args.root).resolve();root.mkdir(parents=True,exist_ok=True)
    if args.fixture and any((args.receipt,args.bronze_root,args.account)):parser.error('Fixture and real input options are mutually exclusive')
    if not args.fixture and not all((args.receipt,args.bronze_root,args.account)):parser.error('Use --fixture or supply --receipt, --bronze-root and --account')
    if sys.version_info[:2]!=(3,11):parser.error('Use Python 3.11 for the pinned local runtime')
    spark=None
    try:
        with single_writer(root):
            # Bind a lake to a data provenance and source root. Never reuse a
            # synthetic success record to certify an unrelated real-data input.
            bronze=(root/'bronze') if args.fixture else Path(args.bronze_root).resolve()
            if args.fixture:
                from local.fixture_data import generate
                bundle=generate(bronze,args.fixture_phase);account='storage123';kind='SYNTHETIC_FIXTURES'
            else:
                preliminary=json.loads(Path(args.receipt).read_text(encoding='utf-8'))
                bundle=load_committed_evidence(args.receipt,preliminary['parent_run_id'])
                account=args.account;kind='LOCAL_COMMITTED_DATA'
            binding={'data_kind':kind,'bronze_root':str(bronze),'account':account}
            file=root/'provenance.json'
            if file.exists() and json.loads(file.read_text())!=binding:raise RuntimeError('LOCAL_LAKE_PROVENANCE_CHANGED')
            if not file.exists():file.write_text(json.dumps(binding,indent=2))
            spark=session(root)
            if not spark.sparkContext._jvm.java.lang.System.getProperty('java.version').startswith('17.'):
                raise RuntimeError('JAVA_17_REQUIRED')
            result=execute(spark,bundle,bronze,account,root,data_kind=kind)
            result['environment']={'python':sys.version.split()[0],'spark':spark.version}
            from importlib.metadata import version
            result['environment']['delta_spark']=version('delta-spark')
            report=root/('acceptance-'+bundle['parent_run_id']+'.json')
            report.write_text(json.dumps(result,indent=2,sort_keys=True),encoding='utf-8')
            print(json.dumps({'status':result['status'],'data_kind':kind,'report':str(report),'replay_verified':True}))
        return 0
    except Exception as exc:
        # Never echo Spark messages, input rows, secrets or complete tracebacks.
        from admm_silver.contracts import SilverError
        from admm_gold.model import GoldError
        safe={'JAVA_17_REQUIRED','LOCAL_LAKE_PROVENANCE_CHANGED','LOCAL_REPLAY_CREATED_DELTA_VERSIONS','SYNTHETIC_FIXTURE_CHANGED'}
        code=str(exc) if isinstance(exc,(SilverError,GoldError)) or str(exc) in safe else 'LOCAL_RUNTIME_FAILED'
        if isinstance(exc,FileExistsError):code='LOCAL_WRITER_LOCKED'
        print(json.dumps({'status':'FAILED','code':code,'error_type':type(exc).__name__}),file=sys.stderr)
        return 1
    finally:
        if spark is not None:spark.stop()

if __name__=='__main__':raise SystemExit(main())
