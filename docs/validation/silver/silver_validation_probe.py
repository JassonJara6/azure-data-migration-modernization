"""Temporary observer: actual local Delta history, no project source changes."""
import json
import sys
from pathlib import Path
from importlib.metadata import version
from unittest.mock import patch
import pytest

OUT=Path(__file__).resolve().parent
TARGET='test_complete_parent_and_replay_without_delta_writes'

@pytest.hookimpl(hookwrapper=True)
def pytest_runtest_call(item):
    if item.name!=TARGET:
        yield
        return
    module=item.module
    original=module.run
    calls=[]
    def observed_run(spark,bundle,**kwargs):
        assert sys.version_info[:2]==(3,11)
        java=str(spark._jvm.java.lang.System.getProperty('java.version'))
        assert java.startswith('17.')
        names=[t['target'] for t in module.TABLES]+['_silver_batches','_silver_progress']
        store=module.DeltaStore(spark,kwargs['namespace'])
        third=len(calls)==2
        calls.append(bundle['parent_run_id'])
        before={n:store.version(n) for n in names} if third else None
        result=original(spark,bundle,**kwargs)
        if third:
            after={n:store.version(n) for n in names}
            assert result['replayed'] is True
            assert result['status']=='SUCCEEDED'
            assert len(result['metrics'])==11
            assert before==after
            assert all(m['rejected_rows']==0 for m in result['metrics'].values())
            report={'status':'LOCAL_REPLAY_VERIFIED','fixtures':'SYNTHETIC_LOCAL_PARQUET',
                'python':sys.version.split()[0],'java':java,'spark':spark.version,
                'delta_spark':version('delta-spark'),'pyarrow':version('pyarrow'),
                'full_sources':[t['source'] for t in module.TABLES if t['mode']=='FULL'],
                'incremental_sources':[t['source'] for t in module.TABLES if t['mode']=='INCREMENTAL'],
                'metrics':result['metrics'],'versions_before':before,'versions_after':after,
                'tables_checked':len(names),'delta_versions_unchanged':True,
                'recovery_after_interruption':'EXISTING_TEST_ASSERTIONS',
                'azure_execution':'NOT_PERFORMED'}
            (OUT/'replay-evidence.json').write_text(json.dumps(report,indent=2),encoding='utf-8')
        return result
    with patch.object(module,'run',observed_run):
        outcome=yield
    if outcome.excinfo is None:
        assert len(calls)==4
        assert len(set(calls[:3]))==1 and calls[3]!=calls[0]
