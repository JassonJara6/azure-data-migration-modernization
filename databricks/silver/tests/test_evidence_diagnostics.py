"""Local receipt IO and notebook failure handling; no Databricks calls."""
import builtins
import json
from pathlib import Path
import pytest
from admm_silver.evidence import load_committed_evidence
from admm_silver.diagnostics import failure_report
from admm_silver.contracts import SilverError

PARENT='20d65511-8190-41a0-8ad2-23d7568363d2'

@pytest.mark.parametrize('text,code',[('null','RECEIPT_OBJECT_REQUIRED'),('[]','RECEIPT_OBJECT_REQUIRED'),('{}','RECEIPT_PARENT_REQUIRED'),('{"parent_run_id":null}','RECEIPT_PARENT_REQUIRED'),('{"parent_run_id":"other"}','REQUESTED_PARENT_MISMATCH')])
def test_demonstrable_json_shape_errors_are_contract_errors(tmp_path,text,code):
    path=tmp_path/'receipt.json';path.write_text(text)
    with pytest.raises(SilverError,match=code):load_committed_evidence(path,PARENT)


def test_valid_receipt_preserved(tmp_path):
    bundle={'parent_run_id':PARENT,'extra':'not discarded'}
    path=tmp_path/'receipt.json';path.write_text(json.dumps(bundle))
    assert load_committed_evidence(path,PARENT)==bundle

@pytest.mark.parametrize('kind',['missing','permission','read','encoding','json','duplicate'])
def test_io_and_parse_diagnostics_hide_sensitive_details(tmp_path,monkeypatch,kind):
    path=tmp_path/'synthetic-private-receipt.json'
    path.write_text('{"parent_run_id":"synthetic-secret",')
    expected={'missing':'FileNotFoundError','permission':'PermissionError','read':'OSError','encoding':'UnicodeDecodeError','json':'JSONDecodeError','duplicate':'SilverError'}[kind]
    if kind=='missing':path.unlink()
    if kind=='encoding':path.write_bytes(b'\xff')
    if kind=='duplicate':path.write_text('{"parent_run_id":1,"parent_run_id":2}')
    original=builtins.open
    class BrokenRead:
        def __enter__(self):return self
        def __exit__(self,*args):return False
        def read(self):raise OSError('synthetic-secret read failure')
    def open_mock(*args,**kwargs):
        if args[0]==path:
            if kind=='permission':raise PermissionError('synthetic-secret access denied')
            if kind=='read':return BrokenRead()
        return original(*args,**kwargs)
    monkeypatch.setattr(builtins,'open',open_mock)
    with pytest.raises(Exception) as caught:load_committed_evidence(path,PARENT)
    report=failure_report(caught.value,'committed_evidence')
    assert report['error_type']==expected and report['stage']=='committed_evidence'
    assert report['location']['file'] in ('admm_silver/evidence.py','admm_silver/contracts.py')
    assert report['location']['line']>0
    serialized=json.dumps(report)
    assert 'synthetic-' not in serialized and str(tmp_path) not in serialized
    assert 'Traceback' not in serialized and 'message' not in report


def test_exact_io_line_and_nonstatic_error_code_redacted(tmp_path):
    import inspect
    from admm_silver import evidence
    source,start=inspect.getsourcelines(evidence.load_committed_evidence)
    expected=start+next(i for i,line in enumerate(source) if 'with open(' in line)
    with pytest.raises(FileNotFoundError) as caught:load_committed_evidence(tmp_path/'missing.json',PARENT)
    assert failure_report(caught.value,'committed_evidence')['location']=={'file':'admm_silver/evidence.py','line':expected}
    assert failure_report(SilverError('synthetic secret message'),'committed_evidence')['error_code']=='RUNTIME_FAILED'


def test_notebook_reports_evidence_failure_before_pipeline(monkeypatch,capsys):
    from types import SimpleNamespace as NS
    from admm_silver import storage,pipeline
    values=dict(catalog='admm_dev',schema='silver',bronze_volume='/Volumes/admm_dev/inputs/bronze',
        receipt_file='/Volumes/admm_dev/inputs/receipts/synthetic-private.json',account='storage123',
        parent_run_id=PARENT,execution_id='synthetic-run',replay_check='true')
    widgets=NS(text=lambda *a:None,get=lambda key:values[key])
    dbutils=NS(widgets=widgets,notebook=NS(exit=lambda *a:pytest.fail('Must not exit successfully')))
    spark=NS(conf=NS(set=lambda *a:None),sql=lambda *a:NS(collect=lambda:[]))
    monkeypatch.setattr(storage,'verify_volume',lambda *a:None)
    monkeypatch.setattr(pipeline,'run',lambda *a,**k:pytest.fail('No Silver apply after evidence failure'))
    import admm_silver.evidence as evidence
    def denied(*a,**k):raise PermissionError('synthetic-secret credentials detail')
    monkeypatch.setattr(evidence,'open',denied,raising=False)
    notebook=Path(__file__).resolve().parents[1]/'notebooks/run_silver.py'
    with pytest.raises(RuntimeError,match='^SILVER_RUNTIME_FAILED ') as raised:
        exec(compile(notebook.read_text(),str(notebook),'exec'),{'dbutils':dbutils,'spark':spark})
    output=capsys.readouterr().out
    report=json.loads(output)
    assert json.loads(str(raised.value).split(' ',1)[1])==report
    assert 'synthetic-' not in str(raised.value)
    assert report['stage']=='committed_evidence' and report['error_type']=='PermissionError'
    assert report['location']['file']=='admm_silver/evidence.py'
    assert 'synthetic-' not in output and 'credentials' not in output


def test_volume_receipt_driver_read_is_complete_utf8(monkeypatch):
    # No DBUtils/head fallback: receipt may exceed the usual head preview size.
    import io
    import admm_silver.evidence as evidence
    path='/Volumes/admm_dev/inputs/receipts/'+PARENT+'.json'
    payload={'parent_run_id':PARENT,'padding':'á'*70000}
    calls=[]
    def volume_open(actual,**kwargs):
        calls.append((actual,kwargs))
        return io.StringIO(json.dumps(payload,ensure_ascii=False))
    monkeypatch.setattr(evidence,'open',volume_open,raising=False)
    assert load_committed_evidence(path,PARENT)==payload
    assert calls==[(path,{'encoding':'utf-8'})]
