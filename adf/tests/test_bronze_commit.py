"""Real Parquet validation and mocked SQL/ADF boundary tests, no Azure writes."""
import copy
import io
import json
import sys
from datetime import datetime,timezone,timedelta
from pathlib import Path
from types import SimpleNamespace as NS
from uuid import UUID
import pytest
import pyarrow as pa
import pyarrow.parquet as pq
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'tools'))
import reconcile_bronze as recon
import commit_reconciled_bronze as worker
import commit_bronze_parent as parent
from bronze_diagnostics import BronzeFailure

CHILD='00000000-0000-0000-0000-000000000001'
PARENT='10000000-0000-0000-0000-000000000001'


def plan(composite=False,full=False):
    keys=['SalesOrderID','SalesOrderDetailID'] if composite else ['ProductID']
    return dict(child_run_id=CHILD,parent_run_id=PARENT,load_type='FULL' if full else 'INCREMENTAL',
      primary_key_columns=json.dumps(keys),expected_rows=2,copied_rows=2,
      bronze_path='adventureworks/production/product/load_type='+('full' if full else 'incremental')+'/ingestion_date=2026-10-17/run_id='+CHILD,
      watermark_lower='' if full else '1900-01-01T00:00:00',watermark_upper='' if full else '2026-10-17T12:00:00.1234567',
      lower_key_json='' if full else json.dumps(dict.fromkeys(keys,0)),
      upper_key_json='' if full else json.dumps(dict.fromkeys(keys,2)),status='COPIED')


def parquet(p,change=None):
    keys=json.loads(p['primary_key_columns']);full=p['load_type']=='FULL'
    rows=[]
    for value in [1,2]:
        row={k:value for k in keys}
        row.update(_pipeline_run_id=p['child_run_id'],_parent_pipeline_run_id=p['parent_run_id'],
          _source_system='adventureworks',_load_type=p['load_type'],_watermark_lower=p['watermark_lower'],
          _watermark_upper=p['watermark_upper'],_watermark_lower_key_json=p['lower_key_json'],
          _watermark_upper_key_json=p['upper_key_json'],_ingested_at_utc='2026-10-17T12:01:00Z')
        if not full:row['ModifiedDate']=p['watermark_upper']
        rows.append(row)
    if change:change(rows)
    sink=io.BytesIO();pq.write_table(pa.Table.from_pylist(rows),sink)
    return [('part-00000.parquet',sink.getvalue())]


@pytest.mark.parametrize('composite,full',[(False,False),(True,False),(False,True)])
def test_reconcile_complete_contract(composite,full):
    p=plan(composite,full);e=recon.reconcile(parquet(p),2,CHILD,p)
    assert e['reconciled_rows']==2 and len(e['files'][0]['sha256'])==64


@pytest.mark.parametrize('field,bad',[
 ('_pipeline_run_id','wrong'),('_parent_pipeline_run_id','wrong'),('_source_system','wrong'),
 ('_load_type','FULL'),('_watermark_upper','2027-01-01T00:00:00'),
 ('_watermark_lower_key_json','{}'),('_watermark_upper_key_json','{"ProductID":3}'),
 ('_ingested_at_utc',None),('ProductID',None),('ModifiedDate',None),
 ('ModifiedDate','2026-10-17T12:00:00.1234568')])
def test_metadata_or_outside_window_rejected(field,bad):
    p=plan()
    with pytest.raises((ValueError,TypeError)):
        recon.reconcile(parquet(p,lambda rows:rows[0].update({field:bad})),2,CHILD,p)


def test_duplicate_keys_and_missing_upper_rejected():
    p=plan()
    with pytest.raises(ValueError):recon.reconcile(parquet(p,lambda rows:rows[0].update(ProductID=2)),2,CHILD,p)
    with pytest.raises(ValueError):recon.reconcile(parquet(p,lambda rows:rows[1].update(ModifiedDate='2026-01-01T00:00:00')),2,CHILD,p)


def test_empty_window_and_missing_nonempty_folder():
    p=plan();p.update(expected_rows=0,watermark_upper=p['watermark_lower'],upper_key_json=p['lower_key_json'])
    assert recon.reconcile([],0,CHILD,p)['reconciled_rows']==0
    with pytest.raises(ValueError):recon.reconcile([],1,CHILD,p)
    p['watermark_upper']='2026-10-17T00:00:00'
    with pytest.raises(ValueError):recon.reconcile([],0,CHILD,p)


def test_json_duplicate_keys_and_precision():
    with pytest.raises(ValueError):recon.cursor_key('{"id":1,"id":2}',['id'])
    assert recon.ticks('2026-10-17T12:00:00.1234568')-recon.ticks('2026-10-17T12:00:00.1234567')==1


class MemoryFS:
    def __init__(self,p):self.p=p;self.files=parquet(p);self.etag='etag-1';self.on_read=lambda:None
    def get_paths(self,path,recursive):
        return [NS(name=path+'/'+n,is_directory=False,etag=self.etag,content_length=len(data)) for n,data in self.files]
    def get_file_client(self,path):
        def download_file(**kwargs):
            assert kwargs['etag']==self.etag
            data=next(b for n,b in self.files if path.endswith('/'+n));self.on_read()
            return NS(readall=lambda:data)
        return NS(download_file=download_file)


def test_etag_mutation_and_digest_stability():
    p=plan();fs=MemoryFS(p)
    e,text,digest=recon.read_and_reconcile(fs,p,'storage123','bronze')
    assert digest==recon.manifest_digest(text)
    assert e['files'][0]['etag']=='etag-1'
    fs.on_read=lambda:setattr(fs,'etag','changed')
    with pytest.raises(ValueError):recon.read_and_reconcile(fs,p,'storage123','bronze')


class FakeStore:
    def __init__(self):
        self.configs=[dict(config_id=i,load_type='INCREMENTAL' if i<=5 else 'FULL') for i in range(1,12)]
        self.runs=[dict(config_id=i,child_run_id=str(UUID(int=i)),status='COPIED') for i in range(1,12)]
        self.receipt=None;self.commits=0
    def active_configs(self):return self.configs
    def parent_runs(self,_):return self.runs
    def parent_receipt(self,_):return self.receipt
    def commit_parent(self,p):self.commits+=1;self.receipt=dict(parent_run_id=p,committed_children=11,manifest_sha256='a'*64)


def test_parent_all_or_none_and_idempotent_retry(monkeypatch):
    store=FakeStore();calls=[]
    monkeypatch.setattr(parent,'attest_run',lambda s,c,a,f,child:calls.append(child) or {'child':child})
    monkeypatch.setattr(parent,'verify_unchanged',lambda c,p:None)
    verify=lambda p,ids:None
    result=parent.commit_parent(store,None,'storage123','bronze',PARENT,verify)
    assert result['status']=='COMMITTED' and len(calls)==11 and store.commits==1
    assert parent.commit_parent(store,None,'storage123','bronze',PARENT,lambda *a:pytest.fail('historical replay'))['replayed']
    assert store.commits==1


def test_one_bad_child_cannot_commit_parent(monkeypatch):
    store=FakeStore()
    def attest(*args):
        if args[-1]==store.runs[-1]['child_run_id']:raise ValueError('bad Parquet')
        return {}
    monkeypatch.setattr(parent,'attest_run',attest)
    with pytest.raises(BronzeFailure):parent.commit_parent(store,None,'storage123','bronze',PARENT,lambda *a:None)
    assert store.commits==0


@pytest.mark.parametrize('failure',['missing','duplicate','mode'])
def test_incomplete_parent_blocks_before_adls(monkeypatch,failure):
    store=FakeStore()
    if failure=='missing':store.runs.pop()
    if failure=='duplicate':store.runs[-1]['config_id']=1
    if failure=='mode':store.configs[-1]['load_type']='INCREMENTAL'
    monkeypatch.setattr(parent,'attest_run',lambda *a:pytest.fail('must not read ADLS'))
    with pytest.raises(BronzeFailure):parent.commit_parent(store,None,'storage123','bronze',PARENT,lambda *a:None)


def test_adf_failure_or_child_mismatch_rejected():
    now=datetime.now(timezone.utc)
    activities=[NS(activity_name=n,status='Succeeded',activity_type='IfCondition',output={},error={}) for n in ['AcknowledgeLandingOnly','ReadActiveConfigurations','ValidateActiveConfigurations','ForEachSource']]
    activities += [NS(activity_name='WatermarkConfirmationBlocked',status='Failed',activity_type='Fail',error={'errorCode':'WATERMARK_CONFIRMATION_BLOCKED'},output={})]
    children=[str(UUID(int=i)) for i in range(1,12)]
    activities += [NS(activity_name='LandSource',status='Succeeded',activity_type='ExecutePipeline',output={'pipelineRunId':c},error={}) for c in children]
    adf=NS(pipeline_runs=NS(get=lambda *a:NS(pipeline_name='pl_adventureworks_bronze',status='Failed',run_start=now,run_end=now+timedelta(minutes=1))),
       activity_runs=NS(query_by_pipeline_run=lambda *a:NS(value=activities,continuation_token=None)))
    parent.verify_adf_parent(adf,'rg','factory',PARENT,children)
    activities[-1].output['pipelineRunId']='wrong'
    with pytest.raises(ValueError):parent.verify_adf_parent(adf,'rg','factory',PARENT,children)
    activities[4].error={'errorCode':'SOMETHING_ELSE'}
    with pytest.raises(ValueError):parent.verify_adf_parent(adf,'rg','factory',PARENT,children)


def test_arrow_timestamp_ns_preserves_seven_digit_cursor():
    p=plan();files=parquet(p)
    table=pq.read_table(io.BytesIO(files[0][1]))
    column=pa.array([recon.ticks(p['watermark_upper'])*100]*2,type=pa.timestamp('ns'))
    table=table.set_column(table.schema.get_field_index('ModifiedDate'),'ModifiedDate',column)
    sink=io.BytesIO();pq.write_table(table,sink)
    assert recon.reconcile([('part-00000.parquet',sink.getvalue())],2,CHILD,p)['reconciled_rows']==2
