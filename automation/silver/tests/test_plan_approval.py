"""Approval/application control-flow tests. Every apply below is a stub, never Terraform."""
import copy
import json
import os
import sys
from pathlib import Path
import pytest
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
import launch
from plan_guard import Blocked,comparison_sha,sha


def payload():
    return {'timestamp':'2026-01-01T00:00:00Z','resource_changes':[{
        'address':'databricks_catalog.silver','mode':'managed',
        'change':{'actions':['create'],'after':{'name':'admm_dev'}}}],
        'planned_values':{'outputs':{'catalog':{'value':'admm_dev'}}},
        'variables':{'operator':{'value':'operator@example.invalid'}}}


def harness(tmp_path,*,deploy=True,mutation=None):
    obj=object.__new__(launch.Launcher);obj.work=tmp_path;obj.deploy=deploy
    calls=[];artifacts={};reads=0;plans=0
    def tf(root,*args,**kwargs):
        nonlocal reads,plans
        calls.append(args)
        if args[0]=='plan':
            plans+=1
            artifacts['binary']=Path(next(a[5:] for a in args if a.startswith('-out=')))
            artifacts['binary'].write_bytes(b'synthetic-binary-'+str(plans).encode())
            artifacts['text']=artifacts['binary'].parent/'plan.txt'
        elif args[0]=='show':
            assert Path(args[-1])==artifacts['binary']
            if '-json' in args:
                reads+=1
                if mutation:mutation('json-'+str(reads),artifacts)
                return copy.deepcopy(payload())
            if mutation:mutation('text',artifacts)
            return 'synthetic full readable plan'
        elif args[0]=='apply':
            assert len(args)==3 and args[1]=='-input=false'
            assert Path(args[2])==artifacts['binary']
            # This assertion executes in a mock, never invokes real Terraform.
            assert sha(artifacts['binary'])==artifacts['approved_sha']
        else:pytest.fail('Unexpected mock command')
    obj.tf=tf
    return obj,calls,artifacts


def approve(monkeypatch,artifacts,*,mutate=None):
    def approval(label,digest):
        assert label=='APPLY-UC-WORKSPACE'
        assert digest==sha(artifacts['binary']) and len(digest)==64
        artifacts['approved_sha']=digest
        if mutate:mutate(artifacts)
    monkeypatch.setattr(launch,'approval',approval)


def test_approval_and_mock_apply_use_identical_binary_without_replan(tmp_path,monkeypatch):
    obj,calls,artifacts=harness(tmp_path);approve(monkeypatch,artifacts)
    checked=[]
    assert obj.plan(launch.UC_WORKSPACE,'uc-workspace',{},before_apply=lambda:checked.append('revalidated'))
    assert checked==['revalidated']
    assert [c[0] for c in calls]==['plan','show','show','show','apply']
    review=json.loads((artifacts['binary'].parent/'review.json').read_text())
    assert review['sha256']==artifacts['approved_sha']
    assert review['approval_artifact']=='terraform_saved_binary_plan'
    assert review['comparison_json_sha256']==comparison_sha(payload())
    assert review['review_text_sha256']==sha(artifacts['text'])


@pytest.mark.parametrize('event',['json-1','text','approval','json-2','authorization'])
def test_any_binary_replacement_blocks_mock_apply_even_with_same_resource_actions(tmp_path,monkeypatch,event):
    def corrupt(artifacts):artifacts['binary'].write_bytes(b'replaced-binary-same-seven-resource-summary')
    obj,calls,artifacts=harness(tmp_path,mutation=lambda phase,a:corrupt(a) if phase==event else None)
    approve(monkeypatch,artifacts,mutate=corrupt if event=='approval' else None)
    hook=lambda:corrupt(artifacts) if event=='authorization' else None
    with pytest.raises(Blocked,match='PLAN_CHANGED_'):
        obj.plan(launch.UC_WORKSPACE,'uc-workspace',{},before_apply=hook)
    assert not any(c[0]=='apply' for c in calls)


def test_altered_human_review_blocks_apply(tmp_path,monkeypatch):
    obj,calls,artifacts=harness(tmp_path)
    approve(monkeypatch,artifacts,mutate=lambda a:a['text'].write_text('different reviewed content'))
    with pytest.raises(Blocked,match='REVIEW_TEXT_CHANGED'):obj.plan(launch.UC_WORKSPACE,'uc-workspace',{})
    assert not any(c[0]=='apply' for c in calls)


def test_full_json_is_rechecked_not_only_addresses_and_actions(tmp_path,monkeypatch):
    obj,calls,artifacts=harness(tmp_path);original=obj.tf;reads=0
    def tf(root,*args,**kwargs):
        nonlocal reads
        result=original(root,*args,**kwargs)
        if args[:2]==('show','-json'):
            reads+=1
            if reads==2:result['resource_changes'][0]['change']['after']['name']='different_catalog'
        return result
    obj.tf=tf;approve(monkeypatch,artifacts)
    with pytest.raises(Blocked,match='CONTENT_CHANGED'):obj.plan(launch.UC_WORKSPACE,'uc-workspace',{})
    assert not any(c[0]=='apply' for c in calls)


def test_authorization_revocation_after_approval_blocks_mock_apply(tmp_path,monkeypatch):
    obj,calls,artifacts=harness(tmp_path);approve(monkeypatch,artifacts)
    def revoked():raise Blocked('RUN_AS_REVOKED_SYNTHETIC')
    with pytest.raises(Blocked,match='RUN_AS_REVOKED'):obj.plan(launch.UC_WORKSPACE,'uc-workspace',{},before_apply=revoked)
    assert not any(c[0]=='apply' for c in calls)


@pytest.mark.parametrize('wrong_hash',['0'*64,comparison_sha(payload())])
def test_declined_approval_never_applies(tmp_path,monkeypatch,wrong_hash):
    obj,calls,_=harness(tmp_path)
    monkeypatch.setattr('builtins.input',lambda _: 'APPLY-UC-WORKSPACE '+wrong_hash)
    with pytest.raises(Blocked,match='APPROVAL_NOT_GIVEN'):obj.plan(launch.UC_WORKSPACE,'uc-workspace',{})
    assert not any(c[0]=='apply' for c in calls)


def test_separate_runs_never_overwrite_pending_plan_and_review(tmp_path,monkeypatch):
    obj,calls,artifacts=harness(tmp_path,deploy=False)
    monkeypatch.setattr('builtins.input',lambda _:pytest.fail('Plan-only must not prompt'))
    assert obj.plan(launch.UC_WORKSPACE,'uc-workspace',{}) is False
    first=artifacts['binary'];binary=first.read_bytes();text=artifacts['text'].read_bytes()
    assert obj.plan(launch.UC_WORKSPACE,'uc-workspace',{}) is False
    assert artifacts['binary']!=first and first.read_bytes()==binary
    assert (first.parent/'plan.txt').read_bytes()==text
    metadata=[json.loads(p.read_text()) for p in tmp_path.rglob('review.json')]
    assert len({m['plan_sha256'] for m in metadata})==2
    assert len({m['comparison_json_sha256'] for m in metadata})==1
    assert not any(c[0]=='apply' for c in calls)


def test_comparison_hash_ignores_only_top_level_timestamp_and_json_key_order():
    original=payload();second=dict(reversed(list(original.items())));second['timestamp']='2026-01-02T00:00:00Z'
    assert comparison_sha(original)==comparison_sha(second)
    for mutate in [lambda p:p['resource_changes'][0]['change']['after'].update(name='other'),
                   lambda p:p['variables']['operator'].update(value='other@example.invalid'),
                   lambda p:p['planned_values']['outputs']['catalog'].update(value='other'),
                   lambda p:p.update(configuration={'root_module':{}}),
                   lambda p:p['resource_changes'][0]['change']['after'].update(timestamp='business timestamp')]:
        changed=copy.deepcopy(original);mutate(changed)
        assert comparison_sha(original)!=comparison_sha(changed)


@pytest.mark.skipif(os.environ.get('SILVER_PROVIDER_AUTH_TESTS')!='1',reason='Real local Terraform plan reproduction opt-in required')
def test_real_terraform_same_seven_creates_different_binary_sha(tmp_path):
    import os,shutil,subprocess,time,zipfile
    terraform=os.environ.get('SILVER_TERRAFORM_BIN') or shutil.which('terraform')
    assert terraform
    # Built-in provider, local synthetic configuration, no external API or backend.
    (tmp_path/'main.tf').write_text('resource "terraform_data" "sample" {\n count = 7\n input = "unchanged"\n}\n')
    env={k:v for k,v in os.environ.items() if not k.startswith(('TF_VAR_','TF_LOG'))}
    def run(*args):
        result=subprocess.run([str(terraform),'-chdir='+str(tmp_path),*args],env=env,text=True,capture_output=True,timeout=45)
        assert result.returncode==0,result.stderr
        return result.stdout
    run('init','-backend=false','-input=false')
    plans=[];files=[]
    for index in range(2):
        if index:time.sleep(1.1) # Timestamp is stored with whole-second precision.
        saved=tmp_path/('saved-'+str(index)+'.tfplan');files.append(saved)
        run('plan','-input=false','-out='+str(saved))
        plans.append(json.loads(run('show','-json',str(saved))))
    assert all([r['change']['actions'] for r in p['resource_changes']]==[['create']]*7 for p in plans)
    assert sha(files[0])!=sha(files[1])
    assert [k for k in plans[0].keys()|plans[1].keys() if plans[0].get(k)!=plans[1].get(k)]==['timestamp']
    assert comparison_sha(plans[0])==comparison_sha(plans[1])
    with zipfile.ZipFile(files[0]) as first,zipfile.ZipFile(files[1]) as second:
        assert first.read('tfplan')!=second.read('tfplan')
    # This test executes init/plan/show only. It must never call real apply.


def test_noop_plan_never_requests_approval_or_mock_apply(tmp_path,monkeypatch):
    obj,calls,artifacts=harness(tmp_path);original=obj.tf
    def tf(root,*args,**kwargs):
        value=original(root,*args,**kwargs)
        if args[:2]==('show','-json'):value['resource_changes']=[]
        return value
    obj.tf=tf
    monkeypatch.setattr(launch,'approval',lambda *a:pytest.fail('No-op must not request approval'))
    assert obj.plan(launch.UC_WORKSPACE,'uc-workspace',{}) is True
    assert not any(c[0]=='apply' for c in calls)
