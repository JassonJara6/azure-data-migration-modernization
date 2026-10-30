"""Pinned real CLI, strict loopback wire contract. Not live Databricks evidence."""
import json
import os
import subprocess
import threading
from http.server import BaseHTTPRequestHandler,ThreadingHTTPServer
import pytest
from test_v4_primary import settings
from v4_transition import target_clusters,JOB_ID

CLI=os.environ.get('SILVER_DATABRICKS_BIN')
pytestmark=pytest.mark.skipif(not CLI,reason='Set SILVER_DATABRICKS_BIN to the pinned CLI')


def test_real_cli_bundle_schema_supports_explicit_empty_flexibility():
    p=subprocess.run([CLI,'bundle','schema'],capture_output=True,text=True,encoding='utf-8',check=True,timeout=30)
    schema=json.loads(p.stdout)
    def resolve(ref):
        value=schema
        for part in ref.removeprefix('#/').split('/'):
            value=value[part.replace('~1','/').replace('~0','~')]
        return value
    flexibility=resolve('#/$defs/github.com/databricks/databricks-sdk-go/service/compute.NodeTypeFlexibility')
    object_schema=next(v for v in flexibility['oneOf'] if v.get('type')=='object')
    field=object_schema['properties']['alternate_node_type_ids']
    array=resolve(field['$ref'])
    array_schema=next(v for v in array['oneOf'] if v.get('type')=='array')
    assert array_schema.get('minItems',0)==0
    assert resolve(array_schema['items']['$ref'])['type']=='string'


def test_real_cli_jobs_update_posts_only_approved_cluster_settings(tmp_path):
    old,uc=settings();target=target_clusters(old,uc)
    expected={'job_id':int(JOB_ID),'new_settings':{'job_clusters':target}}
    seen=[]
    class Handler(BaseHTTPRequestHandler):
        def log_message(self,*a):pass
        def do_GET(self):
            assert self.path=='/.well-known/databricks-config'
            self.send_response(404);self.send_header('Content-Length','0');self.end_headers()
        def do_POST(self):
            data=json.loads(self.rfile.read(int(self.headers['Content-Length'])))
            valid=self.path=='/api/2.1/jobs/update' and data==expected and self.headers.get('Authorization')=='Bearer synthetic-loopback-only'
            seen.append((self.path,data))
            self.send_response(200 if valid else 400);self.send_header('Content-Type','application/json');self.send_header('Content-Length','2');self.end_headers();self.wfile.write(b'{}')
    server=ThreadingHTTPServer(('127.0.0.1',0),Handler);thread=threading.Thread(target=server.serve_forever,daemon=True);thread.start()
    env={k:v for k,v in os.environ.items() if not k.startswith('DATABRICKS_')}
    env.update(DATABRICKS_HOST='http://127.0.0.1:'+str(server.server_port),DATABRICKS_AUTH_TYPE='pat',DATABRICKS_TOKEN='synthetic-loopback-only',DATABRICKS_CONFIG_FILE=str(tmp_path/'none'),NO_PROXY='127.0.0.1,localhost')
    try:
        p=subprocess.run([CLI,'api','post','/api/2.1/jobs/update','--json',json.dumps(expected)],cwd=tmp_path,env=env,capture_output=True,timeout=20)
        assert p.returncode==0,p.stderr
        assert seen==[('/api/2.1/jobs/update',expected)]
    finally:server.shutdown();server.server_close();thread.join(timeout=5)
