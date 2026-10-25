"""Real CLI v1.22.1 + strict loopback Workspace API, never a cloud endpoint.

SILVER_DATABRICKS_BIN opts in. The token is synthetic and accepted only locally.
This verifies the client/wire contract, NOT live Databricks service behavior.
"""
import base64
import hashlib
import json
import os
from pathlib import Path
import subprocess
import threading
from http.server import BaseHTTPRequestHandler,ThreadingHTTPServer
from urllib.parse import urlsplit,parse_qs
import pytest

CLI=os.environ.get('SILVER_DATABRICKS_BIN')
pytestmark=pytest.mark.skipif(not CLI,reason='Set SILVER_DATABRICKS_BIN to the pinned real CLI')
OBJECT='/Shared/admm_dev/admm-silver/dev/artifacts/admm_silver-0.1.0+78184e3e3de9-py3-none-any.whl'
PAYLOAD=(b'PK\x03\x04\x00\xff\x80\r\n'+bytes(range(256))*64)[:16215]

@pytest.mark.parametrize('case',['AUTO','RAW','forbidden','missing','too-large','invalid-base64'])
def test_real_cli_workspace_file_export_contract(tmp_path,case):
    seen=[]
    class Handler(BaseHTTPRequestHandler):
        def log_message(self,*args):pass
        def do_GET(self):
            parsed=urlsplit(self.path);query=parse_qs(parsed.query)
            if parsed.path=='/.well-known/databricks-config':
                self.send_response(404);self.send_header('Content-Length','0');self.end_headers();return
            seen.append((parsed.path,query))
            valid=(parsed.path=='/api/2.0/workspace/export' and query=={'path':[OBJECT],'format':['RAW' if case=='RAW' else 'AUTO']} and
                   self.headers.get('Authorization')=='Bearer synthetic-loopback-only')
            if not valid:
                status=400;body={'error_code':'INVALID_PARAMETER_VALUE','message':'Invalid contract request'}
            elif case=='RAW':
                status=400;body={'error_code':'INVALID_PARAMETER_VALUE','message':'Exporting a FILE asset in RAW format is not supported.'}
            elif case=='forbidden':
                status=403;body={'error_code':'PERMISSION_DENIED','message':'PERMISSION_DENIED'}
            elif case=='missing':
                status=404;body={'error_code':'RESOURCE_DOES_NOT_EXIST','message':'RESOURCE_DOES_NOT_EXIST'}
            elif case=='too-large':
                status=400;body={'error_code':'MAX_NOTEBOOK_SIZE_EXCEEDED','message':'MAX_NOTEBOOK_SIZE_EXCEEDED'}
            else:
                status=200;body={'content':'not-base64!' if case=='invalid-base64' else base64.b64encode(PAYLOAD).decode('ascii'),'file_type':'whl'}
            data=json.dumps(body).encode();self.send_response(status);self.send_header('Content-Type','application/json');self.send_header('Content-Length',str(len(data)));self.end_headers();self.wfile.write(data)
    server=ThreadingHTTPServer(('127.0.0.1',0),Handler)
    thread=threading.Thread(target=server.serve_forever,daemon=True);thread.start()
    destination=tmp_path/'downloaded.whl'
    env={k:v for k,v in os.environ.items() if not k.startswith('DATABRICKS_')}
    env.update(DATABRICKS_HOST='http://127.0.0.1:'+str(server.server_port),DATABRICKS_AUTH_TYPE='pat',
        DATABRICKS_TOKEN='synthetic-loopback-only',DATABRICKS_CONFIG_FILE=str(tmp_path/'no-credentials'),NO_PROXY='127.0.0.1,localhost')
    try:
        version=subprocess.run([CLI,'version'],env=env,capture_output=True,text=True,timeout=10)
        assert version.returncode==0 and '1.22.1' in version.stdout
        result=subprocess.run([CLI,'workspace','export',OBJECT,'--format','RAW' if case=='RAW' else 'AUTO','--file',str(destination)],
            cwd=tmp_path,env=env,capture_output=True,timeout=15)
        assert len(seen)==1
        if case=='AUTO':
            assert result.returncode==0,result.stderr
            assert destination.read_bytes()==PAYLOAD and destination.stat().st_size==16215
            assert hashlib.sha256(destination.read_bytes()).digest()==hashlib.sha256(PAYLOAD).digest()
        else:
            assert result.returncode!=0
            assert not destination.exists() # No false binary or partial success.
            if case=='RAW':assert b'RAW format is not supported' in result.stderr
    finally:server.shutdown();server.server_close();thread.join(timeout=5)
