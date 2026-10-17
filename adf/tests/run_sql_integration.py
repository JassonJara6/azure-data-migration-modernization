"""Create/dispose an isolated SQL Server Developer container for integration tests.

Requires Docker and 3 GB available RAM. Does not connect to an existing SQL
instance or Azure. The generated test password never appears in command args.
"""
import os
import secrets
import subprocess
import sys
import time
from pathlib import Path


def main():
    name='codex-bronze-sql-test-'+secrets.token_hex(5)
    password='Test!'+secrets.token_hex(24)
    env=dict(os.environ,MSSQL_SA_PASSWORD=password,SQLCMDPASSWORD=password)
    started=False
    try:
        subprocess.run(['docker','run','-d','--name',name,'--memory','3g','-e','ACCEPT_EULA=Y',
           '-e','MSSQL_PID=Developer','-e','MSSQL_SA_PASSWORD','mcr.microsoft.com/mssql/server:2022-latest'],
           env=env,check=True,stdout=subprocess.DEVNULL)
        started=True
        ready=False;deadline=time.monotonic()+180
        while time.monotonic()<deadline:
            result=subprocess.run(['docker','exec','-e','SQLCMDPASSWORD',name,'/opt/mssql-tools18/bin/sqlcmd',
              '-S','localhost','-U','sa','-C','-b','-Q','SELECT 1'],env=env,capture_output=True)
            if result.returncode==0:ready=True;break
            time.sleep(2)
        if not ready:raise RuntimeError('Isolated SQL Server did not become ready')
        root=Path(__file__).resolve().parents[2]
        result=subprocess.run([sys.executable,'-m','pytest','-q','adf/tests/test_sqlserver_integration.py'],cwd=root,
          env=dict(os.environ,BRONZE_SQL_TEST_CONTAINER=name,BRONZE_SQL_TEST_PASSWORD=password))
        return result.returncode
    finally:
        if started:subprocess.run(['docker','rm','-f',name],stdout=subprocess.DEVNULL,check=True)

if __name__=='__main__':raise SystemExit(main())
