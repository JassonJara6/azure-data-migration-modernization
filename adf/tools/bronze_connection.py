"""ODBC configuration without shell interpolation of passwords."""
import getpass
import os
import re
import warnings
from bronze_diagnostics import fail


def odbc_value(value):
    if not isinstance(value,str) or '\x00' in value:raise ValueError('Invalid ODBC value')
    return '{'+value.replace('}','}}')+'}'


def build_connection(server,user,password,*,database='AdventureWorks',trust_local_certificate=False):
    if database!='AdventureWorks':raise ValueError('Expected AdventureWorks')
    if not user or not password:raise ValueError('SQL credentials are required')
    host=re.sub(r'^tcp:','',server,flags=re.I).split(',')[0].lower()
    if trust_local_certificate and host not in ('127.0.0.1','localhost','[::1]','::1'):
        raise ValueError('Certificate exception is restricted to loopback SQL')
    values={'DRIVER':'ODBC Driver 18 for SQL Server','SERVER':server,'DATABASE':database,
            'UID':user,'PWD':password,'Encrypt':'yes',
            'TrustServerCertificate':'yes' if trust_local_certificate else 'no'}
    return ';'.join(key+'='+odbc_value(value) for key,value in values.items())+';'


def prompt_password():
    # Refuse getpass's echoing fallback (e.g. redirected stdin / some terminals).
    with warnings.catch_warnings():
        warnings.simplefilter('error',getpass.GetPassWarning)
        try:return getpass.getpass('Contraseña SQL de bronze_reconciler (entrada oculta): ')
        except getpass.GetPassWarning:
            fail('sql_credentials','SECURE_PROMPT_UNAVAILABLE','El terminal no permite entrada oculta.',
                 'En Git Bash ejecuta el comando con winpty; no pegues la contraseña en argumentos ni en la cadena de conexión.')


def configured_connection(*,interactive=False,server='127.0.0.1,1433',user='bronze_reconciler',trust_local_certificate=False):
    if interactive:
        return build_connection(server,user,prompt_password(),trust_local_certificate=trust_local_certificate)
    connection=os.environ.get('BRONZE_SQL_CONNECTION_STRING')
    if not connection:
        fail('sql_credentials','SQL_CONFIGURATION_MISSING','Falta configuración de conexión SQL.',
             'Usa --interactive para construirla sin exponer la contraseña o proporciona BRONZE_SQL_CONNECTION_STRING mediante un gestor seguro.')
    return connection
