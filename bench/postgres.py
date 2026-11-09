"""Private environment-based PostgreSQL credentials for isolated experiment databases."""
import os
import re
import subprocess
import urllib.parse


def connection(admin_url, database):
    url=urllib.parse.urlsplit(admin_url)
    if url.scheme not in ('postgres','postgresql'):
        raise ValueError('PostgreSQL URI required')
    if not re.fullmatch(r'[a-z_][a-z0-9_]{0,62}', database):
        raise ValueError('isolated database name must be a lowercase SQL identifier')
    value=urllib.parse.urlunsplit(url._replace(path='/'+database))
    if not url.netloc:
        value=url.scheme+'://'+value[len(url.scheme)+1:]
    return value


def execute(admin_url, sql):
    url=urllib.parse.urlsplit(admin_url);query=urllib.parse.parse_qs(url.query)
    env=dict(os.environ)
    for key,value in {'PGHOST':url.hostname or query.get('host',[None])[0], 'PGPORT':url.port,
                      'PGUSER':urllib.parse.unquote(url.username) if url.username else None,
                      'PGPASSWORD':urllib.parse.unquote(url.password) if url.password else None,
                      'PGDATABASE':url.path.lstrip('/') or 'postgres', 'PGSSLMODE':query.get('sslmode',[None])[0]}.items():
        if value is not None: env[key]=str(value)
    result=subprocess.run(['psql','-X','-v','ON_ERROR_STOP=1','-tA','-c',sql],env=env,capture_output=True,text=True,timeout=30)
    if result.returncode: raise RuntimeError('isolated experiment database command failed: '+result.stderr[-500:])
    return result.stdout.strip()
