"""Verify file digests; optionally validate proposed contracts and DuckDB DDL.

This check does not certify event semantics, clock synchronization, an ETL, or
simulation/real-runtime accuracy. Those remain research implementation gates.
"""
import argparse
import csv
import hashlib
import json
from pathlib import Path

p=argparse.ArgumentParser()
p.add_argument('--root',type=Path,required=True)
p.add_argument('--contracts',action='store_true')
a=p.parse_args()
manifest=json.loads((a.root/'package_manifest.json').read_text())
errors=[]
for item in manifest['files']:
    path=a.root/item['path']
    if not path.is_file():
        errors.append({'path':item['path'],'error':'missing'})
    elif hashlib.sha256(path.read_bytes()).hexdigest()!=item['sha256']:
        errors.append({'path':item['path'],'error':'sha256_mismatch'})
result={'checked_files':len(manifest['files']),'digest_errors':errors}
if a.contracts:
    import jsonschema
    import duckdb
    base=a.root/'contracts'
    schema=json.loads((base/'event.schema.json').read_text())
    example=json.loads((base/'example_decision.json').read_text())
    jsonschema.Draft202012Validator.check_schema(schema)
    jsonschema.Draft202012Validator(schema,format_checker=jsonschema.FormatChecker()).validate(example)
    conn=duckdb.connect(':memory:')
    conn.execute((base/'tables.sql').read_text())
    count=conn.execute("select count(*) from information_schema.tables where table_schema='agentcloud_research'").fetchone()[0]
    with (base/'field_dictionary.csv').open(encoding='utf-8-sig',newline='') as f: fields=list(csv.DictReader(f))
    with (base/'metric_dictionary.csv').open(encoding='utf-8-sig',newline='') as f: metrics=list(csv.DictReader(f))
    result['contracts']={'schema_and_example':'valid','duckdb_tables':count,'field_rows':len(fields),'metric_rows':len(metrics),'status':'structural_only'}
print(json.dumps(result,ensure_ascii=False,indent=2))
raise SystemExit(bool(errors))
