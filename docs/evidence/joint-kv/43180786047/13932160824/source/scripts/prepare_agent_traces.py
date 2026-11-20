#!/usr/bin/env python3
"""Freeze content-free numeric agent traces; source snapshots stay outside public docs."""
import argparse
import json
import logging
from pathlib import Path
import sys
import time
import urllib.error
import urllib.parse
import urllib.request

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))
from bench.agent_traces import audit_repropatch, export_nebius, merge_manifests, write_manifest
from bench.agent_traces.workload import nebius_task_group

DATASET='nebius/SWE-rebench-openhands-trajectories'


def fetch(url, revision=None):
    for attempt in range(5):
        try:
            with urllib.request.urlopen(url,timeout=45) as response:
                if revision is not None and response.headers.get('x-revision')!=revision:
                    raise ValueError('dataset-server row revision mismatch')
                return json.load(response)
        except (urllib.error.URLError, TimeoutError):
            if attempt==4: raise
            logging.info('source endpoint retry=%s',attempt+1)
            time.sleep(min(8,2**attempt))


def download_nebius(output, count, revision):
    """Freeze official rows only when current revision matches the declared source."""
    if type(count) is not int or count<1:raise ValueError('independent task count must be positive')
    metadata=fetch('https://huggingface.co/api/datasets/'+DATASET)
    if metadata['sha']!=revision or metadata.get('cardData',{}).get('license')!='cc-by-4.0':
        raise ValueError('requested revision/license differs from official dataset metadata')
    output=Path(output);output.parent.mkdir(parents=True,exist_ok=True)
    state_path=output.with_suffix('.download-state.json')
    rows=[json.loads(line) for line in output.read_text().splitlines() if line.strip()] if output.exists() else []
    tasks={nebius_task_group(row) for row in rows}
    state=json.loads(state_path.read_text()) if state_path.exists() else {}
    if state and state.get('revision')!=revision:raise ValueError('acquisition checkpoint revision mismatch')
    offset=state.get('offset',0)
    if rows and not state_path.exists():raise ValueError('snapshot exists without acquisition state')
    while len(tasks)<count:
        query=urllib.parse.urlencode({'dataset':DATASET,'config':'default','split':'train','offset':offset,'length':10})
        try:
            page=fetch('https://datasets-server.huggingface.co/rows?'+query,revision=revision)
        except urllib.error.URLError:
            query=urllib.parse.urlencode({'dataset':DATASET,'config':'default','split':'train','offset':offset,'length':1})
            page=fetch('https://datasets-server.huggingface.co/rows?'+query,revision=revision)
        if not page.get('rows'):raise ValueError('insufficient independent task groups')
        consumed=0
        for item in page['rows']:
            consumed+=1
            if item.get('truncated_cells'):raise ValueError('source row has truncated cells')
            row=item['row'];task=nebius_task_group(row)
            if task in tasks:continue
            tasks.add(task);rows.append(row)
            if len(tasks)==count:break
        offset+=consumed
        with output.open('w') as stream:
            for row in rows:stream.write(json.dumps(row,ensure_ascii=False,sort_keys=True,separators=(',',':'))+'\n')
        output.chmod(0o600)
        state_path.write_text(json.dumps({'offset':offset,'revision':revision}))
        logging.info('official Nebius rows=%s independent_tasks=%s',offset,len(tasks))
    final=fetch('https://huggingface.co/api/datasets/'+DATASET)
    if final['sha']!=revision:raise ValueError('dataset revision changed during acquisition')
    with output.open('w') as stream:
        for row in rows:stream.write(json.dumps(row,ensure_ascii=False,sort_keys=True,separators=(',',':'))+'\n')
    output.chmod(0o600)
    output.with_suffix('.source.json').write_text(json.dumps({'dataset':DATASET,'revision':revision,'license':'CC-BY-4.0','split':'train','acquisition':'official datasets-server /rows; dataset revision checked before and after; full rows, no truncated cells; fresh downloads additionally verify each x-revision response header','scanned_rows':offset,'independent_tasks':len(tasks)},sort_keys=True,indent=2)+'\n')


def main():
    parser=argparse.ArgumentParser()
    parser.add_argument('--repropatch-root',type=Path)
    parser.add_argument('--nebius-rows',type=Path)
    parser.add_argument('--download-nebius',type=Path)
    parser.add_argument('--merge-manifests',type=Path,nargs='+')
    parser.add_argument('--revision',default='35455389ab51bf5e2306bfd436ef72d0f98bf882')
    parser.add_argument('--independent-tasks',type=int,default=128)
    parser.add_argument('--output',type=Path,required=True)
    parser.add_argument('--max-turns',type=int,default=6)
    parser.add_argument('--token-scale',type=float,default=.1)
    parser.add_argument('--output-scale',type=float,default=.05)
    parser.add_argument('--time-scale',type=float,default=.1)
    parser.add_argument('--wait-model-ms',type=float,default=30000)
    args=parser.parse_args()
    if sum(bool(value) for value in (args.repropatch_root,args.nebius_rows,args.download_nebius,args.merge_manifests))!=1:
        parser.error('provide one of --repropatch-root, --nebius-rows, --download-nebius or --merge-manifests')
    logging.basicConfig(level=logging.INFO,format='%(asctime)s | %(levelname)s | %(message)s')
    if args.download_nebius:
        download_nebius(args.download_nebius,args.independent_tasks,args.revision)
        args.nebius_rows=args.download_nebius
    options={k:getattr(args,k) for k in ['max_turns','token_scale','output_scale','time_scale','wait_model_ms']}
    if args.merge_manifests:
        result=merge_manifests(args.merge_manifests)
    elif args.repropatch_root:
        result=audit_repropatch(args.repropatch_root,**options)
    elif args.nebius_rows:
        result=export_nebius(args.nebius_rows,revision=args.revision,**options)
    else:parser.error('provide --repropatch-root or --nebius-rows')
    write_manifest(args.output,result)
    logging.info('manifest=%s independent_tasks=%s trajectories=%s sha256=%s',args.output,result['audit']['independent_task_groups'],result['audit']['trajectories'],result['manifest_sha256'])
    return 0

if __name__=='__main__':raise SystemExit(main())
