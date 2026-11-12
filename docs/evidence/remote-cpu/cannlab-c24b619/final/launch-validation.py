from pathlib import Path
import datetime as dt,hashlib,json,os,shlex,subprocess,sys,tarfile
ROOT=Path('/home/developer/agentcloud-joint-kv-20261112-c24b619-cannlab/validation')
CPU=[0,1,2,3]
def digest(path):
    value=hashlib.sha256()
    with path.open('rb') as source:
        while chunk:=source.read(1<<20):value.update(chunk)
    return value.hexdigest()
def extract(path,destination):
    with tarfile.open(path) as archive:
        for member in archive.getmembers():
            target=(destination/member.name).resolve()
            if os.path.commonpath([str(destination.resolve()),str(target)])!=str(destination.resolve()) or not (member.isdir() or member.isfile()):raise ValueError('unsafe archive member')
        archive.extractall(destination)
def env():
    return dict(PATH=str(ROOT/'toolchain/go/bin')+':/usr/local/bin:/usr/bin:/bin',LANG='C.UTF-8',CUDA_VISIBLE_DEVICES='',GOMAXPROCS='4',GOMEMLIMIT='3GiB',CGO_ENABLED='1',GOTOOLCHAIN='local',GOENV='off',GOPROXY='off',GOSUMDB='off',GOFLAGS='-p=4',GOPATH=str(ROOT/'gopath'),GOMODCACHE=str(ROOT/'go-mod-cache'),GOCACHE=str(ROOT/'go-build-cache'),GOTMPDIR=str(ROOT/'tmp'),TMPDIR=str(ROOT/'tmp'))
def output(args):return subprocess.check_output(args,env=env(),cwd=ROOT/'repo',text=True).strip()
def run():
    os.sched_setaffinity(0,set(CPU))
    source=json.loads((ROOT/'downloads/source-manifest.json').read_text())
    limits=dict(cpu_ids=CPU,GOMAXPROCS=4,GOMEMLIMIT='3GiB',max_rss_mib=4096,min_available_mib=8192,contracted_memory_gib=32,combined_reserved_cpu_ids=[0,1,2,3,4,5],combined_rss_budget_mib=8192)
    cgroup={}
    for path in ['/sys/fs/cgroup/cpu/cpu.cfs_quota_us','/sys/fs/cgroup/cpu/cpu.cfs_period_us','/sys/fs/cgroup/cpuset/cpuset.cpus','/sys/fs/cgroup/memory/memory.limit_in_bytes']:
        if Path(path).is_file():cgroup[path]=Path(path).read_text().strip()
    report=dict(schema_version=1,execution_kind='arm64-go-contract-validation',status='running',commit=source['commit'],source_sha256=source['source_sha256'],python_runtime_source=json.loads((ROOT/'downloads/runtime-manifest.json').read_text()),python_version=output(['python3','--version']),source_directory=str(ROOT/'repo'),source_sync='verified_git_object_source_archive',source_archive_sha256=source['source_archive_sha256'],module_archive_sha256=source['module_archive_sha256'],module_uncompressed_sha256=source['module_uncompressed_sha256'],go_release=json.loads((ROOT/'downloads/go-release.json').read_text()),go_version=output([str(ROOT/'toolchain/go/bin/go'),'version']),gcc_version=output(['gcc','--version']).splitlines()[0],arch=os.uname().machine,cgo_enabled='1',process_id=os.getpid(),started_at=dt.datetime.now(dt.timezone.utc).isoformat(),resource_limits=limits,cgroups=cgroup,actual_affinity=sorted(os.sched_getaffinity(0)),environment=env(),postgresql_executed=False,postgresql_admin_configured=False,commands=[])
    target=ROOT/'validation.json';target.write_text(json.dumps(report,indent=2)+'\n')
    go=str(ROOT/'toolchain/go/bin/go')
    for name,argv,timeout in [('race',[go,'test','-race','-count=1','-p','4','-json','./...'],1800),('vet',[go,'vet','-p','4','./...'],900)]:
        log=ROOT/'logs'/('go-'+name+'-c24b619.log')
        guard=[sys.executable,'scripts/resource_guard.py','--max-rss-mib','4096','--min-available-mib','8192','--timeout',str(timeout),'--log',str(log),'--','taskset','-c','0-3']+argv
        start=dt.datetime.now(dt.timezone.utc).isoformat()
        result=subprocess.run(guard,cwd=ROOT/'repo',env=env(),stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True)
        stdout=ROOT/'logs'/('go-'+name+'-guard.stdout.log');stderr=ROOT/'logs'/('go-'+name+'-guard.stderr.log');stdout.write_text(result.stdout);stderr.write_text(result.stderr)
        record=dict(name=name,argv=argv,guard_argv=guard,started_at=start,exit_code=result.returncode,stdout=str(stdout),stderr=str(stderr),log=str(log))
        try:record['guard']=json.loads(result.stdout.strip().splitlines()[-1])
        except (ValueError,IndexError):record['guard']=dict(status='failed',error='resource guard returned no JSON')
        report['commands'].append(record)
        target.write_text(json.dumps(report,indent=2)+'\n')
    events=[]
    for line in (ROOT/'logs/go-race-c24b619.log').read_text().splitlines():
        if line.startswith('{'):
            try:events.append(json.loads(line))
            except ValueError:pass
    report['top_level_tests_passed']=sum(e.get('Action')=='pass' and bool(e.get('Test')) and '/' not in e['Test'] for e in events)
    report['top_level_tests_skipped']=[e['Package']+':'+e['Test'] for e in events if e.get('Action')=='skip' and e.get('Test') and '/' not in e['Test']]
    report['status']='passed' if all(c['exit_code']==0 for c in report['commands']) else 'failed'
    report['finished_at']=dt.datetime.now(dt.timezone.utc).isoformat()
    report['logs_sha256']={str(path.relative_to(ROOT)):digest(path) for path in sorted((ROOT/'logs').glob('*.log')) if 'wrapper' not in path.name}
    target.write_text(json.dumps(report,indent=2)+'\n')
    print(json.dumps({'status':report['status'],'top_level_tests_passed':report['top_level_tests_passed'],'top_level_tests_skipped':len(report['top_level_tests_skipped']),'commands':[dict(name=c['name'],guard=c['guard']) for c in report['commands']]}),flush=True)
    return 0 if report['status']=='passed' else 1
def prepare():
    source=json.loads((ROOT/'downloads/source-manifest.json').read_text())
    modules=ROOT/'downloads/go-modules-293e19f.tar.gz'
    with modules.open('wb') as target:
        for path in sorted((ROOT/'downloads').glob('module-part-*')):
            with path.open('rb') as incoming:
                while chunk:=incoming.read(1<<20):target.write(chunk)
    if digest(modules)!=source['module_archive_sha256']:raise ValueError('module archive checksum differs')
    archive=ROOT/'downloads/source-c24b619.tar.gz'
    if digest(archive)!=source['source_archive_sha256']:raise ValueError('source archive checksum differs')
    extract(archive,ROOT/'repo');extract(modules,ROOT/'go-mod-cache')
    for entry in source['files']:
        path=ROOT/'repo'/entry['path']
        if digest(path)!=entry['sha256'] or path.stat().st_size!=entry['bytes']:raise ValueError('source member checksum differs: '+entry['path'])
    value=hashlib.sha256()
    for entry in sorted(source['files'],key=lambda x:x['path']):
        name=entry['path']
        if name.startswith('upstream/') or name=='scripts/resource_guard.py':continue
        value.update(name.encode()+b'\0'+(ROOT/'repo'/name).read_bytes()+b'\0')
    if value.hexdigest()!=source['source_sha256']:raise ValueError('Go source digest differs')
    runtime=json.loads((ROOT/'downloads/runtime-manifest.json').read_text())
    runtime_archive=ROOT/'downloads/runtime-c24b619.tar.gz'
    if digest(runtime_archive)!=runtime['archive_sha256']:raise ValueError('runtime archive checksum differs')
    extract(runtime_archive,ROOT/'repo')
    for entry in runtime['files']:
        path=ROOT/'repo'/entry['path']
        if digest(path)!=entry['sha256'] or path.stat().st_size!=entry['bytes']:raise ValueError('runtime source checksum differs: '+entry['path'])
    output(['python3','-c','import runtime.tools_server'])
    with (ROOT/'logs/go-validation-c24b619.wrapper.log').open('wb') as target:
        child=subprocess.Popen(['taskset','-c','0-3',sys.executable,str(Path(__file__).resolve()),'run'],cwd=ROOT/'repo',stdin=subprocess.DEVNULL,stdout=target,stderr=subprocess.STDOUT,start_new_session=True)
    (ROOT/'logs/go-validation-c24b619.pid').write_text(str(child.pid)+'\n')
    print(json.dumps({'pid':child.pid,'root':str(ROOT),'source_sha256':source['source_sha256'],'status':'launched'}))
if __name__=='__main__':
    if len(sys.argv)>1 and sys.argv[1]=='run':sys.exit(run())
    prepare()
