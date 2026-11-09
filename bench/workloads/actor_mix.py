"""Four deterministic workloads and a persistent RAM probe inside a gVisor Actor."""
import argparse
import ast
import csv
import hashlib
import http.server
import io
import json
import os
from pathlib import Path
import resource
import subprocess
import sys
import threading
import time
import urllib.request
import uuid

PORT = 18771


def memory_server(mib):
    # Random data materializes every page and exercises snapshot transfer cost.
    heap = bytearray(os.urandom(mib * 2**20))
    tree = ast.parse('def value(x):\n    return x * 7 + 11\n')
    nonce = uuid.uuid4().hex
    context = {'turns': 0, 'workspace_nonce': nonce}

    class Handler(http.server.BaseHTTPRequestHandler):
        def do_GET(self):
            if self.path == '/advance':
                context['turns'] += 1
            body = json.dumps(dict(nonce=nonce, pid=os.getpid(), heap_bytes=len(heap),
                heap_sha256=hashlib.sha256(heap).hexdigest(), ast_identity=id(tree),
                ast_sha256=hashlib.sha256(ast.dump(tree).encode()).hexdigest(), context=context,
                max_rss_kib=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)).encode()
            self.send_response(200)
            self.send_header('Content-Type', 'application/json')
            self.send_header('Content-Length', str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *_):
            pass

    http.server.HTTPServer(('127.0.0.1', PORT), Handler).serve_forever()


def proof(advance=False):
    with urllib.request.urlopen(f'http://127.0.0.1:{PORT}/'+('advance' if advance else 'proof'), timeout=10) as response:
        return json.load(response)


def start_memory(mib):
    try:
        return proof()
    except OSError:
        pass
    with open('memory-probe.log', 'ab', buffering=0) as log:
        subprocess.Popen([sys.executable, str(Path(__file__).resolve()), 'memory', '--memory-mib', str(mib)],
                         stdin=subprocess.DEVNULL, stdout=log, stderr=log, start_new_session=True)
    deadline = time.monotonic()+15
    while time.monotonic() < deadline:
        try:
            return proof()
        except OSError:
            time.sleep(.05)
    raise RuntimeError('Actor RAM probe did not become ready')


def workload(kind, seed, turn):
    begin = time.perf_counter()
    directory = Path(f'round-{turn}')
    directory.mkdir(exist_ok=True)
    if kind == 'coding':
        modules = []
        for m in range(8):
            text = '\n'.join(f'def f{i}(x):\n    return (x * {i+1} + {seed}) % 1000003' for i in range(256))
            path = directory/f'module{m}.py'
            path.write_text(text)
            modules.append(path)
        subprocess.run([sys.executable, '-m', 'compileall', '-q', str(directory)], check=True)
        namespace = {}
        exec(compile(modules[0].read_text(), str(modules[0]), 'exec'), namespace)
        assert namespace['f3'](13) == (52+seed) % 1000003
        result = dict(kind='multi-file-python-compilation', modules=len(modules), functions=2048,
                      test_assertions=1, compiled_files=len(list(directory.rglob('*.pyc'))))
    elif kind == 'research':
        articles = [(f'source={seed}:{i}\n'+'research evidence data\n'*4096).encode() for i in range(8)]

        class Source(http.server.BaseHTTPRequestHandler):
            def do_GET(self):
                time.sleep(.025)
                data = articles[int(self.path[1:])]
                self.send_response(200)
                self.send_header('Content-Length', str(len(data)))
                self.end_headers()
                self.wfile.write(data)

            def log_message(self, *_):
                pass

        server = http.server.ThreadingHTTPServer(('127.0.0.1', 0), Source)
        thread = threading.Thread(target=server.serve_forever, kwargs={'poll_interval': .01}, daemon=True)
        thread.start()
        try:
            retrieved = []
            for i in range(len(articles)):
                with urllib.request.urlopen(f'http://127.0.0.1:{server.server_port}/{i}', timeout=5) as response:
                    content = response.read()
                assert content == articles[i]
                (directory/f'source-{i}.txt').write_bytes(content)
                retrieved.append(content)
        finally:
            server.shutdown()
            server.server_close()
            thread.join()
        result = dict(kind='http-research-and-source-verification', documents=8,
                      bytes_read=sum(map(len, retrieved)), source='Actor-local controlled HTTP fixture',
                      per_document_service_delay_seconds=.025,
                      corpus_sha256=hashlib.sha256(b''.join(retrieved)).hexdigest())
    elif kind == 'batch':
        expected = 0
        for batch in range(16):
            rows = [(batch*256+i, (i+seed) % 997) for i in range(256)]
            expected += sum(value for _, value in rows)
            with (directory/f'batch-{batch}.csv').open('w') as handle:
                csv.writer(handle).writerows(rows)
                handle.flush()
                os.fsync(handle.fileno())
        actual = 0
        for path in directory.glob('*.csv'):
            with path.open() as handle:
                actual += sum(int(row[1]) for row in csv.reader(handle))
        assert actual == expected
        result = dict(kind='fsync-csv-batch-and-aggregation', files=16, rows=4096, checksum=actual)
    elif kind == 'interactive':
        data = {'workspace': seed, 'round': turn, 'items': [{'price': i+1, 'quantity': 2} for i in range(20)]}
        (directory/'request.json').write_text(json.dumps(data))
        parsed = json.loads((directory/'request.json').read_text())
        total = sum(item['price']*item['quantity'] for item in parsed['items'])
        assert total == 420
        result = dict(kind='interactive-file-query', items=20, total=total)
    else:
        raise ValueError(kind)
    result.update(wall_seconds=time.perf_counter()-begin, seed=seed, turn=turn, ram=proof(True))
    (directory/'receipt.json').write_text(json.dumps(result))
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action', choices=['start', 'memory', 'proof', 'work'])
    parser.add_argument('--memory-mib', type=int, default=16)
    parser.add_argument('--kind', choices=['coding', 'research', 'batch', 'interactive'], default='coding')
    parser.add_argument('--seed', type=int, default=7101)
    parser.add_argument('--turn', type=int, default=0)
    args = parser.parse_args()
    if args.memory_mib < 1 or args.memory_mib > 1024:
        parser.error('memory working set must be within 1..1024 MiB')
    if args.action == 'memory':
        memory_server(args.memory_mib)
        return
    result = start_memory(args.memory_mib) if args.action == 'start' else proof() if args.action == 'proof' else workload(args.kind, args.seed, args.turn)
    print(json.dumps(result))


if __name__ == '__main__':
    main()
