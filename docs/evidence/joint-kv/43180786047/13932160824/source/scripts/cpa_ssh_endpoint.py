#!/usr/bin/env python3
"""One bounded CPA request for an SSH authorized_keys forced command."""
from __future__ import annotations

import argparse
import base64
import fcntl
import hmac
import json
from pathlib import Path
import resource
import signal
import sys
import urllib.error
import urllib.request

REQUEST_LIMIT = 1 << 20
RESPONSE_LIMIT = 4 << 20


class Rejected(ValueError):
    def __init__(self, status, message):
        super().__init__(message)
        self.status = status


def validate(request, config):
    if not hmac.compare_digest(str(request.get('key', '')).encode(), config['client_key'].encode()):
        raise Rejected(401, 'CPA relay authentication required')
    method, path = request.get('method'), request.get('path')
    if (method, path) not in (('GET', '/v1/models'), ('POST', '/v1/chat/completions')):
        raise Rejected(400, 'unsupported CPA operation')
    body = base64.b64decode(request.get('body') or '', validate=True)
    if len(body) > REQUEST_LIMIT:
        raise Rejected(413, 'CPA request exceeds limit')
    if method == 'GET' and body:
        raise Rejected(400, 'model catalog requires an empty body')
    if method == 'POST':
        payload = json.loads(body)
        if not isinstance(payload, dict) or payload.get('model') != config['model']:
            raise Rejected(403, 'model is outside this relay credential')
        if payload.get('stream') not in (None, False):
            raise Rejected(400, 'relay requests use complete JSON responses')
    return method, path, body


def relay(request, config):
    method, path, body = validate(request, config)
    upstream = urllib.request.Request('http://127.0.0.1:8317' + path,
        data=body if method == 'POST' else None, method=method,
        headers={'Content-Type': 'application/json', 'Authorization': 'Bearer ' + config['api_key'],
                 'Connection': 'close'})
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    try:
        response = opener.open(upstream, timeout=270)
    except urllib.error.HTTPError as error:
        response = error
    with response:
        length = response.headers.get('Content-Length')
        if length and int(length) > RESPONSE_LIMIT:
            raise Rejected(502, 'CPA response exceeds limit')
        payload = response.read(RESPONSE_LIMIT + 1)
        if len(payload) > RESPONSE_LIMIT:
            raise Rejected(502, 'CPA response exceeds limit')
        if path == '/v1/models' and response.status == 200:
            catalog = json.loads(payload)
            catalog['data'] = [item for item in catalog['data'] if item.get('id') == config['model']]
            payload = json.dumps(catalog).encode()
        return dict(status=response.status, content_type='application/json', body=base64.b64encode(payload).decode())


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--config', type=Path, required=True)
    args = parser.parse_args()
    resource.setrlimit(resource.RLIMIT_AS, (256 * 2**20, 256 * 2**20))
    resource.setrlimit(resource.RLIMIT_CPU, (30, 30))
    signal.alarm(300)
    held = None
    try:
        config = json.loads(args.config.read_text())
        request = json.loads(sys.stdin.buffer.readline(2 * REQUEST_LIMIT))
        validate(request, config)
        # Per-host slots cover simultaneous hosted runners without a daemon.
        slots = args.config.parent / 'slots'
        slots.mkdir(mode=0o700, exist_ok=True)
        for index in range(4):
            handle = (slots / str(index)).open('a')
            try:
                fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
                held = handle
                break
            except BlockingIOError:
                handle.close()
        if held is None:
            raise Rejected(429, 'CPA relay concurrency limit reached')
        reply = relay(request, config)
    except Exception as error:
        status = error.status if isinstance(error, Rejected) else 502
        message = str(error) if isinstance(error, Rejected) else 'CPA transport outcome requires reconciliation'
        body = json.dumps({'error': {'code': 'cpa_relay_error', 'message': message}}).encode()
        reply = dict(status=status, content_type='application/json', body=base64.b64encode(body).decode())
    finally:
        if held is not None:
            held.close()
    print(json.dumps(reply))


if __name__ == '__main__':
    main()
