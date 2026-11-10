"""Authenticated AgentCloud HTTP client; request IDs are supplied by the caller."""
from __future__ import annotations
import json
import http.client
import ssl
import urllib.error
import urllib.parse
import urllib.request


class CloudError(RuntimeError):
    def __init__(self, code: str, message: str, status: int = 0, details: dict | None = None):
        super().__init__(f"{code}: {message}")
        self.code, self.status = code, status
        self.message, self.details = message, details or {}

    def error(self):
        return dict(code=self.code, message=self.message, details=self.details)


def tool_outcome_error(receipt, tool_op_id=None):
    """Preserve the original tool identity and recovery facts for the Harness."""
    details = dict(receipt.get('details') or receipt.get('recovery_details') or {})
    result = receipt.get('result') or {}
    error = result.get('error') or {}
    if isinstance(error, dict):
        details = dict(error.get('details') or {}, **details)
    recovery = receipt.get('recovery') or details.get('recovery')
    if recovery:
        details['recovery'] = dict(recovery, read_only_first=True, automatic_write_replay=False)
    operation = receipt.get('tool_op_id') or tool_op_id
    if operation:
        details.setdefault('tool_op_id', operation)
    unknown = recovery.get('unknown_operations') if recovery else None
    details.setdefault('unknown_operations', unknown or [dict(tool_op_id=operation,
        status='OUTCOME_UNKNOWN', receipt_seq=receipt.get('receipt_seq'), automatic_replay=False)])
    details.setdefault('recovery', dict(read_only_first=True, automatic_write_replay=False,
        allowed_tools=['file_read'], steps=['保留原工具操作 ID，先用 file_read 检查工作区和相关文件，再依据持久回执对账。']))
    return CloudError('OUTCOME_UNKNOWN', '工具结果未知，先检查工作区并完成对账', 409, details)


class _ServerNameHTTPSConnection(http.client.HTTPSConnection):
    def __init__(self, host, *, server_name=None, **kwargs):
        self.server_name = server_name
        super().__init__(host, **kwargs)

    def connect(self):
        http.client.HTTPConnection.connect(self)
        self.sock = self._context.wrap_socket(self.sock,
            server_hostname=self.server_name or self._tunnel_host or self.host)


class _ServerNameHTTPSHandler(urllib.request.HTTPSHandler):
    def __init__(self, context, server_name):
        super().__init__(context=context)
        self.server_name = server_name

    def https_open(self, request):
        def connection(host, **kwargs):
            return _ServerNameHTTPSConnection(host, server_name=self.server_name, **kwargs)
        return self.do_open(connection, request, context=self._context)


class _NoRedirectHandler(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, request, response, code, message, headers, new_url):
        return None


class Client:
    def __init__(self, base_url: str, token: str, timeout: float = 30, headers: dict | None = None,
                 *, ca_file: str | None = None, cert_file: str | None = None,
                 key_file: str | None = None, server_name: str | None = None):
        self.base_url, self.token, self.timeout = base_url.rstrip('/'), token, timeout
        self.headers=headers or {}
        hostname=urllib.parse.urlparse(base_url).hostname
        handlers=[_NoRedirectHandler()]
        if hostname in ('127.0.0.1','localhost','::1'):
            handlers.append(urllib.request.ProxyHandler({}))
        if ca_file or cert_file or key_file or server_name:
            if key_file and not cert_file:
                raise ValueError('TLS私钥需要同时提供客户端证书')
            context=ssl.create_default_context(cafile=ca_file)
            if cert_file:
                context.load_cert_chain(cert_file, key_file)
            handlers.append(_ServerNameHTTPSHandler(context,server_name))
        self.opener=urllib.request.build_opener(*handlers)

    def call(self, method: str, path: str, body: dict | None = None) -> dict:
        data = None if body is None else json.dumps(body).encode()
        request = urllib.request.Request(self.base_url + path, data=data, method=method,
            headers={**self.headers, 'Authorization': 'Bearer ' + self.token, 'Content-Type': 'application/json'})
        try:
            with self.opener.open(request, timeout=self.timeout) as response:
                return json.load(response)
        except urllib.error.HTTPError as exc:
            try:
                error = json.load(exc)['error']
            except (ValueError, KeyError):
                error = {'code': 'HTTP_ERROR', 'message': str(exc)}
            raise CloudError(error['code'], error['message'], exc.code, error.get('details')) from exc

    @staticmethod
    def path(session_id: str) -> str:
        return '/v1/sessions/' + urllib.parse.quote(session_id, safe='')

    def create(self, session_id: str, workspace_id: str, mode: str, profile: str = 'coding') -> dict:
        return self.call('POST', '/v1/sessions', dict(session_id=session_id, workspace_id=workspace_id, mode=mode, profile=profile))

    def session(self, session_id: str) -> dict:
        return self.call('GET', self.path(session_id))

    def session_status(self, session_id: str) -> dict:
        return self.call('GET', self.path(session_id) + '/status')

    def tool(self, session_id: str, epoch: int, tool_op_id: str, kind: str, **kwargs) -> dict:
        return self.call('POST', self.path(session_id) + '/tools', dict(epoch=epoch, tool_op_id=tool_op_id, kind=kind, **kwargs))

    def tool_receipt(self, session_id: str, tool_op_id: str) -> dict:
        return self.call('GET', self.path(session_id) + '/tools/' + urllib.parse.quote(tool_op_id, safe=''))

    def lifecycle(self, session_id: str, action: str, epoch: int, operation_id: str) -> dict:
        return self.call('POST', self.path(session_id) + '/' + action, dict(expected_epoch=epoch, operation_id=operation_id))

    def request(self, session_id: str, logical_request_id: str, payload: dict) -> dict:
        return self.call('POST', self.path(session_id) + '/requests', dict(logical_request_id=logical_request_id, payload=payload))

    def get_request(self, session_id: str, logical_request_id: str) -> dict:
        return self.call('GET', self.path(session_id) + '/requests/' + urllib.parse.quote(logical_request_id, safe=''))

    def claim(self, session_id: str, logical_request_id: str, epoch: int) -> dict:
        return self.call('POST', self.path(session_id) + '/requests/' + urllib.parse.quote(logical_request_id, safe='') + '/claim', dict(epoch=epoch))
