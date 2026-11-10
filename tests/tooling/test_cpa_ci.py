import base64
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import stat
import tempfile
import unittest

from scripts.cpa_ssh_endpoint import Rejected, REQUEST_LIMIT, validate
from scripts.prepare_cpa_ci import prepare


class CPARestrictionTests(unittest.TestCase):
    def setUp(self):
        self.config = dict(client_key='fixture-client-key', model='gpt-6-luna', api_key='private-provider-key')

    def request(self, **payload):
        return dict(method='POST', path='/v1/chat/completions', key=self.config['client_key'],
            body=base64.b64encode(json.dumps(dict(model='gpt-6-luna', stream=False, **payload)).encode()).decode())

    def test_fixed_model_and_catalog_are_allowed(self):
        self.assertEqual(validate(self.request(messages=[]), self.config)[:2], ('POST', '/v1/chat/completions'))
        self.assertEqual(validate(dict(method='GET', path='/v1/models', body='', key='fixture-client-key'), self.config),
                         ('GET', '/v1/models', b''))

    def test_other_origins_methods_models_and_credentials_are_rejected(self):
        requests = [dict(self.request(), path='http://example.com/private'),
                    dict(self.request(), method='DELETE'), dict(self.request(), key='wrong'),
                    dict(self.request(), body=base64.b64encode(b'{"model":"another"}').decode())]
        for request in requests:
            with self.subTest(request=request['path']), self.assertRaises(Rejected):
                validate(request, self.config)

    def test_request_size_is_bounded_before_provider_access(self):
        request = dict(self.request(), body=base64.b64encode(b'x' * (REQUEST_LIMIT + 1)).decode())
        with self.assertRaises(Rejected) as error:
            validate(request, self.config)
        self.assertEqual(error.exception.status, 413)

    def test_runner_materialization_is_private_pinned_and_exclusive(self):
        config = dict(host='192.0.2.1', user='runner', port=22, client_key='x' * 32,
            model='gpt-6-luna', private_key='-----BEGIN OPENSSH PRIVATE KEY-----\nfixture\n',
            known_hosts='192.0.2.1 ssh-ed25519 fixture',
            expires_at=(datetime.now(timezone.utc) + timedelta(days=1)).isoformat())
        with tempfile.TemporaryDirectory() as root:
            directory = Path(root) / 'private'
            result = prepare(json.dumps(config), directory)
            self.assertEqual(result['private_files'], 4)
            self.assertEqual(stat.S_IMODE(directory.stat().st_mode), 0o700)
            for path in directory.iterdir():
                self.assertEqual(stat.S_IMODE(path.stat().st_mode), 0o600)
            ssh = (directory / 'ssh_config').read_text()
            self.assertIn('StrictHostKeyChecking yes', ssh)
            self.assertIn('IdentitiesOnly yes', ssh)
            model = json.loads((directory / 'model.json').read_text())
            self.assertEqual(model['api_key'], config['client_key'])
            self.assertNotIn('private-provider-key', (directory / 'model.json').read_text())
            with self.assertRaises(FileExistsError):
                prepare(json.dumps(config), directory)
            config['expires_at'] = '2000-01-01T00:00:00Z'
            with self.assertRaisesRegex(ValueError, 'expired'):
                prepare(json.dumps(config), Path(root) / 'expired')


if __name__ == '__main__':
    unittest.main()
