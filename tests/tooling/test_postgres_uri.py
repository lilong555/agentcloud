import unittest
from bench.postgres import connection


class PostgreSQLURITests(unittest.TestCase):
    def test_unix_socket_database_uri_preserves_authority_delimiters(self):
        self.assertEqual(connection('postgresql:///postgres?host=/var/run/postgresql','acceptance_1'),
            'postgresql:///acceptance_1?host=/var/run/postgresql')

    def test_tcp_credentials_and_options_survive_database_selection(self):
        self.assertEqual(connection('postgresql://ci:p%40ss@127.0.0.1:5432/admin?sslmode=disable','scale_1'),
            'postgresql://ci:p%40ss@127.0.0.1:5432/scale_1?sslmode=disable')
        with self.assertRaises(ValueError): connection('postgresql:///postgres','invalid;sql')
