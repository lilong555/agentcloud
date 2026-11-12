"""Real local ZeroMQ transport contracts; run in the locked Dynamo environment."""
import time
import unittest

import msgspec
import zmq

from experiments.dynamo.kv_events import ports
from experiments.dynamo.lifecycle_kv_mocker import Observer


class ObserverTransportTests(unittest.TestCase):
    def wait(self, predicate):
        deadline = time.monotonic() + 5
        while not predicate():
            if time.monotonic() >= deadline:
                self.fail('transport observation deadline')
            time.sleep(.005)

    def publish(self, publisher, sequence=0):
        batch = [time.time(), [{'type': 'BlockStored', 'block_hashes': [42]}], 0]
        publisher.send_multipart([b'', sequence.to_bytes(8, 'big'), msgspec.msgpack.encode(batch)])

    def test_late_publisher_handshake_precedes_first_event_across_twenty_connections(self):
        for iteration in range(20):
            with self.subTest(iteration=iteration), zmq.Context() as context, context.socket(zmq.PUB) as publisher:
                publisher.setsockopt(zmq.LINGER, 0)
                endpoint = f'tcp://127.0.0.1:{ports(1)[0]}'
                observer = Observer(endpoint)
                observer.thread.start()
                try:
                    self.assertTrue(observer.ready.wait(5))
                    self.assertFalse(observer.connected.is_set())
                    publisher.bind(endpoint)
                    self.assertTrue(observer.connected.wait(5))
                    self.publish(publisher)
                    self.wait(lambda: bool(observer.snapshot()['batches']))
                    result = observer.snapshot()
                    self.assertEqual(result['first_sequence'], 0)
                    self.assertEqual(result['counts'], {'BlockStored': 1})
                    self.assertEqual(result['errors'], [])
                    self.assertEqual(result['sequence_gaps'], [])
                    self.assertEqual(result['transport_events'][0]['event'], zmq.EVENT_HANDSHAKE_SUCCEEDED)
                finally:
                    observer.close()
                    self.assertFalse(observer.thread.is_alive())

    def test_missing_initial_sequence_is_visible_and_disconnect_clears_connection(self):
        with zmq.Context() as context, context.socket(zmq.PUB) as publisher:
            publisher.setsockopt(zmq.LINGER, 0)
            endpoint = f'tcp://127.0.0.1:{ports(1)[0]}'
            observer = Observer(endpoint)
            observer.thread.start()
            try:
                self.assertTrue(observer.ready.wait(5))
                publisher.bind(endpoint)
                self.assertTrue(observer.connected.wait(5))
                self.publish(publisher, sequence=2)
                self.wait(lambda: bool(observer.snapshot()['batches']))
                self.assertEqual(observer.snapshot()['sequence_gaps'], [(-1, 2)])
                publisher.close()
                self.wait(lambda: not observer.connected.is_set())
                self.assertEqual(observer.snapshot()['transport_events'][-1]['event'], zmq.EVENT_DISCONNECTED)
            finally:
                observer.close()


if __name__ == '__main__':
    unittest.main()
