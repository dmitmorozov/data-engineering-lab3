"""Opt-in REAL ZooKeeper tests. Never start/stop containers, install or touch Git.

Run only after the user has started the ensemble:
  LAB4_TEST_ZK=127.0.0.1:2181,... python -m unittest discover ...
Test znodes are left under uniquely named namespaces for auditability.
"""
import json
import os
import sys
import unittest
import uuid
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]/'src'))


@unittest.skipUnless(os.getenv('LAB4_TEST_ZK'), 'Requires user-started real ZooKeeper ensemble')
class ZooKeeperIntegrationTests(unittest.TestCase):
    def setUp(self):
        from zk_protocol import ZKProtocol, connect
        self.client = connect(os.environ['LAB4_TEST_ZK'])
        self.protocol = ZKProtocol(self.client, 'test-'+uuid.uuid4().hex)

    def tearDown(self):
        self.client.stop()
        self.client.close()

    def test_fencing_rejects_old_epoch_atomically_and_preserves_backup_pointer(self):
        from kazoo.exceptions import BadVersionError
        from core import encoded
        protocol = self.protocol
        protocol.initialize('fenced')
        old = protocol.claim('A')
        # Advance epoch as a new holder would. This test exercises the storage gate,
        # not session expiration; expiration is demonstrated separately in the 40s experiment.
        _, stat = protocol.read('epoch')
        new_stat = self.client.set(protocol.base+'/epoch', encoded({'epoch': 2}), version=stat.version)
        new = {'epoch': 2, 'epoch_version': new_stat.version, 'coordinator_id': 'B'}
        protocol.publish({'coordinator_id': 'B'}, new, 'fenced')
        with self.assertRaises(BadVersionError):
            protocol.publish({'coordinator_id': 'A'}, old, 'fenced')
        value, stat = protocol.read('result')
        self.assertEqual(value, {'coordinator_id': 'B'})
        self.assertEqual(stat.version, 1)

    def test_unsafe_two_writes_reach_version_two(self):
        protocol = self.protocol
        protocol.initialize('unsafe')
        protocol.publish({'coordinator_id': 'B'}, {}, 'unsafe')
        protocol.publish({'coordinator_id': 'A'}, {}, 'unsafe')
        value, stat = protocol.read('result')
        self.assertEqual(value['coordinator_id'], 'A')
        self.assertEqual(stat.version, 2)


if __name__ == '__main__':
    unittest.main()
