import json
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]/'src'))
from core import (ObjectStore, apply_window, checkpoint_loaded_event,
                  empty_state, make_signature, map_batch)


def output(number, skills):
    rows = [{'source_segment': 'analysts', 'vacancy': {'key_skills': skills}}]
    return map_batch((number, rows, None, True, False))


class RecoveryTests(unittest.TestCase):
    def setUp(self):
        self.signature = make_signature('a'*64, None, 128, 4)
        self.temp_root = Path(__file__).resolve().parents[1]/'.runtime/test-tmp'
        self.temp_root.mkdir(parents=True, exist_ok=True)

    def test_resume_only_uncommitted_rows_with_utf8_and_duplicates(self):
        rows = [output(0, [' SQL ', 'Python', 'SQL', 'Анализ']),
                output(1, []), output(2, ['SQL', 'Python'])]
        uninterrupted = empty_state(self.signature)
        apply_window(uninterrupted, rows)
        interrupted = empty_state(self.signature)
        apply_window(interrupted, rows[:2])
        with tempfile.TemporaryDirectory(dir=self.temp_root) as directory:
            store = ObjectStore(directory)
            pointer = store.put_snapshot('test', interrupted)
            restored = store.load_snapshot(pointer, self.signature)
            self.assertEqual(restored['next_batch'], 2)
            self.assertEqual(restored['records'], 2)
            apply_window(restored, rows[2:])
        self.assertEqual(uninterrupted, restored)
        self.assertEqual(restored['counts'][('Python', 'SQL')], 2)

    def test_duplicate_window_is_rejected_before_any_partial_apply(self):
        state = empty_state(self.signature)
        with self.assertRaises(ValueError):
            apply_window(state, [output(0, ['SQL']), output(0, ['Python'])])
        self.assertEqual(state['records'], 0)
        self.assertFalse(state['counts'])

    def test_corrupted_snapshot_and_parameter_mismatch_rejected(self):
        state = empty_state(self.signature)
        apply_window(state, [output(0, ['SQL'])])
        with tempfile.TemporaryDirectory(dir=self.temp_root) as directory:
            store = ObjectStore(directory)
            pointer = store.put_snapshot('test', state)
            other = make_signature('b'*64, None, 128, 4)
            with self.assertRaisesRegex(ValueError, 'parameters'):
                store.load_snapshot(pointer, other)
            store.path(pointer['key']).write_bytes(b'corrupt')
            with self.assertRaisesRegex(ValueError, 'SHA-256'):
                store.load_snapshot(pointer, self.signature)

    def test_snapshot_and_pointer_progress_must_agree(self):
        with tempfile.TemporaryDirectory(dir=self.temp_root) as directory:
            store = ObjectStore(directory)
            pointer = store.put_snapshot('test', empty_state(self.signature))
            pointer['next_batch'] = 1
            with self.assertRaisesRegex(ValueError, 'progress disagree'):
                store.load_snapshot(pointer, self.signature)

    def test_store_rejects_path_traversal(self):
        with tempfile.TemporaryDirectory(dir=self.temp_root) as directory:
            with self.assertRaises(ValueError):
                ObjectStore(directory).path('../outside')

    def test_checkpoint_author_cannot_replace_loading_coordinator(self):
        pointer = {'coordinator_id': 'A', 'epoch': 1, 'epoch_version': 1,
                   'sha256': 'a'*64, 'records': 512, 'next_batch': 4}
        event = {'coordinator_id': 'B', **checkpoint_loaded_event(pointer, {'epoch': 2})}
        self.assertEqual(event['coordinator_id'], 'B')
        self.assertEqual(event['epoch'], 2)
        self.assertEqual(event['checkpoint_coordinator_id'], 'A')
        self.assertEqual(event['checkpoint_epoch'], 1)
        self.assertEqual(event['sha256'], pointer['sha256'])
        self.assertEqual(event['next_batch'], 4)


if __name__ == '__main__':
    unittest.main()
