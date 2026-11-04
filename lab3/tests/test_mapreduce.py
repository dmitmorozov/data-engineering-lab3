import json
import sys
import unittest
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from mapreduce import calculate

def source_bytes(rows):
    return b"\n".join(json.dumps(row, ensure_ascii=False).encode("utf-8") for row in rows)

class MapReduceTests(unittest.TestCase):
    def test_multibyte_cap_duplicates_empty_and_variant(self):
        rows = [
            {"source_segment": "analysts", "vacancy": {"key_skills": [" SQL ", "Python", "SQL", "Анализ"]}},
            {"source_segment": "analysts", "vacancy": {"key_skills": ["Python", "SQL"]}},
            {"source_segment": "analysts", "vacancy": {"key_skills": []}},
            {"source_segment": "other", "vacancy": {"key_skills": ["WRONG"]}},
        ]
        raw = source_bytes(rows)
        chunks = (raw[i:i+1] for i in range(len(raw)))  # Splits every UTF-8 code point.
        counts, stats = calculate(chunks, 2, workers=1, batch_records=1, max_inflight=2)
        self.assertEqual(counts, {("SQL",): 2, ("Python",): 2, ("Python", "SQL"): 2})
        self.assertEqual(stats["records"], 3)
        self.assertEqual(stats["read_bytes"], len(raw))

    def test_hard_mapper_exit_replay_and_combiner(self):
        rows = [{"source_segment": "analysts", "vacancy": {"key_skills": ["C", "A", "B"]}}] * 5
        raw = source_bytes(rows)
        baseline, _ = calculate([raw], None, workers=2, batch_records=1, max_inflight=2)
        events = []
        recovered, stats = calculate([raw], None, workers=2, batch_records=1,
                                     max_inflight=2, fail_chunk=1, event=events.append)
        combined, combined_stats = calculate([raw], None, combine=True, workers=2,
                                             batch_records=5, max_inflight=1)
        self.assertEqual(baseline, recovered)
        self.assertEqual(baseline, combined)
        self.assertEqual(baseline[("A", "B", "C")], 5)
        self.assertEqual(stats["records"], 5)
        self.assertEqual(stats["recoveries"], 1)
        self.assertEqual(combined_stats["shuffle_records"], 7)
        self.assertTrue(any(e["event"] == "mapper_process_failure" for e in events))

if __name__ == "__main__":
    unittest.main()
