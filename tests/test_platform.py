import json
import sqlite3
import tempfile
import unittest
from pathlib import Path
from streamshield.events import event_at, events, write_jsonl
from streamshield.processor import process_jsonl
from streamshield.rules import validate, evaluate


class PlatformTests(unittest.TestCase):
    def test_deterministic_events(self):
        self.assertEqual(event_at(33), event_at(33))
        self.assertEqual(list(events(10, start=5)), [event_at(i) for i in range(5, 15)])

    def test_unique_event_ids(self):
        generated = list(events(500))
        self.assertEqual(len({r['event_id'] for r in generated}), 500)

    def test_amount_validation(self):
        event = event_at(1)
        event['amount_cents'] = -1
        with self.assertRaises(ValueError):
            validate(event)

    def test_high_amount_signal(self):
        self.assertIn('HIGH_AMOUNT', evaluate(event_at(0)))

    def test_synthetic_country_signal(self):
        self.assertIn('SYNTHETIC_COUNTRY_ANOMALY', evaluate(event_at(137)))

    def test_replay_is_idempotent(self):
        with tempfile.TemporaryDirectory() as tmp:
            source, db = Path(tmp) / 'events.jsonl', Path(tmp) / 'events.db'
            write_jsonl(source, 200)
            first = process_jsonl(source, db)
            second = process_jsonl(source, db)
            self.assertEqual(first['inserted'], 200)
            self.assertEqual(second['inserted'], 0)
            self.assertEqual(second['duplicate'], 200)
            with sqlite3.connect(db) as con:
                self.assertEqual(con.execute('SELECT COUNT(*) FROM processed_events').fetchone()[0], 200)
                self.assertEqual(con.execute('SELECT COUNT(*) FROM risk_alerts').fetchone()[0], first['alerts'])

    def test_quarantine_malformed_line(self):
        with tempfile.TemporaryDirectory() as tmp:
            source, db = Path(tmp) / 'events.jsonl', Path(tmp) / 'events.db'
            source.write_text(json.dumps(event_at(1)) + '\nnot json\n', encoding='utf-8')
            report = process_jsonl(source, db)
            self.assertEqual(report['rejected'], 1)
            self.assertEqual(report['inserted'], 1)

    def test_schema_version_rejected(self):
        item = event_at(1)
        item['schema_version'] = 2
        with self.assertRaises(ValueError):
            validate(item)


if __name__ == '__main__':
    unittest.main()
