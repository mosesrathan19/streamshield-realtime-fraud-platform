"""Pure Python tests; don't require PostgreSQL or PySpark runtime."""
import unittest
from datetime import datetime, timezone
from streamshield.postgres_load import COLUMNS, normalize_row


class MappingTests(unittest.TestCase):
    def sample(self):
        return dict(
            event_id="e1", transaction_id="t1", account_id="a1",
            merchant_id="m1", merchant_category="travel", amount_cents=15000,
            currency="USD", country="US", event_ts=datetime(2025, 12, 31, 16),
            event_ts_epoch_us=1767225600000000,
            risk_high_amount=False, risk_synthetic_country=False, risk_flag=False,
        )

    def test_column_order(self):
        values = normalize_row(self.sample())
        self.assertEqual(len(values), len(COLUMNS))
        self.assertEqual(values[0], "e1")
        self.assertEqual(values[8].tzinfo, timezone.utc)
        self.assertEqual(values[8], datetime(2026, 1, 1, tzinfo=timezone.utc))

    def test_missing_epoch_rejected(self):
        item = self.sample()
        del item["event_ts_epoch_us"]
        with self.assertRaises(ValueError):
            normalize_row(item)

    def test_missing_field_rejected(self):
        item = self.sample()
        del item["account_id"]
        with self.assertRaises(ValueError):
            normalize_row(item)

    def test_bad_amount_rejected(self):
        item = self.sample()
        item["amount_cents"] = -5
        with self.assertRaises(ValueError):
            normalize_row(item)

    def test_inconsistent_risk_flags_rejected(self):
        item = self.sample()
        item["risk_flag"] = True
        with self.assertRaises(ValueError):
            normalize_row(item)


if __name__ == "__main__":
    unittest.main()
