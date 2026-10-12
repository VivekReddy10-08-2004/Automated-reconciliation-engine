from datetime import date
from pathlib import Path
import tempfile
import unittest

from reconciliation_engine.storage.retention import (
    apply_partition_retention,
    plan_partition_retention,
)


class RetentionTests(unittest.TestCase):
    def test_plan_is_date_bounded_and_dry_run_is_non_destructive(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            old = root / "partitions/raw/control_date=2026-01-01"
            current = root / "partitions/raw/control_date=2026-10-01"
            old.mkdir(parents=True)
            current.mkdir(parents=True)
            (old / "payload.parquet").write_bytes(b"old")
            (current / "payload.parquet").write_bytes(b"current")

            plan = plan_partition_retention(root, before=date(2026, 6, 1))
            selected = apply_partition_retention(plan, data_root=root)

            self.assertEqual(len(plan), 1)
            self.assertEqual(plan[0].bytes_on_disk, 3)
            self.assertEqual(selected, (old.resolve(),))
            self.assertTrue(old.exists())
            self.assertTrue(current.exists())

    def test_apply_removes_only_planned_control_date_directory(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            old = root / "partitions/normalized/control_date=2025-01-01"
            old.mkdir(parents=True)
            (old / "data.parquet").write_bytes(b"old")
            plan = plan_partition_retention(root, before=date(2026, 1, 1))

            apply_partition_retention(plan, data_root=root, dry_run=False)

            self.assertFalse(old.exists())


if __name__ == "__main__":
    unittest.main()
