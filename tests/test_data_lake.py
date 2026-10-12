import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import pandas as pd
from pandas.testing import assert_frame_equal

from reconciliation_engine.storage.data_lake import (
    EQUITY_ARCHIVE_COLUMNS,
    EQUITY_COLUMNS,
    EQUITY_INSTRUMENT,
    archive_equity_source,
    archive_reconciliation_results,
    read_dataframe,
    read_reconciliation_results,
    write_dataframe,
)
from reconciliation_engine.batch import main, replay_archived_run


def equity_frame(close: float) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "symbol": "AAPL",
                "trade_date": "2026-10-09",
                "open": 100.0,
                "high": 102.0,
                "low": 99.0,
                "close": close,
                "volume": 1_000,
            }
        ]
    )


class DataLakeTests(unittest.TestCase):
    def test_retry_reuses_the_completed_logical_run(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "data"
            database = Path(directory) / "reconciliation.duckdb"
            sources = (equity_frame(101.0), equity_frame(100.0))

            with patch(
                "reconciliation_engine.batch.fetch_equity_sources",
                return_value=sources,
            ) as fetch_sources, patch(
                "reconciliation_engine.batch.send_run_notification"
            ), patch(
                "builtins.print"
            ):
                first = main(str(database), root, control_date="2026-10-09")
                second = main(str(database), root, control_date="2026-10-09")

            self.assertEqual(first["run_id"], second["run_id"])
            self.assertFalse(first["already_completed"])
            self.assertTrue(second["already_completed"])
            fetch_sources.assert_called_once()
            run_id = first["run_id"]
            self.assertTrue(
                (root / f"raw/YAHOO/equities/run_id={run_id}/payload.parquet").is_file()
            )
            self.assertTrue(
                (root / f"raw/ALPACA/equities/run_id={run_id}/payload.parquet").is_file()
            )
            self.assertTrue(
                (root / f"normalized/YAHOO/equities/run_id={run_id}/data.parquet").is_file()
            )
            self.assertTrue(
                (root / f"normalized/ALPACA/equities/run_id={run_id}/data.parquet").is_file()
            )
            self.assertTrue(
                (root / f"reconciliation/equities/run_id={run_id}/results.parquet").is_file()
            )

    def test_writes_partitioned_file_and_reads_it_without_an_api(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "data"
            source = equity_frame(101.0)

            path = write_dataframe(
                source,
                "YAHOO",
                EQUITY_INSTRUMENT,
                "run-1",
                layer="raw",
                data_root=root,
            )
            restored = read_dataframe(
                "YAHOO",
                EQUITY_INSTRUMENT,
                "run-1",
                layer="raw",
                data_root=root,
            )

            expected_path = (
                root
                / "raw"
                / "YAHOO"
                / "equities"
                / "run_id=run-1"
                / "payload.parquet"
            )
            self.assertEqual(path, expected_path)
            self.assertTrue(path.is_file())
            self.assertEqual(restored.loc[0, "run_id"], "run-1")
            self.assertEqual(restored.loc[0, "symbol"], "AAPL")

    def test_refuses_to_overwrite_an_existing_run(self):
        with tempfile.TemporaryDirectory() as directory:
            write_dataframe(
                equity_frame(101.0),
                "YAHOO",
                EQUITY_INSTRUMENT,
                "same-run",
                layer="normalized",
                data_root=directory,
            )

            with self.assertRaises(FileExistsError):
                write_dataframe(
                    equity_frame(999.0),
                    "YAHOO",
                    EQUITY_INSTRUMENT,
                    "same-run",
                    layer="normalized",
                    data_root=directory,
                )

    def test_missing_file_returns_empty_equity_schema(self):
        with tempfile.TemporaryDirectory() as directory:
            restored = read_dataframe(
                "ALPACA",
                EQUITY_INSTRUMENT,
                "missing-run",
                layer="normalized",
                data_root=directory,
            )

            self.assertTrue(restored.empty)
            self.assertEqual(list(restored.columns), EQUITY_ARCHIVE_COLUMNS)

    def test_archives_both_layers_and_replays_reconciliation(self):
        with tempfile.TemporaryDirectory() as directory:
            yahoo = archive_equity_source(
                equity_frame(101.0),
                "YAHOO",
                "run-2",
                data_root=directory,
            )
            alpaca = archive_equity_source(
                equity_frame(100.0),
                "ALPACA",
                "run-2",
                data_root=directory,
            )

            self.assertTrue(
                (
                    Path(directory)
                    / "raw/YAHOO/equities/run_id=run-2/payload.parquet"
                ).is_file()
            )
            self.assertTrue(
                (
                    Path(directory)
                    / "normalized/ALPACA/equities/run_id=run-2/data.parquet"
                ).is_file()
            )

            replayed = replay_archived_run("run-2", directory)
            archive_reconciliation_results(replayed, "run-2", data_root=directory)
            restored_results = read_reconciliation_results("run-2", data_root=directory)

            self.assertEqual(list(yahoo.columns), EQUITY_COLUMNS)
            self.assertEqual(list(alpaca.columns), EQUITY_COLUMNS)
            self.assertEqual(replayed.loc[0, "status"], "BREAK")
            self.assertEqual(restored_results.loc[0, "run_id"], "run-2")
            assert_frame_equal(
                replayed.reset_index(drop=True),
                restored_results.drop(columns="run_id").reset_index(drop=True),
                check_dtype=False,
            )

    def test_interrupted_retry_reuses_immutable_provider_archive(self):
        with tempfile.TemporaryDirectory() as directory:
            first = archive_equity_source(
                equity_frame(101.0),
                "YAHOO",
                "stable-run",
                data_root=directory,
                idempotent=True,
            )
            retry = archive_equity_source(
                equity_frame(999.0),
                "YAHOO",
                "stable-run",
                data_root=directory,
                idempotent=True,
            )

            self.assertEqual(first.iloc[0]["close"], 101.0)
            self.assertEqual(retry.iloc[0]["close"], 101.0)


if __name__ == "__main__":
    unittest.main()
