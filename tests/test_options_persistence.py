import tempfile
import unittest
from pathlib import Path

import duckdb
import pandas as pd

from reconciliation_engine.reconciliation.options import reconcile_option_sources
from reconciliation_engine.storage.duckdb import initialize_database, persist_option_run


def _chain(close: float) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "underlying": "AAPL",
                "expiry": "2026-12-18",
                "strike": 200.0,
                "option_type": "C",
                "contract_symbol": "AAPL261218C00200000",
                "trade_date": "2026-10-13",
                "close": close,
                "bid": close - 0.1,
                "ask": close + 0.1,
                "volume": 10.0,
                "open_interest": 20.0,
            }
        ]
    )


class OptionPersistenceTests(unittest.TestCase):
    def test_option_chain_is_atomic_and_idempotent(self):
        with tempfile.TemporaryDirectory() as directory:
            database = str(Path(directory) / "options.duckdb")
            initialize_database(database)
            yahoo, alpaca = _chain(5.0), _chain(5.2)
            result = reconcile_option_sources({"YAHOO": yahoo, "ALPACA": alpaca})
            arguments = {
                "run_id": "option-run",
                "underlying": "AAPL",
                "expiry": "2026-12-18",
                "started_at": "2026-10-13T00:00:00Z",
                "finished_at": "2026-10-13T00:00:01Z",
                "status": "SUCCESS_WITH_BREAKS",
                "yahoo_options": yahoo,
                "alpaca_options": alpaca,
                "reconciliation": result,
                "db_path": database,
            }
            self.assertTrue(persist_option_run(**arguments))
            self.assertFalse(persist_option_run(**arguments))
            with duckdb.connect(database, read_only=True) as connection:
                self.assertEqual(
                    connection.execute("SELECT count(*) FROM option_runs").fetchone()[
                        0
                    ],
                    1,
                )
                self.assertEqual(
                    connection.execute(
                        "SELECT count(*) FROM option_contracts_staging"
                    ).fetchone()[0],
                    2,
                )
                self.assertEqual(
                    connection.execute(
                        "SELECT status FROM option_reconciliation_results"
                    ).fetchone()[0],
                    "BREAK",
                )


if __name__ == "__main__":
    unittest.main()
