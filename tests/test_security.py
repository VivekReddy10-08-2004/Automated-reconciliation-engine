import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from reconciliation_engine.security import (
    load_alpaca_credentials,
    load_massive_api_key,
    restrict_file_permissions,
    validate_private_file,
)


@unittest.skipUnless(os.name == "posix", "POSIX permission test")
class SecurityTests(unittest.TestCase):
    @patch.dict(os.environ, {"MASSIVE_API_KEY": "runtime-massive-key"}, clear=True)
    def test_massive_key_can_come_from_runtime_environment(self):
        self.assertEqual(
            load_massive_api_key("missing-project.env"), "runtime-massive-key"
        )

    def test_runtime_environment_credentials_do_not_require_a_secret_file(self):
        credentials = {
            "ALPACA_API_KEY": "runtime-api-key",
            "ALPACA_SECRET_KEY": "runtime-secret-key",
        }
        with patch.dict(os.environ, credentials, clear=True), patch(
            "reconciliation_engine.security.validate_private_file"
        ) as validate:
            self.assertEqual(
                load_alpaca_credentials("missing-project.env"),
                ("runtime-api-key", "runtime-secret-key"),
            )
        validate.assert_not_called()

    def test_rejects_group_readable_secret_and_can_harden_it(self):
        with tempfile.TemporaryDirectory() as directory:
            secret = Path(directory) / "project.env"
            secret.write_text("API_KEY=example\n", encoding="utf-8")
            secret.chmod(0o640)

            with self.assertRaises(PermissionError):
                validate_private_file(secret)

            restrict_file_permissions(secret)

            self.assertEqual(secret.stat().st_mode & 0o777, 0o600)
            self.assertEqual(validate_private_file(secret), secret)


if __name__ == "__main__":
    unittest.main()
