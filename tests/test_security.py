import os
import tempfile
import unittest
from pathlib import Path

from security import restrict_file_permissions, validate_private_file


@unittest.skipUnless(os.name == "posix", "POSIX permission test")
class SecurityTests(unittest.TestCase):
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
