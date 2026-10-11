"""Small, local security controls for credentials and persisted data."""

from __future__ import annotations

import os
import stat
from pathlib import Path

from dotenv import load_dotenv


PRIVATE_FILE_MODE = 0o600


def restrict_file_permissions(path: str | Path) -> None:
    """Limit a local secret or database file to its owner on POSIX systems."""
    file_path = Path(path)
    if os.name == "posix" and file_path.exists():
        file_path.chmod(PRIVATE_FILE_MODE)


def validate_private_file(path: str | Path) -> Path:
    """Reject secret files readable or writable by group/other users."""
    file_path = Path(path)
    if not file_path.is_file():
        raise RuntimeError(f"Required secret file does not exist: {file_path}")

    if os.name == "posix":
        mode = stat.S_IMODE(file_path.stat().st_mode)
        if mode & (stat.S_IRWXG | stat.S_IRWXO):
            raise PermissionError(
                f"Secret file {file_path} has permissions {mode:o}; "
                f"set them to {PRIVATE_FILE_MODE:o}"
            )
    return file_path


def load_alpaca_credentials(env_path: str | Path = "project.env") -> tuple[str, str]:
    """Load credentials from runtime environment or a private local env file."""
    api_key = os.getenv("ALPACA_API_KEY") or os.getenv("APCA_API_KEY_ID")
    secret_key = os.getenv("ALPACA_SECRET_KEY") or os.getenv("APCA_API_SECRET_KEY")
    if api_key and secret_key:
        return api_key, secret_key

    secret_file = validate_private_file(env_path)
    load_dotenv(secret_file, override=False)
    api_key = os.getenv("ALPACA_API_KEY") or os.getenv("APCA_API_KEY_ID")
    secret_key = os.getenv("ALPACA_SECRET_KEY") or os.getenv("APCA_API_SECRET_KEY")
    if not api_key or not secret_key:
        raise RuntimeError("Alpaca API credentials are not configured")
    return api_key, secret_key
