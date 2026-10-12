"""Authenticated read-only REST API for reconciliation consumers."""

from reconciliation_engine.api.service import create_app

__all__ = ["create_app"]
