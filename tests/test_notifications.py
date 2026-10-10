import unittest
from unittest.mock import Mock, patch

import requests

from notifications import send_run_notification


class NotificationTests(unittest.TestCase):
    @patch("notifications.requests.post")
    @patch("notifications.os.getenv", return_value=None)
    @patch("notifications.load_dotenv")
    @patch("builtins.print")
    def test_clean_run_produces_no_notification(
        self,
        print_mock,
        load_dotenv,
        getenv,
        post,
    ):
        send_run_notification(
            {"run_id": "run-clean", "status": "SUCCESS", "break_count": 0}
        )

        load_dotenv.assert_called_once_with("project.env")
        print_mock.assert_not_called()
        post.assert_not_called()

    @patch("notifications.requests.post")
    @patch("notifications.os.getenv", return_value=None)
    @patch("notifications.load_dotenv")
    @patch("builtins.print")
    def test_break_run_without_webhook_prints_one_summary(
        self,
        print_mock,
        load_dotenv,
        getenv,
        post,
    ):
        send_run_notification(
            {
                "run_id": "run-break",
                "status": "SUCCESS_WITH_BREAKS",
                "break_count": 3,
            }
        )

        print_mock.assert_called_once_with(
            "Run ID: run-break | Status: SUCCESS_WITH_BREAKS | Break count: 3"
        )
        post.assert_not_called()

    @patch("notifications.requests.post")
    @patch("notifications.os.getenv", return_value="https://example.test/webhook-secret")
    @patch("notifications.load_dotenv")
    @patch("builtins.print")
    def test_failed_run_with_webhook_posts_one_summary(
        self,
        print_mock,
        load_dotenv,
        getenv,
        post,
    ):
        response = Mock()
        post.return_value = response

        send_run_notification(
            {"run_id": "run-failed", "status": "FAILED", "break_count": 0}
        )

        post.assert_called_once_with(
            "https://example.test/webhook-secret",
            json={
                "text": "Run ID: run-failed | Status: FAILED | Break count: 0"
            },
            timeout=10,
        )
        response.raise_for_status.assert_called_once_with()
        print_mock.assert_not_called()

    @patch("notifications.requests.post")
    @patch("notifications.os.getenv", return_value="https://example.test/webhook-secret")
    @patch("notifications.load_dotenv")
    def test_webhook_failure_does_not_expose_webhook_url(
        self,
        load_dotenv,
        getenv,
        post,
    ):
        post.side_effect = requests.RequestException(
            "request failed for https://example.test/webhook-secret"
        )

        with self.assertRaisesRegex(
            RuntimeError,
            "^Run notification delivery failed$",
        ) as raised:
            send_run_notification(
                {"run_id": "run-failed", "status": "FAILED", "break_count": 0}
            )

        self.assertNotIn("webhook-secret", str(raised.exception))


if __name__ == "__main__":
    unittest.main()
