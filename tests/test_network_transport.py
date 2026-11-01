from datetime import datetime, timedelta, timezone
import io
import json
from pathlib import Path
import socket
import struct
import tempfile
import threading
import unittest
from unittest.mock import Mock

import duckdb
from rich.console import Console

from reconciliation_engine.realtime.network_transport import (
    EventValidationError,
    EventTableOutput,
    FrameError,
    MarketEvent,
    MarketEventTransport,
    MulticastTransport,
    SequenceTracker,
    TCPTransport,
    build_capture_filter,
    build_tcpdump_command,
    monitor_transport,
)


def _payload(sequence_number: int = 10) -> dict[str, object]:
    exchange_time = datetime.now(timezone.utc) - timedelta(milliseconds=5)
    return {
        "source": "vendor-a",
        "sequence_number": sequence_number,
        "symbol": "aapl",
        "event_type": "trade",
        "price": 100.25,
        "quantity": 50,
        "exchange_timestamp": exchange_time.isoformat(),
    }


def _frame(sequence_number: int = 10) -> bytes:
    return json.dumps(_payload(sequence_number), separators=(",", ":")).encode()


class MarketEventContractTests(unittest.TestCase):
    def test_normalizes_event_and_builds_deterministic_identity(self):
        payload = _payload()
        received_at = datetime.now(timezone.utc)

        first = MarketEvent.from_mapping(
            payload, transport="tcp", received_at=received_at
        )
        second = MarketEvent.from_mapping(
            payload, transport="tcp", received_at=received_at
        )

        self.assertEqual(first.event_id, second.event_id)
        self.assertEqual(first.source, "VENDOR-A")
        self.assertEqual(first.stream_id, "VENDOR-A")
        self.assertEqual(first.symbol, "AAPL")
        self.assertEqual(first.event_type, "TRADE")
        self.assertEqual(first.transport, "TCP")
        self.assertGreaterEqual(first.latency_ms, 0)

    def test_rejects_missing_invalid_and_future_event_data(self):
        with self.assertRaisesRegex(EventValidationError, "missing fields"):
            MarketEvent.from_mapping({}, transport="TCP")

        invalid = _payload()
        invalid["sequence_number"] = -1
        with self.assertRaisesRegex(EventValidationError, "sequence_number"):
            MarketEvent.from_mapping(invalid, transport="TCP")

        future = _payload()
        future["exchange_timestamp"] = (
            datetime.now(timezone.utc) + timedelta(seconds=1)
        ).isoformat()
        with self.assertRaisesRegex(EventValidationError, "cannot precede"):
            MarketEvent.from_mapping(future, transport="TCP")


class SequenceTrackerTests(unittest.TestCase):
    def test_classifies_gap_duplicate_and_reordered_vendor_sequences(self):
        tracker = SequenceTracker()

        self.assertEqual(tracker.observe("vendor-a", "trades", 10).status, "FIRST")
        self.assertEqual(tracker.observe("vendor-a", "trades", 11).status, "IN_ORDER")
        gap = tracker.observe("vendor-a", "trades", 14)
        self.assertEqual(gap.status, "GAP")
        self.assertEqual(gap.expected_sequence, 12)
        self.assertEqual(gap.missing_count, 2)
        self.assertEqual(tracker.observe("vendor-a", "trades", 14).status, "DUPLICATE")
        self.assertEqual(tracker.observe("vendor-a", "trades", 13).status, "REORDERED")
        self.assertEqual(tracker.observe("vendor-b", "trades", 500).status, "FIRST")

    def test_tracks_same_stream_id_independently_for_each_source(self):
        tracker = SequenceTracker()

        self.assertEqual(tracker.observe("vendor-a", "trades", 100).status, "FIRST")
        self.assertEqual(tracker.observe("vendor-b", "trades", 1).status, "FIRST")
        self.assertEqual(tracker.observe("vendor-a", "trades", 101).status, "IN_ORDER")
        self.assertEqual(tracker.observe("vendor-b", "trades", 2).status, "IN_ORDER")

    def test_duplicate_classification_expires_at_bounded_window(self):
        tracker = SequenceTracker(duplicate_window=2)

        tracker.observe("vendor-a", "trades", 1)
        tracker.observe("vendor-a", "trades", 2)
        tracker.observe("vendor-a", "trades", 3)

        self.assertEqual(
            tracker.observe("vendor-a", "trades", 1).status,
            "REORDERED",
        )


class TCPTransportTests(unittest.TestCase):
    def _socket_transport(
        self, framing: str, payload: bytes
    ) -> tuple[TCPTransport, socket.socket]:
        client, server = socket.socketpair()
        server.sendall(payload)
        transport = TCPTransport(
            "127.0.0.1",
            9000,
            framing=framing,
            reconnect_attempts=0,
            socket_factory=lambda _address, _timeout: client,
        )
        return transport, server

    def test_receives_newline_delimited_json(self):
        transport, server = self._socket_transport("newline", _frame() + b"\n")
        try:
            event = transport.receive()
        finally:
            transport.close()
            server.close()

        self.assertEqual(event.sequence_number, 10)
        self.assertEqual(event.transport, "TCP")

    def test_receives_from_real_loopback_tcp_listener(self):
        listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        listener.bind(("127.0.0.1", 0))
        listener.listen(1)
        host, port = listener.getsockname()

        def send_frame() -> None:
            connection, _address = listener.accept()
            with connection:
                connection.sendall(_frame(16) + b"\n")
            listener.close()

        sender = threading.Thread(target=send_frame, daemon=True)
        sender.start()
        transport = TCPTransport(
            host,
            port,
            reconnect_attempts=0,
            connect_timeout=1,
            read_timeout=1,
        )
        try:
            event = transport.receive()
        finally:
            transport.close()
            listener.close()
            sender.join(timeout=1)

        self.assertFalse(sender.is_alive())
        self.assertEqual(event.sequence_number, 16)

    def test_preserves_buffered_second_newline_frame(self):
        transport, server = self._socket_transport(
            "newline", _frame(10) + b"\n" + _frame(11) + b"\n"
        )
        try:
            first = transport.receive()
            second = transport.receive()
        finally:
            transport.close()
            server.close()

        self.assertEqual((first.sequence_number, second.sequence_number), (10, 11))

    def test_wraps_tcp_socket_with_authenticated_tls_context(self):
        client, server = socket.socketpair()
        server.sendall(_frame(15) + b"\n")
        context = Mock()
        context.wrap_socket.return_value = client
        transport = TCPTransport(
            "feed.example.test",
            9001,
            tls=True,
            ssl_context=context,
            reconnect_attempts=0,
            socket_factory=lambda _address, _timeout: client,
        )
        try:
            event = transport.receive()
        finally:
            transport.close()
            server.close()

        context.wrap_socket.assert_called_once_with(
            client, server_hostname="feed.example.test"
        )
        self.assertEqual(event.transport, "TLS")

    def test_receives_network_order_length_prefixed_json(self):
        frame = _frame(11)
        transport, server = self._socket_transport(
            "length-prefixed", struct.pack("!I", len(frame)) + frame
        )
        try:
            event = transport.receive()
        finally:
            transport.close()
            server.close()

        self.assertEqual(event.sequence_number, 11)

    def test_reconnects_after_peer_closes(self):
        first_client, first_server = socket.socketpair()
        second_client, second_server = socket.socketpair()
        first_server.close()
        second_server.sendall(_frame(12) + b"\n")
        sockets = iter((first_client, second_client))
        connections: list[tuple[str, int]] = []

        def socket_factory(address, _timeout):
            connections.append(address)
            return next(sockets)

        transport = TCPTransport(
            "127.0.0.1",
            9000,
            reconnect_attempts=1,
            reconnect_backoff_seconds=0,
            socket_factory=socket_factory,
            sleeper=lambda _delay: None,
        )
        try:
            event = transport.receive()
        finally:
            transport.close()
            second_server.close()

        self.assertEqual(event.sequence_number, 12)
        self.assertEqual(len(connections), 2)

    def test_rejects_oversized_newline_frame(self):
        transport, server = self._socket_transport("newline", b"123456\n")
        transport.max_frame_bytes = 4
        try:
            with self.assertRaisesRegex(FrameError, "exceeds"):
                transport.receive()
        finally:
            transport.close()
            server.close()


class _FakeMulticastSocket:
    def __init__(self, payload: bytes) -> None:
        self.payload = payload
        self.options: list[tuple[int, int, object]] = []
        self.bound = None
        self.timeout = None
        self.closed = False

    def setsockopt(self, level, option, value):
        self.options.append((level, option, value))

    def bind(self, address):
        self.bound = address

    def settimeout(self, timeout):
        self.timeout = timeout

    def recvfrom(self, _size):
        return self.payload, ("192.0.2.10", 5000)

    def close(self):
        self.closed = True


class MulticastTransportTests(unittest.TestCase):
    def test_joins_receives_and_leaves_multicast_group(self):
        active_socket = _FakeMulticastSocket(_frame(21))
        factory = Mock(return_value=active_socket)
        transport = MulticastTransport(
            "239.10.10.10",
            5000,
            interface="127.0.0.1",
            socket_factory=factory,
        )

        event = transport.receive()
        transport.close()

        options = [item[1] for item in active_socket.options]
        self.assertEqual(event.transport, "MULTICAST")
        self.assertIn(socket.IP_ADD_MEMBERSHIP, options)
        self.assertIn(socket.IP_DROP_MEMBERSHIP, options)
        self.assertTrue(active_socket.closed)

    def test_rejects_non_multicast_group(self):
        with self.assertRaisesRegex(ValueError, "multicast"):
            MulticastTransport("127.0.0.1", 5000)


class _ListTransport(MarketEventTransport):
    def __init__(self, events: list[MarketEvent]) -> None:
        self._events = iter(events)

    def receive(self) -> MarketEvent:
        return next(self._events)

    def close(self) -> None:
        return None


class _FiniteStreamTransport(_ListTransport):
    def events(self, limit=None):
        yield from self._events


class MonitoringAndCaptureTests(unittest.TestCase):
    def test_indefinite_monitor_does_not_retain_events_by_default(self):
        received_at = datetime.now(timezone.utc)
        events = [
            MarketEvent.from_mapping(
                _payload(sequence), transport="TCP", received_at=received_at
            )
            for sequence in range(10, 15)
        ]

        observed = monitor_transport(
            _FiniteStreamTransport(events),
            count=None,
            journal_db_path=None,
            output=None,
        )

        self.assertEqual(observed, [])

    def test_finite_monitor_can_disable_event_collection(self):
        event = MarketEvent.from_mapping(
            _payload(10),
            transport="TCP",
            received_at=datetime.now(timezone.utc),
        )

        observed = monitor_transport(
            _ListTransport([event]),
            count=1,
            journal_db_path=None,
            output=None,
            collect_events=False,
        )

        self.assertEqual(observed, [])

    def test_renders_bounded_dataframe_style_event_table(self):
        table_output = EventTableOutput(max_rows=2)
        for sequence, status in ((10, "FIRST"), (12, "GAP"), (11, "REORDERED")):
            event = MarketEvent.from_mapping(
                _payload(sequence),
                transport="TCP",
                received_at=datetime.now(timezone.utc),
            )
            table_output.observe_json(
                json.dumps(
                    {
                        **event.as_dict(),
                        "sequence_status": status,
                        "expected_sequence": 13,
                        "missing_count": 1 if status == "GAP" else 0,
                    }
                )
            )

        stream = io.StringIO()
        Console(file=stream, width=180, color_system=None).print(table_output.render())
        rendered = stream.getvalue()

        self.assertEqual(table_output.total_events, 3)
        self.assertEqual(len(table_output.rows), 2)
        self.assertIn("Network market events", rendered)
        self.assertIn("GAP", rendered)
        self.assertIn("REORDERED", rendered)
        self.assertNotIn("FIRST", rendered)

    def test_monitor_journals_vendor_sequence_evidence(self):
        received_at = datetime.now(timezone.utc)
        events = [
            MarketEvent.from_mapping(
                _payload(sequence), transport="TCP", received_at=received_at
            )
            for sequence in (10, 12)
        ]
        with tempfile.TemporaryDirectory() as directory:
            db_path = str(Path(directory) / "events.duckdb")
            observed = monitor_transport(
                _ListTransport(events),
                count=2,
                journal_db_path=db_path,
                output=None,
            )
            with duckdb.connect(db_path, read_only=True) as connection:
                rows = connection.execute(
                    """
                    SELECT source, stream_id, event_type, sequence_number, sequence_status,
                           expected_sequence, missing_count, correlation_id
                    FROM realtime_event_journal
                    ORDER BY sequence_number
                    """
                ).fetchall()

        self.assertEqual(len(observed), 2)
        self.assertEqual(rows[0][0:5], ("VENDOR-A", "VENDOR-A", "TRADE", 10, "FIRST"))
        self.assertEqual(rows[1][3:7], (12, "GAP", 11, 1))
        self.assertTrue(rows[1][7])

    def test_builds_narrow_capture_filters_and_non_executing_command(self):
        tcp_filter = build_capture_filter("tcp", host_or_group="192.0.2.20", port=9000)
        multicast_filter = build_capture_filter(
            "multicast", host_or_group="239.10.10.10", port=5000
        )
        command = build_tcpdump_command("eth0", "captures/feed.pcap", multicast_filter)

        self.assertEqual(tcp_filter, "tcp and host 192.0.2.20 and port 9000")
        self.assertIn("udp and dst host 239.10.10.10", multicast_filter)
        self.assertEqual(command[0], "tcpdump")
        self.assertEqual(command[-1], multicast_filter)


if __name__ == "__main__":
    unittest.main()
