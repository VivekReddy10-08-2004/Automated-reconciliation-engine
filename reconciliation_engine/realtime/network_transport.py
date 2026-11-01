"""TCP/TLS and UDP multicast adapters for traceable market-data events."""

from __future__ import annotations

import argparse
from abc import ABC, abstractmethod
from collections import deque
from collections.abc import Callable, Iterator, Mapping
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
import ipaddress
import json
import math
from pathlib import Path
import shlex
import socket
import ssl
import struct
import time
from typing import Any
from uuid import NAMESPACE_URL, uuid5

from opentelemetry import trace
from rich.console import Console
from rich.live import Live
from rich.table import Table
from rich.text import Text

from reconciliation_engine.observability import (
    configure_telemetry,
    get_telemetry,
    log_event,
)
from reconciliation_engine.storage.duckdb import (
    initialize_database,
    persist_realtime_event,
)


SEQUENCE_STATUSES = frozenset({"FIRST", "IN_ORDER", "GAP", "DUPLICATE", "REORDERED"})
FRAMING_MODES = frozenset({"newline", "length-prefixed"})
DEFAULT_MAX_FRAME_BYTES = 1_048_576
DEFAULT_MAX_DATAGRAM_BYTES = 65_507


class TransportError(RuntimeError):
    """Base error for network transport failures."""


class ConnectionClosedError(TransportError):
    """The peer closed a TCP connection before a complete frame arrived."""


class FrameError(TransportError):
    """A network frame was empty, oversized, truncated, or malformed."""


class EventValidationError(ValueError):
    """A decoded payload does not satisfy the common market-event contract."""


def _utc_datetime(value: object, field_name: str) -> datetime:
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, str):
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError as error:
            raise EventValidationError(
                f"{field_name} must be a valid ISO-8601 timestamp"
            ) from error
    else:
        raise EventValidationError(f"{field_name} must be an ISO-8601 timestamp")
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise EventValidationError(f"{field_name} must be timezone-aware")
    return parsed.astimezone(timezone.utc)


def _positive_number(value: object, field_name: str, *, allow_zero: bool) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise EventValidationError(f"{field_name} must be numeric")
    number = float(value)
    if not math.isfinite(number) or number < 0 or (not allow_zero and number == 0):
        qualifier = "non-negative" if allow_zero else "positive"
        raise EventValidationError(f"{field_name} must be a {qualifier} finite number")
    return number


@dataclass(frozen=True)
class MarketEvent:
    """Transport-independent market-data event with vendor sequence lineage."""

    event_id: str
    source: str
    stream_id: str
    transport: str
    sequence_number: int
    symbol: str
    event_type: str
    price: float
    quantity: float
    exchange_timestamp: datetime
    received_timestamp: datetime
    correlation_id: str

    @classmethod
    def from_mapping(
        cls,
        payload: Mapping[str, object],
        *,
        transport: str,
        received_at: datetime | None = None,
    ) -> "MarketEvent":
        required = {
            "source",
            "sequence_number",
            "symbol",
            "event_type",
            "price",
            "quantity",
            "exchange_timestamp",
        }
        missing = required.difference(payload)
        if missing:
            raise EventValidationError(
                f"Market event is missing fields: {sorted(missing)}"
            )
        sequence = payload["sequence_number"]
        if isinstance(sequence, bool) or not isinstance(sequence, int) or sequence < 0:
            raise EventValidationError("sequence_number must be a non-negative integer")
        strings: dict[str, str] = {}
        for field_name in ("source", "symbol", "event_type"):
            value = payload[field_name]
            if not isinstance(value, str) or not value.strip():
                raise EventValidationError(f"{field_name} must be a non-empty string")
            strings[field_name] = value.strip().upper()
        normalized_transport = transport.strip().upper()
        if normalized_transport not in {"TCP", "TLS", "MULTICAST"}:
            raise EventValidationError("transport must be TCP, TLS, or MULTICAST")
        stream_value = payload.get("stream_id", strings["source"])
        if not isinstance(stream_value, str) or not stream_value.strip():
            raise EventValidationError("stream_id must be a non-empty string")
        stream_id = stream_value.strip().upper()
        exchange_time = _utc_datetime(
            payload["exchange_timestamp"], "exchange_timestamp"
        )
        receive_time = _utc_datetime(
            received_at or datetime.now(timezone.utc), "received_timestamp"
        )
        if receive_time < exchange_time:
            raise EventValidationError(
                "received_timestamp cannot precede exchange_timestamp"
            )
        identity = (
            f"{strings['source']}|{stream_id}|{normalized_transport}|{sequence}|"
            f"{strings['symbol']}|{strings['event_type']}|{exchange_time.isoformat()}"
        )
        event_id_value = payload.get("event_id")
        event_id = (
            str(event_id_value).strip()
            if event_id_value is not None
            else str(uuid5(NAMESPACE_URL, identity))
        )
        if not event_id:
            raise EventValidationError("event_id must be non-empty when supplied")
        correlation_value = payload.get("correlation_id", event_id)
        correlation_id = str(correlation_value).strip()
        if not correlation_id:
            raise EventValidationError("correlation_id must be non-empty when supplied")
        return cls(
            event_id=event_id,
            source=strings["source"],
            stream_id=stream_id,
            transport=normalized_transport,
            sequence_number=sequence,
            symbol=strings["symbol"],
            event_type=strings["event_type"],
            price=_positive_number(payload["price"], "price", allow_zero=False),
            quantity=_positive_number(payload["quantity"], "quantity", allow_zero=True),
            exchange_timestamp=exchange_time,
            received_timestamp=receive_time,
            correlation_id=correlation_id,
        )

    @property
    def latency_ms(self) -> float:
        return round(
            (self.received_timestamp - self.exchange_timestamp).total_seconds() * 1000,
            3,
        )

    def as_dict(self) -> dict[str, object]:
        values = asdict(self)
        values["exchange_timestamp"] = self.exchange_timestamp.isoformat()
        values["received_timestamp"] = self.received_timestamp.isoformat()
        values["latency_ms"] = self.latency_ms
        return values

    def as_journal_event(
        self,
        sequence_status: str,
        *,
        reason: str,
        expected_sequence: int | None = None,
        missing_count: int = 0,
    ) -> dict[str, object]:
        if sequence_status not in SEQUENCE_STATUSES:
            raise ValueError(f"Unsupported sequence status: {sequence_status}")
        return {
            "event_id": self.event_id,
            "sequence_number": self.sequence_number,
            "sequence_status": sequence_status,
            "transport": self.transport,
            "source": self.source,
            "stream_id": self.stream_id,
            "event_type": self.event_type,
            "quantity": self.quantity,
            "correlation_id": self.correlation_id,
            "expected_sequence": expected_sequence,
            "missing_count": missing_count,
            "symbol": self.symbol,
            "price": self.price,
            "exchange_timestamp": self.exchange_timestamp.isoformat(),
            "received_timestamp": self.received_timestamp.isoformat(),
            "latency_ms": self.latency_ms,
            "pct_change": None,
            "is_anomaly": False,
            "reason": reason,
        }


@dataclass(frozen=True)
class SequenceObservation:
    status: str
    expected_sequence: int | None
    missing_count: int


class SequenceTracker:
    """Classify vendor sequences independently for each named source stream."""

    def __init__(self, *, duplicate_window: int = 4096) -> None:
        if duplicate_window <= 0:
            raise ValueError("duplicate_window must be positive")
        self.duplicate_window = duplicate_window
        self._highest: dict[tuple[str, str], int] = {}
        self._recent: dict[tuple[str, str], deque[int]] = {}
        self._recent_counts: dict[tuple[str, str], dict[int, int]] = {}

    def observe(
        self,
        source: str,
        stream_id: str,
        sequence_number: int,
    ) -> SequenceObservation:
        if not isinstance(source, str) or not source.strip():
            raise ValueError("source must be non-empty")
        if not isinstance(stream_id, str) or not stream_id.strip():
            raise ValueError("stream_id must be non-empty")
        if (
            isinstance(sequence_number, bool)
            or not isinstance(sequence_number, int)
            or sequence_number < 0
        ):
            raise ValueError("sequence_number must be a non-negative integer")
        key = (source.strip().upper(), stream_id.strip().upper())
        highest = self._highest.get(key)
        recent = self._recent.setdefault(key, deque())
        recent_counts = self._recent_counts.setdefault(key, {})
        if sequence_number in recent_counts:
            observation = SequenceObservation(
                "DUPLICATE",
                None if highest is None else highest + 1,
                0,
            )
        elif highest is None:
            observation = SequenceObservation("FIRST", None, 0)
            self._highest[key] = sequence_number
        elif sequence_number == highest + 1:
            observation = SequenceObservation("IN_ORDER", highest + 1, 0)
            self._highest[key] = sequence_number
        elif sequence_number > highest + 1:
            observation = SequenceObservation(
                "GAP", highest + 1, sequence_number - highest - 1
            )
            self._highest[key] = sequence_number
        else:
            observation = SequenceObservation("REORDERED", highest + 1, 0)
        recent.append(sequence_number)
        recent_counts[sequence_number] = recent_counts.get(sequence_number, 0) + 1
        while len(recent) > self.duplicate_window:
            removed = recent.popleft()
            remaining = recent_counts[removed] - 1
            if remaining:
                recent_counts[removed] = remaining
            else:
                del recent_counts[removed]
        return observation


class MarketEventTransport(ABC):
    """Blocking adapter interface shared by stream and datagram transports."""

    @abstractmethod
    def receive(self) -> MarketEvent:
        raise NotImplementedError

    @abstractmethod
    def close(self) -> None:
        raise NotImplementedError

    def events(self, *, limit: int | None = None) -> Iterator[MarketEvent]:
        if limit is not None and limit < 0:
            raise ValueError("limit must be non-negative or None")
        count = 0
        while limit is None or count < limit:
            yield self.receive()
            count += 1

    def __enter__(self) -> "MarketEventTransport":
        return self

    def __exit__(self, *_: object) -> None:
        self.close()


SocketFactory = Callable[[tuple[str, int], float], socket.socket]


class TCPTransport(MarketEventTransport):
    """Receive framed JSON events over TCP with optional TLS and reconnects."""

    def __init__(
        self,
        host: str,
        port: int,
        *,
        framing: str = "newline",
        tls: bool = False,
        ssl_context: ssl.SSLContext | None = None,
        connect_timeout: float = 5.0,
        read_timeout: float = 30.0,
        reconnect_attempts: int = 3,
        reconnect_backoff_seconds: float = 0.25,
        max_frame_bytes: int = DEFAULT_MAX_FRAME_BYTES,
        socket_factory: SocketFactory = socket.create_connection,
        sleeper: Callable[[float], None] = time.sleep,
    ) -> None:
        if not isinstance(host, str) or not host.strip():
            raise ValueError("host must be non-empty")
        _validate_port(port)
        normalized_framing = framing.strip().lower()
        if normalized_framing not in FRAMING_MODES:
            raise ValueError(f"framing must be one of: {sorted(FRAMING_MODES)}")
        if connect_timeout <= 0 or read_timeout <= 0:
            raise ValueError("TCP timeouts must be positive")
        if reconnect_attempts < 0 or reconnect_backoff_seconds < 0:
            raise ValueError("Reconnect settings must be non-negative")
        if max_frame_bytes <= 0:
            raise ValueError("max_frame_bytes must be positive")
        self.host = host.strip()
        self.port = port
        self.framing = normalized_framing
        self.tls = tls
        self.ssl_context = ssl_context
        self.connect_timeout = connect_timeout
        self.read_timeout = read_timeout
        self.reconnect_attempts = reconnect_attempts
        self.reconnect_backoff_seconds = reconnect_backoff_seconds
        self.max_frame_bytes = max_frame_bytes
        self.socket_factory = socket_factory
        self.sleeper = sleeper
        self._socket: socket.socket | None = None
        self._buffer = bytearray()

    @property
    def transport_name(self) -> str:
        return "TLS" if self.tls else "TCP"

    def connect(self) -> None:
        if self._socket is not None:
            return
        raw_socket = self.socket_factory((self.host, self.port), self.connect_timeout)
        try:
            raw_socket.settimeout(self.read_timeout)
            if self.tls:
                context = self.ssl_context or ssl.create_default_context()
                active_socket = context.wrap_socket(
                    raw_socket,
                    server_hostname=self.host,
                )
            else:
                active_socket = raw_socket
        except Exception:
            raw_socket.close()
            raise
        self._socket = active_socket
        self._buffer.clear()
        log_event(
            "network_transport_connected",
            transport=self.transport_name,
            host=self.host,
            port=self.port,
            framing=self.framing,
        )

    def _recv(self) -> bytes:
        assert self._socket is not None
        chunk = self._socket.recv(min(65_536, self.max_frame_bytes + 1))
        if not chunk:
            raise ConnectionClosedError("TCP peer closed the connection")
        return chunk

    def _read_newline_frame(self) -> bytes:
        while True:
            delimiter = self._buffer.find(b"\n")
            if delimiter >= 0:
                if delimiter > self.max_frame_bytes:
                    raise FrameError("Newline-delimited frame exceeds max_frame_bytes")
                frame = bytes(self._buffer[:delimiter])
                del self._buffer[: delimiter + 1]
                if not frame.strip():
                    raise FrameError("Newline-delimited frame cannot be empty")
                return frame
            if len(self._buffer) > self.max_frame_bytes:
                raise FrameError("Newline-delimited frame exceeds max_frame_bytes")
            self._buffer.extend(self._recv())

    def _fill(self, size: int) -> None:
        while len(self._buffer) < size:
            self._buffer.extend(self._recv())

    def _read_length_prefixed_frame(self) -> bytes:
        self._fill(4)
        frame_length = struct.unpack("!I", self._buffer[:4])[0]
        del self._buffer[:4]
        if frame_length <= 0:
            raise FrameError("Length-prefixed frame cannot be empty")
        if frame_length > self.max_frame_bytes:
            raise FrameError("Length-prefixed frame exceeds max_frame_bytes")
        self._fill(frame_length)
        frame = bytes(self._buffer[:frame_length])
        del self._buffer[:frame_length]
        return frame

    def _read_frame(self) -> bytes:
        if self.framing == "newline":
            return self._read_newline_frame()
        return self._read_length_prefixed_frame()

    def receive(self) -> MarketEvent:
        last_error: Exception | None = None
        for attempt in range(self.reconnect_attempts + 1):
            try:
                self.connect()
                frame = self._read_frame()
                try:
                    return _decode_event(frame, transport=self.transport_name)
                except (FrameError, EventValidationError) as error:
                    log_event(
                        "network_frame_rejected",
                        transport=self.transport_name,
                        framing=self.framing,
                        payload_bytes=len(frame),
                        error_type=type(error).__name__,
                    )
                    raise
            except (ConnectionClosedError, OSError, socket.timeout) as error:
                last_error = error
                self.close()
                if attempt >= self.reconnect_attempts:
                    break
                delay = self.reconnect_backoff_seconds * (2**attempt)
                log_event(
                    "network_transport_reconnecting",
                    transport=self.transport_name,
                    host=self.host,
                    port=self.port,
                    attempt=attempt + 1,
                    delay_seconds=delay,
                    error_type=type(error).__name__,
                )
                self.sleeper(delay)
        raise TransportError(
            f"TCP receive failed after {self.reconnect_attempts + 1} attempts"
        ) from last_error

    def close(self) -> None:
        active_socket = self._socket
        self._socket = None
        self._buffer.clear()
        if active_socket is not None:
            try:
                active_socket.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            active_socket.close()
            log_event(
                "network_transport_disconnected",
                transport=self.transport_name,
                host=self.host,
                port=self.port,
            )


class MulticastTransport(MarketEventTransport):
    """Receive JSON events from one IPv4 UDP multicast group."""

    def __init__(
        self,
        group: str,
        port: int,
        *,
        interface: str = "0.0.0.0",
        bind_address: str = "",
        read_timeout: float = 30.0,
        max_datagram_bytes: int = DEFAULT_MAX_DATAGRAM_BYTES,
        socket_factory: Callable[..., socket.socket] = socket.socket,
    ) -> None:
        try:
            group_ip = ipaddress.IPv4Address(group)
            interface_ip = ipaddress.IPv4Address(interface)
        except ipaddress.AddressValueError as error:
            raise ValueError(
                "group and interface must be valid IPv4 addresses"
            ) from error
        if not group_ip.is_multicast:
            raise ValueError("group must be an IPv4 multicast address")
        _validate_port(port)
        if read_timeout <= 0:
            raise ValueError("read_timeout must be positive")
        if not 1 <= max_datagram_bytes <= 65_507:
            raise ValueError("max_datagram_bytes must be between 1 and 65507")
        self.group = str(group_ip)
        self.port = port
        self.interface = str(interface_ip)
        self.bind_address = bind_address
        self.read_timeout = read_timeout
        self.max_datagram_bytes = max_datagram_bytes
        self.socket_factory = socket_factory
        self._socket: socket.socket | None = None
        self._membership = struct.pack(
            "=4s4s",
            socket.inet_aton(self.group),
            socket.inet_aton(self.interface),
        )

    def open(self) -> None:
        if self._socket is not None:
            return
        active_socket = self.socket_factory(
            socket.AF_INET,
            socket.SOCK_DGRAM,
            socket.IPPROTO_UDP,
        )
        try:
            active_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            active_socket.bind((self.bind_address, self.port))
            active_socket.setsockopt(
                socket.IPPROTO_IP,
                socket.IP_ADD_MEMBERSHIP,
                self._membership,
            )
            active_socket.settimeout(self.read_timeout)
        except Exception:
            active_socket.close()
            raise
        self._socket = active_socket
        log_event(
            "network_transport_joined",
            transport="MULTICAST",
            group=self.group,
            port=self.port,
            interface=self.interface,
        )

    def receive(self) -> MarketEvent:
        self.open()
        assert self._socket is not None
        payload, address = self._socket.recvfrom(self.max_datagram_bytes + 1)
        if len(payload) > self.max_datagram_bytes:
            raise FrameError("Multicast datagram exceeds max_datagram_bytes")
        try:
            event = _decode_event(payload, transport="MULTICAST")
        except (FrameError, EventValidationError) as error:
            log_event(
                "network_frame_rejected",
                transport="MULTICAST",
                group=self.group,
                port=self.port,
                payload_bytes=len(payload),
                error_type=type(error).__name__,
            )
            raise
        log_event(
            "network_datagram_received",
            transport="MULTICAST",
            group=self.group,
            port=self.port,
            peer_address=address[0],
            payload_bytes=len(payload),
            event_id=event.event_id,
        )
        return event

    def close(self) -> None:
        active_socket = self._socket
        self._socket = None
        if active_socket is None:
            return
        try:
            active_socket.setsockopt(
                socket.IPPROTO_IP,
                socket.IP_DROP_MEMBERSHIP,
                self._membership,
            )
        except OSError:
            pass
        active_socket.close()
        log_event(
            "network_transport_left",
            transport="MULTICAST",
            group=self.group,
            port=self.port,
            interface=self.interface,
        )


def _validate_port(port: int) -> None:
    if isinstance(port, bool) or not isinstance(port, int) or not 1 <= port <= 65_535:
        raise ValueError("port must be an integer between 1 and 65535")


def _decode_event(frame: bytes, *, transport: str) -> MarketEvent:
    try:
        payload: Any = json.loads(frame.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise FrameError("Network frame must contain one UTF-8 JSON object") from error
    if not isinstance(payload, dict):
        raise FrameError("Network frame JSON must be an object")
    return MarketEvent.from_mapping(payload, transport=transport)


def build_capture_filter(
    transport: str,
    *,
    host_or_group: str,
    port: int,
) -> str:
    """Return a narrow tcpdump/Wireshark capture filter for one feed."""
    _validate_port(port)
    normalized = transport.strip().lower()
    if normalized in {"tcp", "tls"}:
        try:
            ipaddress.ip_address(host_or_group)
        except ValueError as error:
            raise ValueError("TCP capture host must be an IP address") from error
        return f"tcp and host {host_or_group} and port {port}"
    if normalized == "multicast":
        try:
            address = ipaddress.IPv4Address(host_or_group)
        except ipaddress.AddressValueError as error:
            raise ValueError(
                "Multicast capture group must be an IPv4 address"
            ) from error
        if not address.is_multicast:
            raise ValueError("Multicast capture group must be multicast")
        return f"udp and dst host {address} and dst port {port}"
    raise ValueError("transport must be tcp, tls, or multicast")


def build_tcpdump_command(
    interface: str,
    output_path: str | Path,
    capture_filter: str,
) -> tuple[str, ...]:
    """Build, but never execute, a bounded packet-capture command."""
    if not interface.strip():
        raise ValueError("interface must be non-empty")
    destination = Path(output_path)
    if destination.suffix.lower() != ".pcap":
        raise ValueError("packet capture output must use the .pcap suffix")
    if not capture_filter.strip():
        raise ValueError("capture_filter must be non-empty")
    return (
        "tcpdump",
        "-i",
        interface.strip(),
        "-nn",
        "-s",
        "0",
        "-w",
        str(destination),
        capture_filter.strip(),
    )


def monitor_transport(
    transport: MarketEventTransport,
    *,
    count: int | None = None,
    journal_db_path: str | None = "reconciliation.duckdb",
    output: Callable[[str], None] | None = print,
    collect_events: bool | None = None,
) -> list[MarketEvent]:
    """Trace, classify, journal, and optionally collect events from an adapter.

    Finite runs retain their events by default for backwards compatibility.
    Indefinite runs do not retain events, so a long-lived monitor has bounded
    memory. Callers can override either default with ``collect_events``.
    """
    if count is not None and count < 0:
        raise ValueError("count must be non-negative or None")
    if journal_db_path is not None:
        initialize_database(journal_db_path)
    tracker = SequenceTracker()
    observed: list[MarketEvent] = []
    should_collect = count is not None if collect_events is None else collect_events
    for event in transport.events(limit=count):
        with get_telemetry().span(
            "network.event.process",
            **{
                "event.id": event.event_id,
                "event.source": event.source,
                "event.stream_id": event.stream_id,
                "event.sequence_number": event.sequence_number,
                "network.transport": event.transport,
            },
        ) as span:
            sequence = tracker.observe(
                event.source,
                event.stream_id,
                event.sequence_number,
            )
            span.set_attribute("event.sequence_status", sequence.status)
            span.set_attribute("event.missing_count", sequence.missing_count)
            span_context = trace.get_current_span().get_span_context()
            trace_id = (
                format(span_context.trace_id, "032x") if span_context.is_valid else None
            )
            span_id = (
                format(span_context.span_id, "016x") if span_context.is_valid else None
            )
            reason = _sequence_reason(event, sequence)
            if journal_db_path is not None:
                persist_realtime_event(
                    event.as_journal_event(
                        sequence.status,
                        reason=reason,
                        expected_sequence=sequence.expected_sequence,
                        missing_count=sequence.missing_count,
                    ),
                    trace_id=trace_id,
                    span_id=span_id,
                    db_path=journal_db_path,
                    initialize_schema=False,
                )
            log_event(
                "network_market_event",
                event_id=event.event_id,
                correlation_id=event.correlation_id,
                source=event.source,
                stream_id=event.stream_id,
                transport=event.transport,
                sequence_number=event.sequence_number,
                sequence_status=sequence.status,
                missing_count=sequence.missing_count,
                latency_ms=event.latency_ms,
            )
            if output:
                rendered = {
                    **event.as_dict(),
                    "sequence_status": sequence.status,
                    "expected_sequence": sequence.expected_sequence,
                    "missing_count": sequence.missing_count,
                }
                output(json.dumps(rendered, separators=(",", ":"), sort_keys=True))
            if should_collect:
                observed.append(event)
    return observed


def _sequence_reason(
    event: MarketEvent,
    observation: SequenceObservation,
) -> str:
    if observation.status == "GAP":
        return (
            f"Missing {observation.missing_count} event(s) before vendor sequence "
            f"{event.sequence_number}; expected {observation.expected_sequence}"
        )
    if observation.status == "DUPLICATE":
        return f"Duplicate vendor sequence {event.sequence_number}"
    if observation.status == "REORDERED":
        return (
            f"Reordered vendor sequence {event.sequence_number}; "
            f"next expected was {observation.expected_sequence}"
        )
    return f"Vendor sequence {event.sequence_number} classified {observation.status}"


class EventTableOutput:
    """Maintain a bounded, dataframe-style view of recently received events."""

    STATUS_STYLES = {
        "FIRST": "cyan",
        "IN_ORDER": "green",
        "GAP": "bold red",
        "DUPLICATE": "yellow",
        "REORDERED": "magenta",
    }

    def __init__(self, max_rows: int = 20) -> None:
        if max_rows <= 0:
            raise ValueError("max_rows must be positive")
        self.max_rows = max_rows
        self.rows: deque[dict[str, object]] = deque(maxlen=max_rows)
        self.total_events = 0

    def observe_json(self, message: str) -> None:
        payload = json.loads(message)
        if not isinstance(payload, dict):
            raise ValueError("Rendered event must be a JSON object")
        self.rows.append(payload)
        self.total_events += 1

    def render(self) -> Table:
        table = Table(
            title=(
                f"Network market events — received {self.total_events} "
                f"(showing latest {min(len(self.rows), self.max_rows)})"
            ),
            expand=True,
            show_lines=False,
        )
        for label, justify in (
            ("Received (UTC)", "left"),
            ("Transport", "left"),
            ("Source / Stream", "left"),
            ("Seq", "right"),
            ("Status", "left"),
            ("Symbol", "left"),
            ("Type", "left"),
            ("Price", "right"),
            ("Qty", "right"),
            ("Latency ms", "right"),
            ("Missing", "right"),
        ):
            table.add_column(label, justify=justify, no_wrap=True)
        if not self.rows:
            table.add_row("Waiting for events…", *("" for _ in range(10)))
            return table
        for event in self.rows:
            received = str(event.get("received_timestamp", ""))
            if "T" in received:
                received = received.split("T", 1)[1].replace("+00:00", "Z")
            source_stream = f"{event.get('source', '')} / {event.get('stream_id', '')}"
            status = str(event.get("sequence_status", ""))
            table.add_row(
                received,
                str(event.get("transport", "")),
                source_stream,
                str(event.get("sequence_number", "")),
                Text(status, style=self.STATUS_STYLES.get(status, "white")),
                str(event.get("symbol", "")),
                str(event.get("event_type", "")),
                _table_number(event.get("price")),
                _table_number(event.get("quantity")),
                _table_number(event.get("latency_ms")),
                str(event.get("missing_count", 0)),
            )
        return table


def _table_number(value: object) -> str:
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return f"{float(value):,.3f}"
    return "" if value is None else str(value)


def _add_monitor_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--count", type=int, default=0)
    parser.add_argument("--db", default="reconciliation.duckdb")
    parser.add_argument("--timeout", type=float, default=30.0)
    parser.add_argument(
        "--output-format",
        choices=("table", "json"),
        default="table",
        help="Display a live table or emit newline-delimited JSON",
    )
    parser.add_argument(
        "--table-rows",
        type=int,
        default=20,
        help="Maximum recent events retained in the live table",
    )


def _argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)

    tcp = subparsers.add_parser("tcp", help="Monitor a TCP or TLS JSON feed")
    tcp.add_argument("host")
    tcp.add_argument("port", type=int)
    tcp.add_argument("--framing", choices=sorted(FRAMING_MODES), default="newline")
    tcp.add_argument("--tls", action="store_true")
    tcp.add_argument("--reconnect-attempts", type=int, default=3)
    _add_monitor_arguments(tcp)

    multicast = subparsers.add_parser(
        "multicast", help="Monitor an IPv4 UDP multicast JSON feed"
    )
    multicast.add_argument("group")
    multicast.add_argument("port", type=int)
    multicast.add_argument("--interface", default="0.0.0.0")
    multicast.add_argument("--bind-address", default="")
    _add_monitor_arguments(multicast)

    capture = subparsers.add_parser(
        "capture-command", help="Print a narrow tcpdump command without executing it"
    )
    capture.add_argument("transport", choices=("tcp", "tls", "multicast"))
    capture.add_argument("host_or_group")
    capture.add_argument("port", type=int)
    capture.add_argument("--interface", required=True)
    capture.add_argument("--output", required=True)
    return parser


def main() -> None:
    arguments = _argument_parser().parse_args()
    if arguments.command == "capture-command":
        capture_filter = build_capture_filter(
            arguments.transport,
            host_or_group=arguments.host_or_group,
            port=arguments.port,
        )
        print(
            shlex.join(
                build_tcpdump_command(
                    arguments.interface, arguments.output, capture_filter
                )
            )
        )
        return

    configure_telemetry(service_name="reconciliation-network-feed")
    count = None if arguments.count == 0 else arguments.count
    if arguments.command == "tcp":
        transport: MarketEventTransport = TCPTransport(
            arguments.host,
            arguments.port,
            framing=arguments.framing,
            tls=arguments.tls,
            read_timeout=arguments.timeout,
            reconnect_attempts=arguments.reconnect_attempts,
        )
    else:
        transport = MulticastTransport(
            arguments.group,
            arguments.port,
            interface=arguments.interface,
            bind_address=arguments.bind_address,
            read_timeout=arguments.timeout,
        )
    try:
        with transport:
            if arguments.output_format == "json":
                monitor_transport(
                    transport,
                    count=count,
                    journal_db_path=arguments.db,
                )
            else:
                table_output = EventTableOutput(arguments.table_rows)
                with Live(
                    table_output.render(),
                    console=Console(),
                    refresh_per_second=4,
                ) as live:

                    def update_table(message: str) -> None:
                        table_output.observe_json(message)
                        live.update(table_output.render())

                    monitor_transport(
                        transport,
                        count=count,
                        journal_db_path=arguments.db,
                        output=update_table,
                    )
    except KeyboardInterrupt:
        return
    except (TransportError, FrameError, EventValidationError, OSError) as error:
        Console(stderr=True).print(
            f"[bold red]Feed monitor stopped:[/bold red] {error}"
        )
        raise SystemExit(1) from None


if __name__ == "__main__":
    main()
