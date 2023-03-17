"""
Binnacle Python SDK: Network Client
Thread-safe client implementation for interacting with Binnacle and standard MQTT brokers.
"""

import socket
import threading
import time
import struct
from typing import Callable, Optional, Dict, List, Tuple
from .protocol import (
    PacketType,
    ConnectPacket,
    PublishPacket,
    SubscribePacket,
    PingReqPacket,
    DisconnectPacket,
    decode_varint,
    decode_string,
)


class BinnacleClient:
    """
    High-level client for Binnacle broker.
    Provides asynchronous event loops, background keepalives, and automatic message dispatch.
    """

    def __init__(
        self,
        client_id: str,
        host: str = "127.0.0.1",
        port: int = 1883,
        keep_alive: int = 60,
        username: Optional[str] = None,
        password: Optional[str] = None,
    ):
        self.client_id = client_id
        self.host = host
        self.port = port
        self.keep_alive = keep_alive
        self.username = username
        self.password = password

        self._sock: Optional[socket.socket] = None
        self._connected = False
        self._running = False
        self._rx_thread: Optional[threading.Thread] = None
        self._ping_thread: Optional[threading.Thread] = None
        self._lock = threading.Lock()
        self._packet_id = 1
        self._callbacks: Dict[str, List[Callable[[str, bytes], None]]] = {}
        self._default_callback: Optional[Callable[[str, bytes], None]] = None

    def set_default_callback(self, cb: Callable[[str, bytes], None]) -> None:
        self._default_callback = cb

    def add_topic_callback(self, topic: str, cb: Callable[[str, bytes], None]) -> None:
        with self._lock:
            if topic not in self._callbacks:
                self._callbacks[topic] = []
            self._callbacks[topic].append(cb)

    def connect(self, timeout: float = 5.0) -> bool:
        """Establish TCP connection and perform MQTT CONNECT handshake."""
        with self._lock:
            if self._connected:
                return True

            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(timeout)
            sock.connect((self.host, self.port))
            self._sock = sock

            # Send CONNECT packet
            pkt = ConnectPacket(
                client_id=self.client_id,
                keep_alive=self.keep_alive,
                clean_start=True,
                username=self.username,
                password=self.password,
            )
            self._sock.sendall(pkt.serialize())

            # Read CONNACK
            header_byte = self._sock.recv(1)
            if not header_byte:
                raise ConnectionError("Server closed connection during handshake")

            pkt_type = header_byte[0] >> 4
            if pkt_type != PacketType.CONNACK:
                raise ConnectionError(f"Expected CONNACK, received packet type {pkt_type}")

            # Read remaining length
            rem_len_buf = bytearray()
            while True:
                b = self._sock.recv(1)
                rem_len_buf.extend(b)
                if (b[0] & 0x80) == 0:
                    break

            rem_len, _ = decode_varint(bytes(rem_len_buf))
            connack_body = self._recv_exact(rem_len)
            if len(connack_body) < 2:
                raise ConnectionError("Truncated CONNACK packet")

            # Check reason code (offset 1 in MQTT 5)
            reason_code = connack_body[1]
            if reason_code != 0:
                raise ConnectionRefusedError(f"CONNACK rejected with reason code {reason_code}")

            self._connected = True
            self._running = True
            self._sock.settimeout(None)

            # Start RX and ping threads
            self._rx_thread = threading.Thread(target=self._read_loop, daemon=True)
            self._rx_thread.start()

            if self.keep_alive > 0:
                self._ping_thread = threading.Thread(target=self._keepalive_loop, daemon=True)
                self._ping_thread.start()

            return True

    def _recv_exact(self, length: int) -> bytes:
        data = bytearray()
        while len(data) < length:
            chunk = self._sock.recv(length - len(data))
            if not chunk:
                raise ConnectionError("Connection lost while reading data")
            data.extend(chunk)
        return bytes(data)

    def publish(
        self,
        topic: str,
        payload: bytes,
        qos: int = 0,
        retain: bool = False,
    ) -> int:
        """Publish payload to topic."""
        with self._lock:
            if not self._connected or not self._sock:
                raise ConnectionError("Client is not connected")

            pid = 0
            if qos > 0:
                pid = self._packet_id
                self._packet_id = (self._packet_id % 65535) + 1

            pkt = PublishPacket(
                topic=topic,
                payload=payload,
                qos=qos,
                retain=retain,
                packet_id=pid,
            )
            self._sock.sendall(pkt.serialize())
            return pid

    def subscribe(self, topic: str, qos: int = 0) -> int:
        """Subscribe to topic filter."""
        with self._lock:
            if not self._connected or not self._sock:
                raise ConnectionError("Client is not connected")

            pid = self._packet_id
            self._packet_id = (self._packet_id % 65535) + 1

            pkt = SubscribePacket(packet_id=pid, topics=[(topic, qos)])
            self._sock.sendall(pkt.serialize())
            return pid

    def disconnect(self) -> None:
        """Gracefully disconnect from broker."""
        self._running = False
        with self._lock:
            if self._connected and self._sock:
                try:
                    pkt = DisconnectPacket(0)
                    self._sock.sendall(pkt.serialize())
                    self._sock.close()
                except Exception:
                    pass
                finally:
                    self._connected = False
                    self._sock = None

    def _read_loop(self) -> None:
        """Background listener reading inbound packets."""
        try:
            while self._running and self._sock:
                header_byte = self._sock.recv(1)
                if not header_byte:
                    break

                pkt_type = header_byte[0] >> 4
                flags = header_byte[0] & 0x0F

                # Read remaining length
                rem_len_buf = bytearray()
                while True:
                    b = self._sock.recv(1)
                    if not b:
                        return
                    rem_len_buf.extend(b)
                    if (b[0] & 0x80) == 0:
                        break

                rem_len, _ = decode_varint(bytes(rem_len_buf))
                body = self._recv_exact(rem_len)

                if pkt_type == PacketType.PUBLISH:
                    self._handle_publish(flags, body)
                elif pkt_type == PacketType.PINGRESP:
                    pass  # Heartbeat ack received
                elif pkt_type == PacketType.SUBACK:
                    pass  # Subscription confirmed
                elif pkt_type == PacketType.PUBACK:
                    pass  # QoS 1 Publish ack received

        except Exception:
            pass
        finally:
            self._connected = False

    def _handle_publish(self, flags: int, body: bytes) -> None:
        """Parse and dispatch inbound PUBLISH message."""
        qos = (flags >> 1) & 0x03
        offset = 0

        topic, consumed = decode_string(body, offset)
        offset += consumed

        if qos > 0:
            offset += 2  # skip packet_id

        # Skip MQTT 5 properties
        if offset < len(body):
            props_len, c = decode_varint(body, offset)
            offset += c + props_len

        payload = body[offset:]

        # Dispatch callbacks
        matched_cbs = []
        with self._lock:
            if topic in self._callbacks:
                matched_cbs.extend(self._callbacks[topic])
            def_cb = self._default_callback

        for cb in matched_cbs:
            try:
                cb(topic, payload)
            except Exception:
                pass

        if not matched_cbs and def_cb:
            try:
                def_cb(topic, payload)
            except Exception:
                pass

    def _keepalive_loop(self) -> None:
        """Periodic keepalive ping sender."""
        interval = max(1, self.keep_alive // 2)
        while self._running:
            time.sleep(interval)
            with self._lock:
                if not self._running or not self._connected or not self._sock:
                    break
                try:
                    pkt = PingReqPacket()
                    self._sock.sendall(pkt.serialize())
                except Exception:
                    break
