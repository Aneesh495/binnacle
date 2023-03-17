"""
Binnacle Python SDK: Wire Protocol Codec
Implements OASIS MQTT 3.1.1 and MQTT 5.0 packet framing and variable-byte integer encoding.
"""

import struct
from enum import IntEnum
from typing import Tuple, Optional, Dict, Any


class PacketType(IntEnum):
    CONNECT = 1
    CONNACK = 2
    PUBLISH = 3
    PUBACK = 4
    PUBREC = 5
    PUBREL = 6
    PUBCOMP = 7
    SUBSCRIBE = 8
    SUBACK = 9
    UNSUBSCRIBE = 10
    UNSUBACK = 11
    PINGREQ = 12
    PINGRESP = 13
    DISCONNECT = 14
    AUTH = 15


class QoS(IntEnum):
    AT_MOST_ONCE = 0
    AT_LEAST_ONCE = 1
    EXACTLY_ONCE = 2


def encode_varint(value: int) -> bytes:
    """Encode an integer up to 268,435,455 into MQTT Variable Byte Integer format."""
    if value < 0 or value > 268435455:
        raise ValueError(f"VarInt value {value} out of range [0, 268435455]")
    out = bytearray()
    while True:
        byte = value & 0x7F
        value >>= 7
        if value > 0:
            byte |= 0x80
        out.append(byte)
        if value == 0:
            break
    return bytes(out)


def decode_varint(data: bytes, offset: int = 0) -> Tuple[int, int]:
    """
    Decode Variable Byte Integer from bytes at given offset.
    Returns (value, bytes_consumed).
    """
    multiplier = 1
    value = 0
    consumed = 0
    while True:
        if offset + consumed >= len(data):
            raise ValueError("Unexpected EOF while decoding variable byte integer")
        byte = data[offset + consumed]
        consumed += 1
        value += (byte & 0x7F) * multiplier
        if multiplier > 128 * 128 * 128:
            raise ValueError("Malformed variable byte integer: multiplier overflow")
        multiplier *= 128
        if (byte & 0x80) == 0:
            return value, consumed
        if consumed >= 4:
            raise ValueError("Malformed variable byte integer: length exceeds 4 bytes")


def encode_string(text: str) -> bytes:
    """Encode UTF-8 string with 2-byte length prefix."""
    payload = text.encode("utf-8")
    return struct.pack("!H", len(payload)) + payload


def decode_string(data: bytes, offset: int = 0) -> Tuple[str, int]:
    """Decode UTF-8 string with 2-byte length prefix."""
    if offset + 2 > len(data):
        raise ValueError("Buffer underflow reading string length")
    length = struct.unpack_from("!H", data, offset)[0]
    start = offset + 2
    end = start + length
    if end > len(data):
        raise ValueError("Buffer underflow reading string content")
    return data[start:end].decode("utf-8"), 2 + length


class MqttPacket:
    """Base MQTT packet representation."""

    def __init__(self, packet_type: PacketType, flags: int = 0):
        self.packet_type = packet_type
        self.flags = flags

    def serialize(self) -> bytes:
        raise NotImplementedError


class ConnectPacket(MqttPacket):
    """MQTT CONNECT packet."""

    def __init__(
        self,
        client_id: str,
        keep_alive: int = 60,
        clean_start: bool = True,
        username: Optional[str] = None,
        password: Optional[str] = None,
        will_topic: Optional[str] = None,
        will_payload: Optional[bytes] = None,
        will_qos: int = 0,
        will_retain: bool = False,
    ):
        super().__init__(PacketType.CONNECT, 0)
        self.client_id = client_id
        self.keep_alive = keep_alive
        self.clean_start = clean_start
        self.username = username
        self.password = password
        self.will_topic = will_topic
        self.will_payload = will_payload
        self.will_qos = will_qos
        self.will_retain = will_retain

    def serialize(self) -> bytes:
        var_header = bytearray()
        # Protocol Name ("MQTT")
        var_header.extend(encode_string("MQTT"))
        # Protocol Version 5 (0x05)
        var_header.append(5)

        # Connect Flags
        flags = 0
        if self.clean_start:
            flags |= 0x02
        if self.will_topic is not None:
            flags |= 0x04
            flags |= (self.will_qos & 0x03) << 3
            if self.will_retain:
                flags |= 0x20
        if self.password is not None:
            flags |= 0x40
        if self.username is not None:
            flags |= 0x80
        var_header.append(flags)

        # Keep Alive
        var_header.extend(struct.pack("!H", self.keep_alive))

        # MQTT 5 Properties (Property length = 0)
        var_header.append(0)

        # Payload
        payload = bytearray()
        payload.extend(encode_string(self.client_id))

        if self.will_topic is not None:
            # Will Properties length (0)
            payload.append(0)
            payload.extend(encode_string(self.will_topic))
            p = self.will_payload or b""
            payload.extend(struct.pack("!H", len(p)) + p)

        if self.username is not None:
            payload.extend(encode_string(self.username))
        if self.password is not None:
            payload.extend(encode_string(self.password))

        remaining = bytes(var_header) + bytes(payload)
        fixed_header = bytes([(self.packet_type << 4) | self.flags]) + encode_varint(len(remaining))
        return fixed_header + remaining


class PublishPacket(MqttPacket):
    """MQTT PUBLISH packet."""

    def __init__(
        self,
        topic: str,
        payload: bytes,
        qos: int = 0,
        dup: bool = False,
        retain: bool = False,
        packet_id: int = 0,
    ):
        flags = (1 if retain else 0) | ((qos & 0x03) << 1) | (8 if dup else 0)
        super().__init__(PacketType.PUBLISH, flags)
        self.topic = topic
        self.payload = payload
        self.qos = qos
        self.dup = dup
        self.retain = retain
        self.packet_id = packet_id

    def serialize(self) -> bytes:
        body = bytearray()
        body.extend(encode_string(self.topic))
        if self.qos > 0:
            body.extend(struct.pack("!H", self.packet_id))
        # MQTT 5 Properties length = 0
        body.append(0)
        body.extend(self.payload)

        fixed_header = bytes([(self.packet_type << 4) | self.flags]) + encode_varint(len(body))
        return fixed_header + bytes(body)


class SubscribePacket(MqttPacket):
    """MQTT SUBSCRIBE packet."""

    def __init__(self, packet_id: int, topics: list):
        # Bit 1 of flags is reserved as 1 in SUBSCRIBE (0x02)
        super().__init__(PacketType.SUBSCRIBE, 0x02)
        self.packet_id = packet_id
        self.topics = topics  # list of (topic_filter, qos)

    def serialize(self) -> bytes:
        body = bytearray()
        body.extend(struct.pack("!H", self.packet_id))
        # MQTT 5 Properties length = 0
        body.append(0)
        for topic_filter, qos in self.topics:
            body.extend(encode_string(topic_filter))
            body.append(qos & 0x03)

        fixed_header = bytes([(self.packet_type << 4) | self.flags]) + encode_varint(len(body))
        return fixed_header + bytes(body)


class PingReqPacket(MqttPacket):
    """MQTT PINGREQ packet."""

    def __init__(self):
        super().__init__(PacketType.PINGREQ, 0)

    def serialize(self) -> bytes:
        return bytes([(self.packet_type << 4), 0])


class DisconnectPacket(MqttPacket):
    """MQTT DISCONNECT packet."""

    def __init__(self, reason_code: int = 0):
        super().__init__(PacketType.DISCONNECT, 0)
        self.reason_code = reason_code

    def serialize(self) -> bytes:
        # Reason code + properties length 0
        body = bytes([self.reason_code, 0])
        fixed_header = bytes([(self.packet_type << 4), len(body)])
        return fixed_header + body
