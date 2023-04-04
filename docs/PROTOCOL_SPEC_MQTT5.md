# OASIS MQTT 5.0 Protocol Specification Reference

## 1. Protocol Architecture and Wire Framing

Binnacle implements the complete OASIS MQTT Version 5.0 and 3.1.1 standards. An MQTT packet consists of three components:
1. **Fixed Header:** 1 byte packet type and control flags, followed by Remaining Length (1-4 bytes).
2. **Variable Header:** Protocol name, version, packet identifiers, reason codes, and the Property section.
3. **Payload:** Application data, topic definitions, credentials, or will attributes.

```
+-----------------------------------------------------------------+
| Byte 1: Packet Type (Bits 7..4) | Flags (Bits 3..0)             |
+-----------------------------------------------------------------+
| Bytes 2..5: Remaining Length (MQTT Variable Byte Integer)       |
+-----------------------------------------------------------------+
| Variable Header (Packet Identifier, Reason Code, Properties)    |
+-----------------------------------------------------------------+
| Payload (Topic, Binary Content, Credentials)                    |
+-----------------------------------------------------------------+
```

---

## 2. Variable Byte Integer Encoding

The Variable Byte Integer uses an encoded 7-bit payload per byte with the most significant bit (bit 7) functioning as a continuation flag. Values range from 0 up to 268,435,455 (0x0FFFFFFF).

| Byte Count | Minimum Value | Maximum Value | Hex Representation |
|:---|:---|:---|:---|
| 1 | 0 | 127 | 0x00 .. 0x7F |
| 2 | 128 | 16,383 | 0x80 0x01 .. 0xFF 0x7F |
| 3 | 16,384 | 2,097,151 | 0x80 0x80 0x01 .. 0xFF 0xFF 0x7F |
| 4 | 2,097,152 | 268,435,455 | 0x80 0x80 0x80 0x01 .. 0xFF 0xFF 0xFF 0x7F |

---

## 3. MQTT 5.0 Properties Taxonomy

MQTT 5.0 introduces extensibility through strongly-typed Property identifiers prefixed in the variable header.

| Identifier | Hex | Property Name | Data Type | Applicable Packets |
|:---|:---|:---|:---|:---|
| 1 | 0x01 | Payload Format Indicator | Byte | PUBLISH, Will |
| 2 | 0x02 | Message Expiry Interval | 4-Byte Integer | PUBLISH, Will |
| 3 | 0x03 | Content Type | UTF-8 String | PUBLISH, Will |
| 8 | 0x08 | Response Topic | UTF-8 String | PUBLISH, Will |
| 9 | 0x09 | Correlation Data | Binary Data | PUBLISH, Will |
| 11 | 0x0B | Subscription Identifier | Variable Byte Int | PUBLISH, SUBSCRIBE |
| 17 | 0x11 | Session Expiry Interval | 4-Byte Integer | CONNECT, CONNACK, DISCONNECT |
| 18 | 0x12 | Assigned Client Identifier | UTF-8 String | CONNACK |
| 19 | 0x13 | Server Keep Alive | 2-Byte Integer | CONNACK |
| 21 | 0x15 | Authentication Method | UTF-8 String | CONNECT, CONNACK, AUTH |
| 22 | 0x16 | Authentication Data | Binary Data | CONNECT, CONNACK, AUTH |
| 24 | 0x18 | Will Delay Interval | 4-Byte Integer | Will Properties |
| 33 | 0x21 | Receive Maximum | 2-Byte Integer | CONNECT, CONNACK |
| 34 | 0x22 | Topic Alias Maximum | 2-Byte Integer | CONNECT, CONNACK |
| 35 | 0x23 | Topic Alias | 2-Byte Integer | PUBLISH |
| 36 | 0x24 | Maximum QoS | Byte | CONNACK |
| 37 | 0x25 | Retain Available | Byte | CONNACK |
| 38 | 0x26 | User Property | UTF-8 String Pair | All Packets |
| 39 | 0x27 | Maximum Packet Size | 4-Byte Integer | CONNECT, CONNACK |
| 40 | 0x28 | Wildcard Sub Available | Byte | CONNACK |
| 41 | 0x29 | Sub Identifier Available | Byte | CONNACK |
| 42 | 0x2A | Shared Sub Available | Byte | CONNACK |

---

## 4. Reason Codes Reference

MQTT 5.0 unifies response statuses across a single 1-byte Reason Code dictionary.

| Decimal | Hex | Name | Meaning | Packets |
|:---|:---|:---|:---|:---|
| 0 | 0x00 | Success / Normal Disconnection | Operation completed successfully | All Acks |
| 1 | 0x01 | Granted QoS 0 | Subscription accepted at QoS 0 | SUBACK |
| 2 | 0x02 | Granted QoS 1 | Subscription accepted at QoS 1 | SUBACK |
| 16 | 0x10 | No Matching Subscribers | Message accepted but no subscribers | PUBACK, PUBREC |
| 128 | 0x80 | Unspecified Error | General server error | All Acks |
| 129 | 0x81 | Malformed Packet | Framing or data format violated | CONNACK, DISCONNECT |
| 130 | 0x82 | Protocol Error | Protocol rule broken | CONNACK, DISCONNECT |
| 131 | 0x83 | Implementation Specific Error | Valid packet rejected by broker | All Acks |
| 134 | 0x86 | Bad User Name or Password | Credentials invalid | CONNACK |
| 135 | 0x87 | Not Authorized | Client lacks permission | All Acks |
| 142 | 0x8E | Session Taken Over | New connection superseded active one | DISCONNECT |
| 147 | 0x93 | Receive Maximum Exceeded | Inflight quota violated | DISCONNECT |
| 148 | 0x94 | Topic Alias Invalid | Topic alias out of range | DISCONNECT |
| 149 | 0x95 | Packet Too Large | Packet exceeds maximum size | CONNACK, DISCONNECT |
| 150 | 0x96 | Message Rate Too High | Rate limit shaper triggered | DISCONNECT |
| 151 | 0x97 | Quota Exceeded | Internal resource exhausted | All Acks |

---

## 5. Architectural Enhancements over MQTT 3.1.1

1. **Explicit Error Reporting:** Every acknowledgment packet (PUBACK, SUBACK, UNSUBACK) returns granular reason codes instead of simple success/failure flags.
2. **User Properties:** Arbitrary key-value metadata pairs can accompany any packet without modifying payload formats.
3. **Flow Control:** Clients and brokers independently advertise `ReceiveMaximum` to bound in-flight QoS 1 and 2 windows, preventing buffer bloat.
4. **Bandwidth Optimization:** The Topic Alias mechanism replaces lengthy topic strings with a 2-byte integer after initial transmission.
5. **Session Expiry Decoupling:** Session state lifetime is configured via `SessionExpiryInterval` independently from TCP connection duration.
