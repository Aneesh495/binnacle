"""
Binnacle Python SDK
High-performance MQTT 5.0 client and telemetry simulation framework.
"""

from .client import BinnacleClient
from .protocol import PacketType, QoS, MqttPacket

__version__ = "1.0.0"
__all__ = ["BinnacleClient", "PacketType", "QoS", "MqttPacket"]
