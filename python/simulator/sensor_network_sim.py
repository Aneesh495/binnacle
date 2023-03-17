"""
Binnacle Industrial Sensor Network Simulator
Simulates a multi-facility manufacturing edge environment streaming sensor feeds.
"""

import argparse
import json
import random
import time
from typing import List
from binnacle_client import BinnacleClient


class IndustrialSensor:
    def __init__(self, facility: str, cell: str, sensor_id: str, sensor_type: str):
        self.facility = facility
        self.cell = cell
        self.sensor_id = sensor_id
        self.sensor_type = sensor_type
        self.base_val = 20.0 if sensor_type == "temp" else (100.0 if sensor_type == "vibration" else 5.0)

    def read(self) -> dict:
        noise = random.uniform(-1.0, 1.0)
        val = self.base_val + noise
        return {
            "facility": self.facility,
            "cell": self.cell,
            "sensor_id": self.sensor_id,
            "type": self.sensor_type,
            "value": round(val, 3),
            "timestamp_ns": int(time.time() * 1e9),
        }


def run_sensor_simulator(host: str, port: int, num_sensors: int, duration_s: int) -> None:
    print(f"[SensorNet] Connecting to {host}:{port}...")
    client = BinnacleClient(client_id="industrial-sensor-gateway-01", host=host, port=port)
    try:
        client.connect()
    except Exception as e:
        print(f"[SensorNet] Connection error: {e}")
        return

    facilities = ["facility-na-01", "facility-eu-02"]
    cells = ["press-line-a", "assembly-cell-b", "paint-booth-c"]
    types = ["temp", "vibration", "pressure"]

    sensors: List[IndustrialSensor] = []
    for i in range(num_sensors):
        f = facilities[i % len(facilities)]
        c = cells[i % len(cells)]
        st = types[i % len(types)]
        sensors.append(IndustrialSensor(f, c, f"sen-{i:04d}", st))

    start = time.time()
    count = 0
    print(f"[SensorNet] Streaming from {len(sensors)} sensors...")

    try:
        while duration_s <= 0 or (time.time() - start) < duration_s:
            for s in sensors:
                data = s.read()
                topic = f"sensors/{data['facility']}/{data['cell']}/{data['type']}/{data['sensor_id']}"
                client.publish(topic, json.dumps(data).encode("utf-8"), qos=0)
                count += 1
            time.sleep(0.05)
    except KeyboardInterrupt:
        pass
    finally:
        elapsed = time.time() - start
        print(f"[SensorNet] Done: sent {count} samples in {elapsed:.2f}s ({count / max(0.01, elapsed):.1f} msg/sec)")
        client.disconnect()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Industrial Sensor Network Simulator")
    parser.add_argument("--host", default="127.0.0.1", help="Broker host")
    parser.add_argument("--port", type=int, default=1883, help="Broker port")
    parser.add_argument("--sensors", type=int, default=30, help="Number of sensors")
    parser.add_argument("--duration", type=int, default=5, help="Duration in seconds")
    args = parser.parse_args()

    run_sensor_simulator(args.host, args.port, args.sensors, args.duration)
