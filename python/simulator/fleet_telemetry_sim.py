"""
Binnacle Fleet Telemetry Simulator
Simulates a high-scale fleet of connected vehicles streaming real-time sensor telematics.
"""

import argparse
import json
import random
import sys
import time
import math
from typing import List
from binnacle_client import BinnacleClient


class VehicleState:
    def __init__(self, vin: str, base_lat: float, base_lon: float):
        self.vin = vin
        self.lat = base_lat
        self.lon = base_lon
        self.speed_kmh = random.uniform(30.0, 95.0)
        self.battery_soc = random.uniform(50.0, 98.0)
        self.odometer_km = random.uniform(1000.0, 50000.0)
        self.motor_temp_c = random.uniform(40.0, 65.0)
        self.tire_pressure_psi = [32.0, 32.1, 31.9, 32.0]
        self.heading = random.uniform(0.0, 360.0)

    def step(self, dt_seconds: float) -> dict:
        # Move along heading
        rad = math.radians(self.heading)
        dist_km = (self.speed_kmh / 3600.0) * dt_seconds
        self.lat += (dist_km / 111.0) * math.cos(rad)
        self.lon += (dist_km / (111.0 * math.cos(math.radians(self.lat)))) * math.sin(rad)

        # Fluctuate speed and battery
        self.speed_kmh = max(0.0, min(140.0, self.speed_kmh + random.uniform(-2.0, 2.0)))
        self.battery_soc = max(5.0, self.battery_soc - 0.001 * dt_seconds)
        self.odometer_km += dist_km
        self.motor_temp_c += random.uniform(-0.1, 0.15)

        # Random mild steering change
        self.heading = (self.heading + random.uniform(-5.0, 5.0)) % 360.0

        return {
            "vin": self.vin,
            "timestamp": int(time.time() * 1000),
            "location": {"lat": round(self.lat, 6), "lon": round(self.lon, 6)},
            "telemetry": {
                "speed_kmh": round(self.speed_kmh, 1),
                "battery_soc": round(self.battery_soc, 2),
                "odometer_km": round(self.odometer_km, 2),
                "motor_temp_c": round(self.motor_temp_c, 1),
                "tire_pressure_psi": [round(p, 1) for p in self.tire_pressure_psi],
                "heading_deg": round(self.heading, 1),
            },
        }


def run_fleet_simulator(
    broker_host: str,
    broker_port: int,
    fleet_size: int,
    interval_ms: int,
    duration_s: int,
) -> None:
    print(f"[FleetSimulator] Initializing connection to {broker_host}:{broker_port}...")
    client = BinnacleClient(
        client_id="binnacle-fleet-sim-01",
        host=broker_host,
        port=broker_port,
        keep_alive=60,
    )

    try:
        client.connect()
    except Exception as e:
        print(f"[FleetSimulator] Connection failed: {e}")
        return

    print(f"[FleetSimulator] Connected. Spawning fleet of {fleet_size} vehicles...")
    fleet: List[VehicleState] = []
    base_lat = 37.7749
    base_lon = -122.4194

    for i in range(fleet_size):
        vin = f"1HGCR2F8{i:05d}"
        fleet.append(
            VehicleState(
                vin,
                base_lat + random.uniform(-0.2, 0.2),
                base_lon + random.uniform(-0.2, 0.2),
            )
        )

    start_time = time.time()
    msg_count = 0
    dt = interval_ms / 1000.0

    try:
        while duration_s <= 0 or (time.time() - start_time) < duration_s:
            cycle_start = time.time()
            for v in fleet:
                sample = v.step(dt)
                topic = f"fleet/{v.vin}/telematics"
                payload = json.dumps(sample).encode("utf-8")
                client.publish(topic, payload, qos=0)
                msg_count += 1

                # Rare alert condition
                if sample["telemetry"]["motor_temp_c"] > 64.0:
                    alert_topic = f"fleet/{v.vin}/alerts"
                    alert_payload = json.dumps(
                        {
                            "vin": v.vin,
                            "type": "THERMAL_WARNING",
                            "temp_c": sample["telemetry"]["motor_temp_c"],
                            "timestamp": sample["timestamp"],
                        }
                    ).encode("utf-8")
                    client.publish(alert_topic, alert_payload, qos=1)
                    msg_count += 1

            elapsed = time.time() - cycle_start
            sleep_time = max(0.0, dt - elapsed)
            time.sleep(sleep_time)

            if msg_count % (fleet_size * 5) == 0:
                rate = msg_count / (time.time() - start_time)
                print(f"[FleetSimulator] Published {msg_count} messages ({rate:.1f} msg/sec)")

    except KeyboardInterrupt:
        print("\n[FleetSimulator] Interrupted by user.")
    finally:
        total_time = time.time() - start_time
        print(
            f"[FleetSimulator] Complete: {msg_count} messages in {total_time:.2f}s "
            f"({msg_count / max(0.001, total_time):.1f} msg/sec)"
        )
        client.disconnect()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Binnacle Fleet Telemetry Simulator")
    parser.add_argument("--host", default="127.0.0.1", help="Broker host")
    parser.add_argument("--port", type=int, default=1883, help="Broker port")
    parser.add_argument("--fleet-size", type=int, default=25, help="Number of vehicles")
    parser.add_argument("--interval", type=int, default=200, help="Interval in ms")
    parser.add_argument("--duration", type=int, default=10, help="Duration in seconds (0 for loop)")
    args = parser.parse_args()

    run_fleet_simulator(
        broker_host=args.host,
        broker_port=args.port,
        fleet_size=args.fleet_size,
        interval_ms=args.interval,
        duration_s=args.duration,
    )
