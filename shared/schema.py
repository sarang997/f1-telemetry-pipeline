from dataclasses import dataclass
import json

@dataclass
class TelemetryData:
    time: float
    x: float
    y: float
    speed_kmh: float
    throttle: float
    brake: float
    gear: int
    g_lat: float
    g_long: float
    rpm: int
    lap_count: int
    lap_time: float
    last_lap_time: float
    sector_name: str
    timestamp: int

    def to_json(self):
        return json.dumps(self.__dict__)

    @staticmethod
    def from_dict(data: dict):
        return TelemetryData(**data)
