"""
Analytics Data Schema

Defines the structure of lap-level analytics output.
"""

from dataclasses import dataclass, asdict
from typing import Optional
import json


@dataclass
class LapAnalytics:
    """
    Aggregated lap-level analytics computed by PySpark.
    """
    # Lap Identification
    lap_count: int
    
    # Speed Metrics (km/h)
    avg_speed_kmh: float
    max_speed_kmh: float
    min_speed_kmh: float
    
    # Pedal Inputs (0.0 - 1.0)
    avg_throttle: float
    avg_brake: float
    throttle_pct: Optional[float] = None  # % of lap with throttle > 0.5
    
    # G-Forces
    max_g_lat: float
    max_g_long: float  # Max acceleration
    min_g_long: float  # Max braking (negative)
    
    # Fuel & Tires
    fuel_start_kg: float
    fuel_end_kg: float
    fuel_used_kg: float
    tire_wear_pct: float
    
    # Lap Timing
    lap_time_s: float
    last_lap_time_s: float
    
    # Metadata
    sample_count: int
    lap_start_timestamp: int
    lap_end_timestamp: int
    
    # Derived Metrics (computed post-aggregation)
    consistency_score: Optional[float] = None  # Lower is better (std dev of speed)
    braking_zones_count: Optional[int] = None
    
    def to_json(self) -> str:
        """Serialize to JSON string."""
        return json.dumps(asdict(self))
    
    @staticmethod
    def from_dict(data: dict) -> 'LapAnalytics':
        """Create from dictionary."""
        return LapAnalytics(**data)
    
    @staticmethod
    def from_json(json_str: str) -> 'LapAnalytics':
        """Create from JSON string."""
        return LapAnalytics.from_dict(json.loads(json_str))
    
    def __str__(self) -> str:
        """Human-readable representation."""
        return (
            f"Lap {self.lap_count}: "
            f"Avg Speed={self.avg_speed_kmh:.1f} km/h, "
            f"Fuel Used={self.fuel_used_kg:.2f} kg, "
            f"Samples={self.sample_count}"
        )
