from abc import ABC, abstractmethod
from typing import Dict, Any, List
import time

class Sensor(ABC):
    """
    Abstract base class for all vehicle sensors.
    """
    def __init__(self, name: str, frequency: float = 100.0):
        self.name = name
        self.frequency = frequency
        self._last_update_time = 0.0
        self._update_interval = 1.0 / frequency

    def should_update(self, current_time: float) -> bool:
        """Check if enough time has passed to update this sensor."""
        return (current_time - self._last_update_time) >= self._update_interval

    @abstractmethod
    def measure(self, truth: Any) -> Dict[str, Any]:
        """
        Produce a measurement based on the ground truth state.
        Must return a dictionary of fields to add to the telemetry payload.
        """
        pass

    def update(self, truth: Any, current_time: float) -> Dict[str, Any]:
        """Called by the manager. Handles frequency logic."""
        if self.should_update(current_time):
            self._last_update_time = current_time
            return self.measure(truth)
        return {}


class SensorManager:
    """
    Manages a collection of sensors and aggregates their outputs.
    """
    def __init__(self):
        self.sensors: List[Sensor] = []
        self.state_buffer: Dict[str, Any] = {}

    def add_sensor(self, sensor: Sensor):
        self.sensors.append(sensor)
        print(f"[SensorManager] Registered: {sensor.name} ({sensor.frequency}Hz)")

    def read_all(self, truth: Any, current_time: float) -> Dict[str, Any]:
        """
        Aggregates data from all sensors.
        Maintains a state buffer to valid sending full packets even if some sensors didn't update.
        """
        updates_this_tick = {}
        for sensor in self.sensors:
            data = sensor.update(truth, current_time)
            if data:
                updates_this_tick.update(data)
        
        # Merge new data into the persistent state buffer
        self.state_buffer.update(updates_this_tick)
        
        # Return a copy of the full state
        # Note: If this is the very first tick and some sensors haven't fired,
        # keys might still be missing, but standard sensors usually fire at T=0.
        return self.state_buffer.copy()
