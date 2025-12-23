import numpy as np
from simulator.framework import Sensor

class Speedometer(Sensor):
    def __init__(self, frequency=100.0, noise_std=0.5):
        super().__init__("Speedometer", frequency)
        self.noise_std = noise_std

    def measure(self, truth):
        # 1. Get Ground Truth
        speed_ms = truth.speed
        speed_kmh = speed_ms * 3.6
        
        # 2. Add Noise
        noisy_speed = speed_kmh + np.random.normal(0, self.noise_std)
        
        return {
            "speed_kmh": max(0, noisy_speed)
        }

class GPSSensor(Sensor):
    def __init__(self, frequency=10.0, position_error=0.2):
        # GPS usually updates slower (10-20Hz)
        super().__init__("GPS", frequency) 
        self.error = position_error

    def measure(self, truth):
        # Add slight position jitter
        x_jit = truth.x + np.random.normal(0, self.error)
        y_jit = truth.y + np.random.normal(0, self.error)
        
        return {
            "x": x_jit,
            "y": y_jit,
            "sector_name": truth.sector_name, # GPS derived
            "lap_count": truth.lap_count
        }

class Accelerometer(Sensor):
    def __init__(self, frequency=100.0, noise_std=0.05):
        super().__init__("Accelerometer", frequency)
        self.noise_std = noise_std

    def measure(self, truth):
        # G-Force Jitter
        g_lat = truth.g_lat + np.random.normal(0, self.noise_std)
        g_long = truth.g_long + np.random.normal(0, self.noise_std)
        
        return {
            "g_lat": g_lat,
            "g_long": g_long
        }

class EngineMonitor(Sensor):
    def __init__(self, frequency=50.0):
        super().__init__("ECU", frequency)

    def measure(self, truth):
        # RPM Noise
        rpm_noise = int(np.random.normal(0, 50))
        
        return {
            "rpm": max(0, truth.rpm + rpm_noise),
            "gear": truth.gear,
            "throttle": truth.current_throttle,
            "brake": truth.current_brake,
            "fuel_kg": truth.fuel,
            "tire_wear_pct": (1.0 - truth.mu_eff/1.1) * 100 # Derived for debug
        }

class TimingTransponder(Sensor):
    def __init__(self):
        super().__init__("Timing", frequency=100.0)
        
    def measure(self, truth):
        return {
            "lap_time": truth.lap_time,
            "last_lap_time": truth.last_lap_time,
            "time": truth.time,
            "timestamp": truth.timestamp
        }
