import numpy as np
import pandas as pd
import sys
import time
import os
# Add parent directory to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from shared.schema import TelemetryData

# --- PHYSICS CONSTANTS ---
BASE_MASS = 798.0         # kg (Car + Driver - Fuel)
FUEL_MASS_START = 110.0   # kg (Max Fuel)
P_ENGINE = 740000.0       # Watts (~1000 HP)
RHO = 1.225               # Air Density
CD_BASE = 0.9             # Drag Coeff
CD_DRS = 0.7              # Drag Coeff with DRS
CL = 1.2                  # Lift Coeff
AREA = 1.6                # Frontal Area
MU_BASE = 1.1             # Base Mech Grip
TIRE_WEAR_FACTOR = 1e-6   # Grip loss per meter
G = 9.81

class F1Sim:
    def __init__(self, track):
        self.track = track
        self.n = len(track.x)
        
        # Simulation State
        self.idx = 0
        self.speed = 0.0 # m/s (Current Speed)
        self.time = 0.0
        self.lap_count = 0
        self.lap_time = 0.0
        self.last_lap_time = 0.0
        
        # Vehicle State
        self.fuel = FUEL_MASS_START
        self.tire_life_dist = 0.0
        self.drs_active = False
        
        # Driver Smoothed Inputs
        self.current_throttle = 0.0
        self.current_brake = 0.0
        
        # Initialize Profile
        self.target_speeds = self._calculate_ideal_velocity_profile()

    @property
    def total_mass(self):
        return BASE_MASS + self.fuel
        
    @property
    def mu_eff(self):
        # Grip decays linearly with distance driven
        return max(0.5, MU_BASE - (self.tire_life_dist * TIRE_WEAR_FACTOR))

    def _calculate_max_corner_speed(self, curvature):
        if curvature < 1e-5:
            return 360.0 / 3.6 
        
        # Assuming avg fuel for static profile
        avg_mass = BASE_MASS + (FUEL_MASS_START / 2)
        denom = curvature - (self.mu_eff * RHO * CL * AREA) / (2 * avg_mass)
        
        if denom <= 0: return 360.0 / 3.6
        v_sq = (self.mu_eff * G) / denom
        return np.sqrt(v_sq)

    def _calculate_ideal_velocity_profile(self):
        # 1. Cornering Limits
        max_speeds = np.array([self._calculate_max_corner_speed(k) for k in self.track.curvature])
        
        # 2. Braking Zones (Backward Pass)
        for _ in range(2): # Two passes for coverage
            for i in range(self.n - 2, -1, -1):
                dist = self.track.dists[i]
                v_next = max_speeds[i+1]
                # Conservative Braking
                a_brake = 4.0 * G 
                v_allowable = np.sqrt(v_next**2 + 2 * a_brake * dist)
                max_speeds[i] = min(max_speeds[i], v_allowable)
            # Link last point to first
            max_speeds[-1] = min(max_speeds[-1], max_speeds[0])

        return max_speeds

    def step(self, dt):
        self.x = self.track.x[self.idx]
        self.y = self.track.y[self.idx]
        curvature = self.track.curvature[self.idx]
        target_v = self.target_speeds[self.idx]
        
        # --- LOGIC: DRS ZONES ---
        # Very simple logic: DRS allowed if low curvature (straight) and lap > 0
        is_straight = curvature < 0.0005
        self.drs_active = (self.lap_count > 0) and is_straight and (self.speed * 3.6 > 100)
        
        current_cd = CD_DRS if self.drs_active else CD_BASE

        # --- PHYSICS: FORCES ---
        
        # 1. Aero
        drag_force = 0.5 * RHO * self.speed**2 * current_cd * AREA
        downforce = 0.5 * RHO * self.speed**2 * CL * AREA
        
        # 2. Driver Input Sim
        target_throttle = 0.0
        target_brake = 0.0
        
        if self.speed < (target_v * 0.98): # Accelerate
            target_throttle = 1.0
        elif self.speed > (target_v * 1.02): # Brake
            target_brake = 1.0
        
        # Smooth Inputs
        INPUT_RATE = 5.0 * dt
        self.current_throttle += np.clip(target_throttle - self.current_throttle, -INPUT_RATE, INPUT_RATE)
        self.current_brake += np.clip(target_brake - self.current_brake, -INPUT_RATE, INPUT_RATE)
        
        # 3. Longitudinal Forces
        # Engine Power limited by grip at low speed
        engine_force = 0.0
        if self.speed > 1.0:
            power_available = min(P_ENGINE / self.speed, self.total_mass * G * 1.5)
            engine_force = power_available * self.current_throttle
        else:
            engine_force = (self.total_mass * G * 0.8) * self.current_throttle
            
        max_grip_force = (self.total_mass * G + downforce) * self.mu_eff
        brake_force = min(4.0 * G * self.total_mass, max_grip_force) * self.current_brake
        
        net_force = engine_force - brake_force - drag_force
        accel_long = net_force / self.total_mass
        
        # --- STATE UPDATE ---
        
        # Speed Integration
        self.speed += accel_long * dt
        if self.speed < 0: self.speed = 0
        
        # Position Integration
        dist_step = self.speed * dt
        self.tire_life_dist += dist_step
        
        # Fuel Burn (Approx 0.002 kg/sec at full throttle)
        self.fuel = max(0.0, self.fuel - (0.002 * self.current_throttle * dt * (self.speed/100 + 1)))

        dist_accum = 0.0
        while dist_accum < dist_step:
            dist_accum += self.track.dists[self.idx]
            self.idx += 1
            if self.idx >= self.n:
                self.last_lap_time = self.lap_time
                self.idx = 0
                self.lap_count += 1
                self.lap_time = 0.0
                print(f"[SIM] Lap {self.lap_count} | Fuel: {self.fuel:.1f}kg | Grip: {self.mu_eff:.3f}")

        self.lap_time += dt
        self.time += dt
        
        # Calculated Outputs
        accel_lat = (self.speed**2) * curvature
        self.g_lat = accel_lat / G
        self.g_long = accel_long / G
        
        # Gear / RPM Logic
        speed_kmh = self.speed * 3.6
        self.gear = 1
        gears = [0, 80, 120, 160, 200, 240, 270, 310, 1000]
        for i in range(1, 9):
            if speed_kmh < gears[i]:
                self.gear = i
                break
        
        # Base RPM (Sensor adds noise later)
        self.rpm = 10000 + (self.speed % 15) * 200
        self.sector_name = self.track.map_names[self.idx]
        self.timestamp = int(time.time_ns())
        
        return self
