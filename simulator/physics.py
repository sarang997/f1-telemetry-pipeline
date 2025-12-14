import numpy as np
import pandas as pd
import sys
import time
import os
# Add parent directory to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from shared.schema import TelemetryData

# Physics Constants (Ideally could be in config, but keeping physics-specific here)
MASS = 798.0         # kg
P_ENGINE = 740000.0  # Watts
RHO = 1.225          # Air Density
CD = 0.9             # Drag Coeff
CL = 1.2             # Lift Coeff
AREA = 1.6           # Frontal Area
MU_MECH = 1.1        # Mech Grip
G = 9.81             # Gravity
BRAKE_FORCE_MAX = 4.0 * G * MASS 

class F1Sim:
    def __init__(self, track):
        self.track = track
        self.n = len(track.x)
        
        # State
        self.idx = 0
        self.speed = 0.0 # m/s
        self.time = 0.0
        self.lap_count = 0
        self.lap_time = 0.0
        self.last_lap_time = 0.0
        
        # Driver State (Smoothed Inputs)
        self.current_throttle = 0.0
        self.current_brake = 0.0
        
        # Pre-calculate speed profile
        self.target_speeds = self._calculate_target_speed_profile()

    def _calculate_max_corner_speed(self, curvature):
        if curvature < 1e-5:
            return 360.0 / 3.6 
        
        denom = curvature - (MU_MECH * RHO * CL * AREA) / (2 * MASS)
        if denom <= 0:
            return 360.0 / 3.6
            
        v_sq = (MU_MECH * G) / denom
        return np.sqrt(v_sq)

    def _calculate_target_speed_profile(self):
        # Backward pass
        max_speeds = np.array([self._calculate_max_corner_speed(k) for k in self.track.curvature])
        
        for _ in range(2):
            for i in range(self.n - 2, -1, -1):
                dist = self.track.dists[i]
                v_next = max_speeds[i+1]
                a_brake = 4.0 * G 
                v_allowable = np.sqrt(v_next**2 + 2 * a_brake * dist)
                max_speeds[i] = min(max_speeds[i], v_allowable)
            max_speeds[-1] = min(max_speeds[-1], max_speeds[0])

        return max_speeds

    def step(self, dt):
        """Advances the simulation by dt seconds. Returns current state dict."""
        
        current_x = self.track.x[self.idx]
        current_y = self.track.y[self.idx]
        curvature = self.track.curvature[self.idx]
        target_v = self.target_speeds[self.idx]
        
        # --- PHYSICS STEP ---
        
        # 1. Aerodynamics
        drag_force = 0.5 * RHO * self.speed**2 * CD * AREA
        downforce = 0.5 * RHO * self.speed**2 * CL * AREA
        
        # 2. Driver Input Logic (PID-ish)
        # Determine Target
        target_throttle = 0.0
        target_brake = 0.0
        
        if self.speed < target_v:
            target_throttle = 1.0
        else:
            target_brake = 1.0

        # Apply Smoothing (Simulate Foot Speed)
        # Rate: 5.0 units/sec = 0.2s for full press
        INPUT_RATE = 5.0 * dt
        
        # Throttle
        if self.current_throttle < target_throttle:
            self.current_throttle = min(target_throttle, self.current_throttle + INPUT_RATE)
        else:
            self.current_throttle = max(target_throttle, self.current_throttle - INPUT_RATE)

        # Brake
        if self.current_brake < target_brake:
            self.current_brake = min(target_brake, self.current_brake + INPUT_RATE)
        else:
            self.current_brake = max(target_brake, self.current_brake - INPUT_RATE)

        # 3. Calculate Forces
        engine_force = 0.0
        brake_force = 0.0
        
        if self.speed > 1.0:
            engine_force = (min(P_ENGINE / self.speed, MASS * G * 1.5)) * self.current_throttle
        else:
            engine_force = (MASS * G * 1.0) * self.current_throttle
            
        max_grip_force = (MASS * G + downforce) * MU_MECH
        brake_force = min(BRAKE_FORCE_MAX, max_grip_force) * self.current_brake
        
        # 3. Net Force
        f_long = engine_force - brake_force - drag_force
        accel_long = f_long / MASS
        
        # 4. Integrate Speed
        self.speed += accel_long * dt
        if self.speed < 0: self.speed = 0
        
        # 5. Integrate Position
        dist_step = self.speed * dt
        
        dist_accum = 0.0
        while dist_accum < dist_step:
            dist_accum += self.track.dists[self.idx]
            self.idx += 1
            if self.idx >= self.n:
                self.last_lap_time = self.lap_time # Capture time before reset
                self.idx = 0
                self.lap_count += 1
                self.lap_time = 0.0
        
        self.lap_time += dt
        self.time += dt
        
        # 6. G-Forces
        accel_lat = (self.speed**2) * curvature
        g_lat = accel_lat / G
        g_long = accel_long / G
        
        # 7. Gear & RPM
        speed_kmh = self.speed * 3.6
        gear = 1
        if speed_kmh > 310: gear = 8
        elif speed_kmh > 270: gear = 7
        elif speed_kmh > 240: gear = 6
        elif speed_kmh > 200: gear = 5
        elif speed_kmh > 160: gear = 4
        elif speed_kmh > 120: gear = 3
        elif speed_kmh > 80: gear = 2
        
        rpm = 10500 + (self.speed % 10) * 150
        
        return TelemetryData(
            time=self.time,
            x=current_x,
            y=current_y,
            speed_kmh=speed_kmh,
            throttle=self.current_throttle,
            brake=self.current_brake,
            gear=gear,
            g_lat=g_lat,
            g_long=g_long,
            rpm=int(rpm),
            lap_count=self.lap_count,
            lap_time=self.lap_time,
            last_lap_time=getattr(self, 'last_lap_time', 0.0),
            sector_name=self.track.map_names[self.idx],
            timestamp=int(time.time_ns())
        )
