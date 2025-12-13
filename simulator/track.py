import numpy as np
from scipy.interpolate import splprep, splev

class SilverstoneTrack:
    def __init__(self):
        # Coordinates from visualizer.py
        self.waypoints = np.array([
            [0, 0], [10, 2], [15, 5], [12, 8], [8, 6], [5, 4], [20, 4],
            [35, 6], [38, 4], [40, 0], [60, 0], [80, 5], [85, 3], [90, 6],
            [120, 6], [130, 0], [110, -5], [100, -2], [0, 0]
        ])
        
        # High-res interpolation
        tck, u = splprep(self.waypoints.T, s=0, per=True)
        # Use more points for finer physics steps
        self.u_new = np.linspace(u.min(), u.max(), 20000) 
        x_norm, y_norm = splev(self.u_new, tck, der=0)
        
        # Scale to real world length (5891m)
        # Calculate current normalized length
        dx_n = np.gradient(x_norm)
        dy_n = np.gradient(y_norm)
        dist_n = np.sum(np.sqrt(dx_n**2 + dy_n**2))
        
        scale = 5891.0 / dist_n
        self.x = x_norm * scale
        self.y = y_norm * scale
        
        # Gradients for direction and curvature (on scaled coordinates)
        dx = np.gradient(self.x)
        dy = np.gradient(self.y)
        ddx = np.gradient(dx)
        ddy = np.gradient(dy)
        
        # Radius of curvature: R = (x'b^2 + y'^2)^1.5 / |x'y'' - y'x''|
        self.curvature = np.abs(dx * ddy - dy * ddx) / np.power(dx**2 + dy**2, 1.5)
        # Handle straight lines (infinite radius -> 0 curvature)
        self.curvature[np.isnan(self.curvature)] = 0.0
        
        # Segment lengths
        self.dists = np.sqrt(dx**2 + dy**2)
        self.total_length = np.sum(self.dists)
        self.total_points = len(self.x)
        
        # Tangent vectors (normalized)
        self.tangent_x = dx / self.dists
        self.tangent_y = dy / self.dists
        
        # Map Names
        self.map_names = self._generate_corner_names()
        
        print(f"Track Loaded. Length: {self.total_length:.2f}m Points: {len(self.x)}")

    def _generate_corner_names(self):
        # Map percentage of track distance to corner names
        names = ["Hamilton Straight"] * self.total_points
        
        def set_zone(start_pct, end_pct, name):
            start = int(start_pct * self.total_points)
            end = int(end_pct * self.total_points)
            for i in range(start, end):
                names[i] = name

        set_zone(0.05, 0.10, "Abbey")
        set_zone(0.12, 0.18, "Village / The Loop")
        set_zone(0.20, 0.30, "Wellington Straight")
        set_zone(0.32, 0.38, "Brooklands / Luffield")
        set_zone(0.40, 0.50, "National Pits Straight")
        set_zone(0.52, 0.56, "Copse Corner")
        set_zone(0.58, 0.65, "Maggotts / Becketts")
        set_zone(0.66, 0.78, "Hangar Straight")
        set_zone(0.80, 0.85, "Stowe")
        set_zone(0.88, 0.92, "Vale / Club")
        
        return names
