import numpy as np
import json
import os
from scipy.interpolate import splprep, splev

class GeoJSONTrack:
    def __init__(self, geojson_path):
        if not os.path.exists(geojson_path):
            raise FileNotFoundError(f"GeoJSON file not found: {geojson_path}")
            
        with open(geojson_path, 'r') as f:
            data = json.load(f)
            
        # Extract LineString coordinates
        # Assumes the first feature is the track
        feature = data['features'][0]
        coords = feature['geometry']['coordinates']
        self.name = feature['properties'].get('Name', 'Unknown Track')
        
        # Convert List to Arrays [Lon, Lat]
        # GeoJSON is usually [Lon, Lat]
        trajectory_lonlat = np.array(coords) 
        lon = trajectory_lonlat[:, 0]
        lat = trajectory_lonlat[:, 1]
        
        # Project to Cartesian Meters (Simple Equirectangular approximation)
        # Center point for projection
        center_lon = np.mean(lon)
        center_lat = np.mean(lat)
        
        # Meters per degree
        # Lat: 111,132 meters
        # Lon: 111,132 * cos(lat)
        R_EARTH = 6378137.0
        lat_rad = np.deg2rad(center_lat)
        
        x_meters = (lon - center_lon) * (np.pi/180) * R_EARTH * np.cos(lat_rad)
        y_meters = (lat - center_lat) * (np.pi/180) * R_EARTH
        
        self.waypoints = np.column_stack((x_meters, y_meters))
        
        # High-res interpolation
        # Using periodic spline (per=True) for closed loop
        tck, u = splprep(self.waypoints.T, s=0, per=True)
        
        # 20,000 points for high-fidelity physics
        self.u_new = np.linspace(u.min(), u.max(), 20000) 
        x_norm, y_norm = splev(self.u_new, tck, der=0)
        
        self.x = x_norm
        self.y = y_norm
        
        # Calculate Gradients
        dx = np.gradient(self.x)
        dy = np.gradient(self.y)
        ddx = np.gradient(dx)
        ddy = np.gradient(dy)
        
        # Curvature
        self.curvature = np.abs(dx * ddy - dy * ddx) / np.power(dx**2 + dy**2, 1.5)
        self.curvature[np.isnan(self.curvature)] = 0.0
        
        # Distances and Tangents
        self.dists = np.sqrt(dx**2 + dy**2)
        self.total_length = np.sum(self.dists)
        self.total_points = len(self.x)
        
        self.tangent_x = dx / self.dists
        self.tangent_y = dy / self.dists
        
        # Generate Map Names (Simple Division for now)
        self.map_names = self._generate_sector_names()
        
        print(f"Loaded Track: {self.name}")
        print(f"Length: {self.total_length:.2f}m | Points: {self.total_points}")

    def _generate_sector_names(self):
        # Divide into 3 sectors
        names = ["Sector 1"] * self.total_points
        s2_start = int(self.total_points * 0.33)
        s3_start = int(self.total_points * 0.66)
        
        for i in range(s2_start, s3_start):
            names[i] = "Sector 2"
        for i in range(s3_start, self.total_points):
            names[i] = "Sector 3"
            
        return names

# For backward compatibility if needed, or we just replace SilverstoneTrack usage
class SilverstoneTrack(GeoJSONTrack):
    def __init__(self):
        # Point detailed path to the specific geojson
        # NOTE: Adjust path as necessary
        base_dir = os.path.dirname(os.path.abspath(__file__))
        path = os.path.join(base_dir, "tracks", "gb-1948.geojson")
        super().__init__(path)
