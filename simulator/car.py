from simulator.framework import SensorManager
from simulator.physics import F1Sim
from simulator.sensors import Speedometer, GPSSensor, Accelerometer, EngineMonitor, TimingTransponder

class SmartCar:
    """
    Simulated F1 Car equipped with a configurable sensor array.
    """
    def __init__(self, track):
        # 1. The Physics Engine (Ground Truth)
        self.sim = F1Sim(track)
        
        # 2. The Sensor Array
        self.sensors = SensorManager()
        
        # 3. Default Loadout
        self.add_sensor(Speedometer())
        self.add_sensor(GPSSensor())
        self.add_sensor(Accelerometer())
        self.add_sensor(EngineMonitor())
        self.add_sensor(TimingTransponder())

    def add_sensor(self, sensor):
        self.sensors.add_sensor(sensor)

    def update(self, dt):
        """
        Advance physics by dt, then read all sensors.
        Returns the aggregated telemetry dictionary.
        """
        # Step Physics
        # Note: sim.step() updates internal state in-place
        self.sim.step(dt)
        
        # Read Sensors
        sensor_data = self.sensors.read_all(truth=self.sim, current_time=self.sim.time)
        
        return sensor_data
