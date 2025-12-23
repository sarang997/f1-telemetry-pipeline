import sys
import os
import json
import numpy as np
import pyqtgraph as pg
from PySide6 import QtCore, QtWidgets, QtGui
from kafka import KafkaConsumer
from scipy.spatial import cKDTree

# Add parent directory to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC, KAFKA_API_VERSION, VISUALIZATION_FPS, VIZ_GROUP_ID
from simulator.track import SilverstoneTrack

class DashboardApp(QtWidgets.QMainWindow):
    def __init__(self):
        super().__init__()
        
        # Load Track
        self.track = SilverstoneTrack()
        self.TRACK_POINTS = len(self.track.x)
        self.tree = cKDTree(np.column_stack((self.track.x, self.track.y)))
        
        # Data Buffers
        self.x_dist = np.linspace(0, self.track.total_length, self.TRACK_POINTS)
        self.lap_speed = np.full(self.TRACK_POINTS, np.nan)
        self.ghost_speed = np.full(self.TRACK_POINTS, np.nan)
        self.lap_throttle = np.full(self.TRACK_POINTS, np.nan)
        self.lap_brake = np.full(self.TRACK_POINTS, np.nan)
        
        self.current_lap_num = -1
        self.last_idx = -1
        
        # Kafka Setup
        try:
            self.consumer = KafkaConsumer(
                KAFKA_TOPIC,
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                value_deserializer=lambda x: json.loads(x.decode('utf-8')),
                auto_offset_reset='latest',
                group_id=VIZ_GROUP_ID,
                api_version=KAFKA_API_VERSION
            )
            print(f"Connected to Kafka at {KAFKA_BOOTSTRAP_SERVERS}")
        except Exception as e:
            print(f"Kafka Error: {e}")
            self.consumer = None

        self.init_ui()
        
        # Timer for updates
        self.timer = QtCore.QTimer()
        self.timer.timeout.connect(self.update)
        self.timer.start(int(1000 / VISUALIZATION_FPS))

    def init_ui(self):
        self.setWindowTitle("F1 Telemetry Dashboard (PyQtGraph)")
        self.resize(1200, 900)
        self.setStyleSheet("background-color: #0d0d0d; color: #ffffff;")
        
        central_widget = QtWidgets.QWidget()
        self.setCentralWidget(central_widget)
        main_layout = QtWidgets.QVBoxLayout(central_widget)
        
        # Top Row: Track and Metrics
        top_layout = QtWidgets.QHBoxLayout()
        main_layout.addLayout(top_layout, stretch=3)
        
        # 1. Track Map
        self.track_widget = pg.PlotWidget(title="TRACK MAP")
        self.track_widget.setAspectLocked(True)
        self.track_widget.showAxis('left', False)
        self.track_widget.showAxis('bottom', False)
        self.track_widget.setBackground('#0d0d0d')
        
        # Plot Track
        self.track_line = pg.PlotCurveItem(self.track.x, self.track.y, pen=pg.mkPen('#333333', width=10))
        self.track_widget.addItem(self.track_line)
        self.track_center_line = pg.PlotCurveItem(self.track.x, self.track.y, pen=pg.mkPen('#00aaff', width=1, style=QtCore.Qt.DashLine))
        self.track_widget.addItem(self.track_center_line)
        
        # Car Dot
        self.car_dot = pg.ScatterPlotItem(size=15, brush=pg.mkBrush('#ff0000'), pen=pg.mkPen('w', width=1))
        self.track_widget.addItem(self.car_dot)
        
        top_layout.addWidget(self.track_widget, stretch=2)
        
        # 2. Metrics (Timing & G-Force)
        metrics_layout = QtWidgets.QVBoxLayout()
        top_layout.addLayout(metrics_layout, stretch=1)
        
        # Timing Panel
        self.timing_label = QtWidgets.QLabel("CURRENT LAP\n--:--.---\n\nLAST LAP\n--:--.---")
        self.timing_label.setStyleSheet("font-family: 'Courier New'; font-size: 24px; font-weight: bold; color: #00ff00;")
        self.timing_label.setAlignment(QtCore.Qt.AlignCenter)
        metrics_layout.addWidget(self.timing_label)
        
        # G-Force Plot
        self.gg_widget = pg.PlotWidget(title="G-FORCE")
        self.gg_widget.setXRange(-5, 5)
        self.gg_widget.setYRange(-5, 5)
        self.gg_widget.setAspectLocked(True)
        self.gg_widget.setBackground('#1a1a1a')
        self.gg_widget.addLine(x=0, pen='#333333')
        self.gg_widget.addLine(y=0, pen='#333333')
        
        # Circle at 4G
        theta = np.linspace(0, 2*np.pi, 100)
        cx = 4 * np.cos(theta)
        cy = 4 * np.sin(theta)
        self.gg_circle = pg.PlotCurveItem(cx, cy, pen=pg.mkPen('#555555', width=1))
        self.gg_widget.addItem(self.gg_circle)
        
        self.gg_dot = pg.ScatterPlotItem(size=12, brush=pg.mkBrush('#00aaff'), pen=pg.mkPen('w', width=1))
        self.gg_widget.addItem(self.gg_dot)
        metrics_layout.addWidget(self.gg_widget)
        
        # Gear / RPM
        self.engine_label = QtWidgets.QLabel("GEAR: N\n0 RPM")
        self.engine_label.setStyleSheet("font-family: 'Impact'; font-size: 40px; color: #ffff00;")
        self.engine_label.setAlignment(QtCore.Qt.AlignCenter)
        metrics_layout.addWidget(self.engine_label)
        
        # Middle: Speed Trace
        self.speed_widget = pg.PlotWidget(title="SPEED (km/h)")
        self.speed_widget.setYRange(0, 360)
        self.speed_widget.setXRange(0, self.track.total_length)
        self.speed_widget.setBackground('#1a1a1a')
        self.speed_widget.showGrid(x=True, y=True, alpha=0.3)
        
        self.ghost_line = pg.PlotCurveItem(pen=pg.mkPen('#444444', width=2, style=QtCore.Qt.DashLine))
        self.speed_line = pg.PlotCurveItem(pen=pg.mkPen('#00aaff', width=3))
        self.speed_widget.addItem(self.ghost_line)
        self.speed_widget.addItem(self.speed_line)
        
        self.speed_cursor = pg.InfiniteLine(pos=0, angle=90, pen=pg.mkPen('w', width=1, style=QtCore.Qt.DotLine))
        self.speed_widget.addItem(self.speed_cursor)
        
        main_layout.addWidget(self.speed_widget, stretch=1.5)
        
        # Bottom: Pedal Traces
        self.pedal_widget = pg.PlotWidget(title="PEDALS")
        self.pedal_widget.setYRange(-0.1, 1.1)
        self.pedal_widget.setXRange(0, self.track.total_length)
        self.pedal_widget.setBackground('#1a1a1a')
        self.pedal_widget.showGrid(x=True, y=True, alpha=0.3)
        
        self.throttle_line = pg.PlotCurveItem(pen=pg.mkPen('#00ff00', width=2))
        self.brake_line = pg.PlotCurveItem(pen=pg.mkPen('#ff0000', width=2))
        self.pedal_widget.addItem(self.throttle_line)
        self.pedal_widget.addItem(self.brake_line)
        
        self.pedal_cursor = pg.InfiniteLine(pos=0, angle=90, pen=pg.mkPen('w', width=1, style=QtCore.Qt.DotLine))
        self.pedal_widget.addItem(self.pedal_cursor)
        
        main_layout.addWidget(self.pedal_widget, stretch=1.5)

    def update(self):
        if not self.consumer:
            return

        msgs = self.consumer.poll(timeout_ms=0, max_records=50)
        if not msgs:
            return

        last_state = None
        for tp, messages in msgs.items():
            for msg in messages:
                state = msg.value
                last_state = state
                
                # New Lap Detection
                if state['lap_count'] > self.current_lap_num:
                    if self.current_lap_num != -1:
                        # Copy current to ghost
                        self.ghost_speed[:] = self.lap_speed[:]
                        self.ghost_line.setData(self.x_dist, self.ghost_speed)
                    
                    self.lap_speed.fill(np.nan)
                    self.lap_throttle.fill(np.nan)
                    self.lap_brake.fill(np.nan)
                    self.current_lap_num = state['lap_count']
                    self.last_idx = -1

                # Find Track Index
                _, idx = self.tree.query([state['x'], state['y']])
                
                if idx < self.TRACK_POINTS:
                    # Fill Gaps if needed
                    if self.last_idx != -1 and abs(idx - self.last_idx) < 500:
                        start, end = min(idx, self.last_idx), max(idx, self.last_idx)
                        self.lap_speed[start:end+1] = state['speed_kmh']
                        self.lap_throttle[start:end+1] = state['throttle']
                        self.lap_brake[start:end+1] = state['brake']
                    else:
                        self.lap_speed[idx] = state['speed_kmh']
                        self.lap_throttle[idx] = state['throttle']
                        self.lap_brake[idx] = state['brake']
                    self.last_idx = idx

        if last_state:
            self.render_state(last_state)

    def render_state(self, state):
        # Update Car Position
        self.car_dot.setData([state['x']], [state['y']])
        
        # Update Metrics
        self.timing_label.setText(
            f"CURRENT LAP\n{state['lap_time']:.3f}\n\n"
            f"LAST LAP\n{state['last_lap_time']:.3f}\n\n"
            f"{state['sector_name']}"
        )
        
        self.engine_label.setText(f"GEAR: {state['gear']}\n{int(state['rpm'])} RPM")
        
        # Update G-Force
        self.gg_dot.setData([state['g_lat']], [state['g_long']])
        
        # Update Trace Curves
        self.speed_line.setData(self.x_dist, self.lap_speed, connect='finite')
        self.throttle_line.setData(self.x_dist, self.lap_throttle, connect='finite')
        self.brake_line.setData(self.x_dist, self.lap_brake, connect='finite')
        
        # Update Cursors
        _, idx = self.tree.query([state['x'], state['y']])
        current_dist = self.x_dist[idx]
        self.speed_cursor.setValue(current_dist)
        self.pedal_cursor.setValue(current_dist)

if __name__ == "__main__":
    app = QtWidgets.QApplication(sys.argv)
    window = DashboardApp()
    window.show()
    sys.exit(app.exec())
