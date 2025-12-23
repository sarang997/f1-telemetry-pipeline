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

# -----------------------------------------------------------------------------
# 1. SPECIALIZED WIDGETS
# -----------------------------------------------------------------------------

class LapHistoryWidget(QtWidgets.QTableWidget):
    """
    Displays the last N laps with sector times (if available) and total time.
    """
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setColumnCount(3)
        self.setHorizontalHeaderLabels(["LAP", "TIME", "STATUS"])
        self.verticalHeader().setVisible(False)
        self.setEditTriggers(QtWidgets.QAbstractItemView.NoEditTriggers)
        self.setSelectionMode(QtWidgets.QAbstractItemView.NoSelection)
        self.setStyleSheet("""
            QTableWidget {
                background-color: #1a1a1a;
                color: #ffffff;
                border: 1px solid #333333;
                font-family: 'Courier New';
                font-size: 14px;
            }
            QHeaderView::section {
                background-color: #333333;
                color: #aaaaaa;
                padding: 4px;
                border: none;
            }
        """)
        self.horizontalHeader().setSectionResizeMode(QtWidgets.QHeaderView.Stretch)
        self.history = []

    def add_lap(self, lap_num, lap_time, is_pb=False):
        row = self.rowCount()
        self.insertRow(row)
        
        # Lap Num
        item_num = QtWidgets.QTableWidgetItem(str(lap_num))
        item_num.setTextAlignment(QtCore.Qt.AlignCenter)
        self.setItem(row, 0, item_num)
        
        # Time
        item_time = QtWidgets.QTableWidgetItem(f"{lap_time:.3f}")
        item_time.setTextAlignment(QtCore.Qt.AlignCenter)
        if is_pb:
            item_time.setForeground(QtGui.QColor('#00ff00')) # Green for PB
        self.setItem(row, 1, item_time)
        
        # Status (Placeholder for now)
        status = "PB" if is_pb else "OK"
        item_status = QtWidgets.QTableWidgetItem(status)
        item_status.setTextAlignment(QtCore.Qt.AlignCenter)
        self.setItem(row, 2, item_status)
        
        self.scrollToBottom()

class AnalyticsWidget(QtWidgets.QWidget):
    """
    Placeholder for future PySpark integration.
    """
    def __init__(self, parent=None):
        super().__init__(parent)
        layout = QtWidgets.QVBoxLayout(self)
        
        self.label = QtWidgets.QLabel("ANALYTICS ENGINE\n[OFFLINE]")
        self.label.setAlignment(QtCore.Qt.AlignCenter)
        self.label.setStyleSheet("font-family: 'Impact'; font-size: 24px; color: #555555;")
        layout.addWidget(self.label)
        
        self.sub_label = QtWidgets.QLabel("Waiting for PySpark Stream...")
        self.sub_label.setAlignment(QtCore.Qt.AlignCenter)
        self.sub_label.setStyleSheet("font-family: 'Arial'; font-size: 14px; color: #444444;")
        layout.addWidget(self.sub_label)
        
        self.setStyleSheet("background-color: #0f0f0f; border: 1px dashed #333333;")

# -----------------------------------------------------------------------------
# 2. MAIN DASHBOARD APPLICATION
# -----------------------------------------------------------------------------

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
        self.fastest_lap_time = float('inf')
        
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
        self.setWindowTitle("F1 Telemetry Hybrid Dashboard")
        self.resize(1600, 1000)
        self.setStyleSheet("background-color: #0d0d0d; color: #ffffff;")
        
        central_widget = QtWidgets.QWidget()
        self.setCentralWidget(central_widget)
        main_layout = QtWidgets.QVBoxLayout(central_widget)
        main_layout.setContentsMargins(0, 0, 0, 0)
        
        # Tab Widget
        self.tabs = QtWidgets.QTabWidget()
        self.tabs.setStyleSheet("""
            QTabWidget::pane {
                border: none;
            }
            QTabBar::tab {
                background-color: #1a1a1a;
                color: #aaaaaa;
                padding: 10px 20px;
                font-size: 14px;
                font-weight: bold;
            }
            QTabBar::tab:selected {
                background-color: #333333;
                color: #ffffff;
            }
        """)
        main_layout.addWidget(self.tabs)
        
        # ---------------------------------------------------------
        # TAB 1: TELEMETRY (Main Dashboard)
        # ---------------------------------------------------------
        telemetry_tab = QtWidgets.QWidget()
        grid = QtWidgets.QGridLayout(telemetry_tab)
        grid.setContentsMargins(10, 10, 10, 10)
        grid.setSpacing(10)
        
        # ---------------------------------------------------------
        # TOP LEFT: Track Map & Live Metrics (BIGGER NOW)
        # ---------------------------------------------------------
        tl_widget = QtWidgets.QWidget()
        tl_layout = QtWidgets.QVBoxLayout(tl_widget)
        tl_layout.setContentsMargins(0,0,0,0)
        
        # Track Map (PyQtGraph) - MUCH BIGGER
        self.track_widget = pg.PlotWidget(title="TRACK MAP & POSITION")
        self.track_widget.setAspectLocked(True)
        self.track_widget.showAxis('left', False)
        self.track_widget.showAxis('bottom', False)
        self.track_widget.setBackground('#0d0d0d')
        
        self.track_line = pg.PlotCurveItem(self.track.x, self.track.y, pen=pg.mkPen('#333333', width=14))
        self.track_widget.addItem(self.track_line)
        self.track_center_line = pg.PlotCurveItem(self.track.x, self.track.y, pen=pg.mkPen('#00aaff', width=2, style=QtCore.Qt.DashLine))
        self.track_widget.addItem(self.track_center_line)
        
        self.car_dot = pg.ScatterPlotItem(size=20, brush=pg.mkBrush('#ff0000'), pen=pg.mkPen('w', width=2))
        self.track_widget.addItem(self.car_dot)
        
        tl_layout.addWidget(self.track_widget, stretch=5)  # Increased from 3 to 5
        
        # Live Stats Row (Speed, Gear, RPM) in Top Left
        stats_layout = QtWidgets.QHBoxLayout()
        
        self.lbl_speed = QtWidgets.QLabel("0\nKM/H")
        self.lbl_speed.setAlignment(QtCore.Qt.AlignCenter)
        self.lbl_speed.setStyleSheet("font-family: 'Impact'; font-size: 36px; color: #00aaff;")
        
        self.lbl_gear = QtWidgets.QLabel("N\nGEAR")
        self.lbl_gear.setAlignment(QtCore.Qt.AlignCenter)
        self.lbl_gear.setStyleSheet("font-family: 'Impact'; font-size: 36px; color: #feab3a;")
        
        self.lbl_rpm = QtWidgets.QLabel("0\nRPM")
        self.lbl_rpm.setAlignment(QtCore.Qt.AlignCenter)
        self.lbl_rpm.setStyleSheet("font-family: 'Impact'; font-size: 36px; color: #ff0055;")
        
        stats_layout.addWidget(self.lbl_speed)
        stats_layout.addWidget(self.lbl_gear)
        stats_layout.addWidget(self.lbl_rpm)
        
        tl_layout.addLayout(stats_layout, stretch=1)
        grid.addWidget(tl_widget, 0, 0)
        
        # ---------------------------------------------------------
        # TOP RIGHT: Lap History & Timing
        # ---------------------------------------------------------
        tr_widget = QtWidgets.QWidget()
        tr_layout = QtWidgets.QVBoxLayout(tr_widget)
        
        # Big Timing Display
        self.timing_label = QtWidgets.QLabel("0:00.000")
        self.timing_label.setAlignment(QtCore.Qt.AlignCenter)
        self.timing_label.setStyleSheet("font-family: 'Courier New'; font-size: 64px; font-weight: bold; color: #ffffff;")
        tr_layout.addWidget(self.timing_label)
        
        # Car Status Panel (moved here from bottom right)
        status_group = QtWidgets.QGroupBox("CAR STATUS")
        status_group.setStyleSheet("QGroupBox { font-weight: bold; border: 1px solid #333; margin-top: 6px; } QGroupBox::title { subcontrol-origin: margin; left: 10px; padding: 0 3px; }")
        status_layout = QtWidgets.QGridLayout(status_group)
        
        # Fuel
        self.lbl_fuel = QtWidgets.QLabel("FUEL\n-- KG")
        self.lbl_fuel.setAlignment(QtCore.Qt.AlignCenter)
        self.lbl_fuel.setStyleSheet("background-color: #222; border-radius: 5px; padding: 10px; font-weight: bold; font-size: 16px;")
        status_layout.addWidget(self.lbl_fuel, 0, 0)
        
        # Tires (Grip/Wear)
        self.lbl_tires = QtWidgets.QLabel("GRIP\n-- %")
        self.lbl_tires.setAlignment(QtCore.Qt.AlignCenter)
        self.lbl_tires.setStyleSheet("background-color: #222; border-radius: 5px; padding: 10px; font-weight: bold; font-size: 16px;")
        status_layout.addWidget(self.lbl_tires, 0, 1)
        
        tr_layout.addWidget(status_group)
        
        # Lap History Table (Reuse our custom class)
        self.lap_table = LapHistoryWidget()
        tr_layout.addWidget(self.lap_table)
        
        grid.addWidget(tr_widget, 0, 1)

        # ---------------------------------------------------------
        # BOTTOM: Telemetry Traces (Speed/Pedals) - FULL WIDTH
        # ---------------------------------------------------------
        bl_widget = QtWidgets.QWidget()
        bl_layout = QtWidgets.QVBoxLayout(bl_widget)
        bl_layout.setContentsMargins(0,0,0,0)
        
        # Speed Trace
        self.speed_plot = pg.PlotWidget(title="SPEED LIVE vs GHOST")
        self.speed_plot.setBackground('#1a1a1a')
        self.speed_plot.showGrid(x=True, y=True, alpha=0.3)
        self.speed_plot.setXRange(0, self.track.total_length)
        self.speed_plot.setYRange(0, 350)
        
        self.ghost_line = pg.PlotCurveItem(pen=pg.mkPen('#444444', width=2, style=QtCore.Qt.DashLine))
        self.speed_line = pg.PlotCurveItem(pen=pg.mkPen('#00aaff', width=3))
        self.speed_plot.addItem(self.ghost_line)
        self.speed_plot.addItem(self.speed_line)
        self.speed_cursor = pg.InfiniteLine(pos=0, angle=90, pen=pg.mkPen('w', width=1, style=QtCore.Qt.DotLine))
        self.speed_plot.addItem(self.speed_cursor)
        
        bl_layout.addWidget(self.speed_plot, stretch=1)
        
        # Pedal Trace
        self.pedal_plot = pg.PlotWidget(title="THROTTLE / BRAKE")
        self.pedal_plot.setBackground('#1a1a1a')
        self.pedal_plot.showGrid(x=True, y=True, alpha=0.3)
        self.pedal_plot.setXRange(0, self.track.total_length)
        self.pedal_plot.setYRange(-0.1, 1.1)
        
        self.throttle_line = pg.PlotCurveItem(pen=pg.mkPen('#00ff00', width=2))
        self.brake_line = pg.PlotCurveItem(pen=pg.mkPen('#ff0000', width=2))
        self.pedal_plot.addItem(self.throttle_line)
        self.pedal_plot.addItem(self.brake_line)
        self.pedal_cursor = pg.InfiniteLine(pos=0, angle=90, pen=pg.mkPen('w', width=1, style=QtCore.Qt.DotLine))
        self.pedal_plot.addItem(self.pedal_cursor)
        
        bl_layout.addWidget(self.pedal_plot, stretch=1)
        
        grid.addWidget(bl_widget, 1, 0, 1, 2)  # Span both columns
        
        self.tabs.addTab(telemetry_tab, "TELEMETRY")
        
        # ---------------------------------------------------------
        # TAB 2: ANALYTICS (Separate Tab)
        # ---------------------------------------------------------
        analytics_tab = QtWidgets.QWidget()
        analytics_layout = QtWidgets.QVBoxLayout(analytics_tab)
        analytics_layout.setContentsMargins(20, 20, 20, 20)
        
        self.analytics_widget = AnalyticsWidget()
        analytics_layout.addWidget(self.analytics_widget)
        
        self.tabs.addTab(analytics_tab, "ANALYTICS")

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
                
                # New Lap Logic - Use .get() for safety
                lap = state.get('lap_count', 0)
                if lap > self.current_lap_num:
                    if self.current_lap_num != -1:
                        # Copy current to ghost
                        self.ghost_speed[:] = self.lap_speed[:]
                        self.ghost_line.setData(self.x_dist, self.ghost_speed)
                        
                        # Add to history
                        last_time = state.get('last_lap_time', 0)
                        if last_time > 0:
                            is_pb = last_time < self.fastest_lap_time
                            if is_pb:
                                self.fastest_lap_time = last_time
                            self.lap_table.add_lap(self.current_lap_num, last_time, is_pb)
                    
                    self.lap_speed.fill(np.nan)
                    self.lap_throttle.fill(np.nan)
                    self.lap_brake.fill(np.nan)
                    self.current_lap_num = lap
                    self.last_idx = -1

                # Find Track Index
                _, idx = self.tree.query([state.get('x',0), state.get('y',0)])
                
                if idx < self.TRACK_POINTS:
                    # Fill Gaps if needed
                    s_kmh = state.get('speed_kmh', 0)
                    s_thr = state.get('throttle', 0)
                    s_brk = state.get('brake', 0)
                    
                    if self.last_idx != -1 and abs(idx - self.last_idx) < 500:
                        start, end = min(idx, self.last_idx), max(idx, self.last_idx)
                        self.lap_speed[start:end+1] = s_kmh
                        self.lap_throttle[start:end+1] = s_thr
                        self.lap_brake[start:end+1] = s_brk
                    else:
                        self.lap_speed[idx] = s_kmh
                        self.lap_throttle[idx] = s_thr
                        self.lap_brake[idx] = s_brk
                    self.last_idx = idx

        if last_state:
            self.render_state(last_state)

    def render_state(self, state):
        # Update Car Position
        self.car_dot.setData([state.get('x',0)], [state.get('y',0)])
        
        # Update Stats
        self.lbl_speed.setText(f"{int(state.get('speed_kmh',0))}\nKM/H")
        self.lbl_gear.setText(f"{state.get('gear','N')}\nGEAR")
        self.lbl_rpm.setText(f"{int(state.get('rpm',0))}\nRPM")
        
        # Update Timing Label
        self.timing_label.setText(f"{state.get('lap_time',0):.3f}")
        
        # Update Status
        fuel = state.get('fuel_kg', 0)
        self.lbl_fuel.setText(f"FUEL\n{fuel:.1f} KG")
        
        # Use simple color coding for fuel
        if fuel < 10:
            self.lbl_fuel.setStyleSheet("background-color: #550000; border-radius: 5px; padding: 10px; font-weight: bold; color: #ff5555;")
        else:
            self.lbl_fuel.setStyleSheet("background-color: #222; border-radius: 5px; padding: 10px; font-weight: bold;")

        # Grip / Tire Wear
        # Note: tire_wear_pct might be available if we add it, but 'mu_eff' is grip coeff (approx 1.1 max)
        # Check if we have tire_wear_pct in state (added in sensors.py?? No, I just checked and it WAS there in EngineMonitor)
        wear = state.get('tire_wear_pct', 0) 
        self.lbl_tires.setText(f"WEAR\n{wear:.1f}%")
        
        # Update Trace Curves
        self.speed_line.setData(self.x_dist, self.lap_speed, connect='finite')
        self.throttle_line.setData(self.x_dist, self.lap_throttle, connect='finite')
        self.brake_line.setData(self.x_dist, self.lap_brake, connect='finite')
        
        # Update Cursors
        _, idx = self.tree.query([state.get('x',0), state.get('y',0)])
        current_dist = self.x_dist[idx]
        self.speed_cursor.setValue(current_dist)
        self.pedal_cursor.setValue(current_dist)

if __name__ == "__main__":
    app = QtWidgets.QApplication(sys.argv)
    window = DashboardApp()
    window.show()
    sys.exit(app.exec())
