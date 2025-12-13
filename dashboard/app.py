import numpy as np
import matplotlib.pyplot as plt
import matplotlib.gridspec as gridspec
from matplotlib.animation import FuncAnimation
import json
import sys
import os
from kafka import KafkaConsumer
from collections import deque
from scipy.spatial import cKDTree

# Add parent directory to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC, KAFKA_API_VERSION, VISUALIZATION_FPS, VIZ_GROUP_ID
from simulator.track import SilverstoneTrack

# --- INITIALIZATION ---
# Load track high-res points to map Distance -> Graph Index
track = SilverstoneTrack()
TRACK_POINTS = len(track.x) # Should be ~20000 high-res points

# Initialize Kafka Consumer
try:
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        auto_offset_reset='latest',
        group_id=VIZ_GROUP_ID, 
        api_version=KAFKA_API_VERSION
    )
    print(f"Connected to Kafka Consumer at {KAFKA_BOOTSTRAP_SERVERS}")
except Exception as e:
    print(f"Failed to connect to Kafka: {e}")
    consumer = None

# --- DATA BUFFERS (LAP BASED) ---
# We map the track 1:1. Since simulation also uses SilverstoneTrack class, 
# 'idx' from sim matches 'idx' here.
lap_speed = np.full(TRACK_POINTS, np.nan)
lap_throttle = np.full(TRACK_POINTS, np.nan)
lap_brake = np.full(TRACK_POINTS, np.nan)
lap_rpm = np.full(TRACK_POINTS, np.nan)

# Ghost Lap (Previous Lap)
ghost_speed = np.full(TRACK_POINTS, np.nan)

current_lap_num = -1
last_idx = -1

# --- PLOT SETUP ---
plt.style.use('dark_background')
fig = plt.figure(figsize=(16, 12), facecolor='#0d0d0d')
fig.suptitle('F1 TELEMETRY: LIVE TIMING', color='white', fontsize=20, fontweight='bold', y=0.98)

# 4 Rows: Map, Gauges, Speed, Pedals
gs = gridspec.GridSpec(4, 2, height_ratios=[3, 1, 1.5, 1.5], width_ratios=[1, 1])

# 1. TRACK MAP (Row 0)
ax_map = fig.add_subplot(gs[0, 0])
ax_map.set_facecolor('#0d0d0d')
ax_map.plot(track.x, track.y, color='#333333', linewidth=12, alpha=0.8)
ax_map.plot(track.x, track.y, color='#00aaff', linewidth=2, linestyle='--', alpha=0.5)
dot, = ax_map.plot([], [], 'ro', markersize=18, markeredgecolor='white', markeredgewidth=3, zorder=5)
ax_map.axis('equal')
ax_map.axis('off')

# 2. TIMING & INFO (Row 0 Right)
ax_text = fig.add_subplot(gs[0, 1])
ax_text.set_facecolor('#0d0d0d')
ax_text.axis('off')
txt_curr_lbl = ax_text.text(0.1, 0.75, "CURRENT LAP", color='#888888', fontsize=14)
txt_curr_val = ax_text.text(0.1, 0.60, "--:--.---", color='white', fontsize=40, fontweight='bold', fontfamily='monospace')
txt_time_lbl = ax_text.text(0.1, 0.40, "LAST LAP", color='#888888', fontsize=14)
txt_time_val = ax_text.text(0.1, 0.25, "--:--.---", color='#00ff00', fontsize=40, fontweight='bold', fontfamily='monospace')
txt_sector = ax_text.text(0.1, 0.05, "SECTOR --", color='#00aaff', fontsize=18, fontstyle='italic')

# 3. GAUGES (Row 1)
# GG (Left)
ax_gg = fig.add_subplot(gs[1, 0])
ax_gg.set_facecolor('#1a1a1a')
ax_gg.set_xlim(-5, 5)
ax_gg.set_ylim(-5, 5)
ax_gg.grid(True, color='#333333', linestyle='--', linewidth=1)
ax_gg.set_xticks([])
ax_gg.set_yticks([])
circle1 = plt.Circle((0, 0), 4, color='#555555', fill=False, linewidth=2)
ax_gg.add_patch(circle1)
dot_gg, = ax_gg.plot([], [], 'o', color='#00aaff', markersize=12, markeredgecolor='white', markeredgewidth=1)
ax_gg.text(-4.5, 4, "G-FORCE", color='#888888', fontsize=12)

# RPM (Right)
ax_eng = fig.add_subplot(gs[1, 1])
ax_eng.axis('off')
txt_gear = ax_eng.text(0.5, 0.5, "N", color='#ffff00', fontsize=60, fontweight='bold', ha='center', va='center')
txt_rpm = ax_eng.text(0.5, 0.2, "0 RPM", color='white', fontsize=18, ha='center', fontfamily='monospace')

# 4. GRAPHS (Row 2 & 3)
x_dist = np.linspace(0, track.total_length, TRACK_POINTS)

# Speed (Row 2)
ax_speed = fig.add_subplot(gs[2, :])
ax_speed.set_facecolor('#1a1a1a')
ax_speed.set_title("SPEED (km/h)", color='#888888', fontsize=12, loc='left')
ax_speed.set_ylim(0, 360)
ax_speed.set_xlim(0, track.total_length)
ax_speed.grid(True, color='#333333', linestyle='--', linewidth=1)
ax_speed.set_xticklabels([]) # Hide X labels for middle graph

line_ghost_speed, = ax_speed.plot(x_dist, ghost_speed, color='#444444', linewidth=2, linestyle='--')
line_speed, = ax_speed.plot(x_dist, lap_speed, color='#00aaff', linewidth=3)
cursor_speed = ax_speed.axvline(x=0, color='white', linestyle=':', alpha=0.8, linewidth=2)

# Pedals (Row 3)
ax_pedals = fig.add_subplot(gs[3, :])
ax_pedals.set_facecolor('#1a1a1a')
ax_pedals.set_title("PEDALS", color='#888888', fontsize=12, loc='left')
ax_pedals.set_ylim(-0.1, 1.1)
ax_pedals.set_xlim(0, track.total_length)
ax_pedals.grid(True, color='#333333', linestyle='--', linewidth=1)

lap_throttle = np.full(TRACK_POINTS, np.nan)
lap_brake = np.full(TRACK_POINTS, np.nan)

line_thr, = ax_pedals.plot(x_dist, lap_throttle, color='#00ff00', linewidth=2, label='Throttle')
line_brk, = ax_pedals.plot(x_dist, lap_brake, color='#ff0000', linewidth=2, label='Brake')
ax_pedals.legend(loc='upper right', frameon=False, fontsize=10)
cursor_pedals = ax_pedals.axvline(x=0, color='white', linestyle=':', alpha=0.8, linewidth=2)


# -- Helper for mapping --
tree = cKDTree(np.column_stack((track.x, track.y)))

def efficient_update(frame):
    global current_lap_num, last_idx
    if not consumer: return dot,

    msgs = consumer.poll(timeout_ms=0, max_records=50)
    if not msgs: return dot,

    last_state = None
    
    for tp, messages in msgs.items():
        for msg in messages:
            state = msg.value
            last_state = state
            
            # 1. Detect New Lap
            if state['lap_count'] > current_lap_num:
                if current_lap_num != -1:
                    np.copyto(ghost_speed, lap_speed)
                    line_ghost_speed.set_ydata(ghost_speed)
                lap_speed.fill(np.nan)
                lap_throttle.fill(np.nan)
                lap_brake.fill(np.nan)
                current_lap_num = state['lap_count']
                last_idx = -1

            # 2. Find Index
            dist, idx = tree.query([state['x'], state['y']])
            
            # 3. Update Buffers with GAP FILLING
            if idx < TRACK_POINTS:
                if last_idx != -1 and abs(idx - last_idx) < 500: 
                    # forward fill
                    if idx > last_idx:
                        lap_speed[last_idx:idx+1] = state['speed_kmh']
                        lap_throttle[last_idx:idx+1] = state['throttle']
                        lap_brake[last_idx:idx+1] = state['brake']
                    # backward fill
                    elif idx < last_idx:
                        lap_speed[idx:last_idx+1] = state['speed_kmh']
                        lap_throttle[idx:last_idx+1] = state['throttle']
                        lap_brake[idx:last_idx+1] = state['brake']
                else:
                    lap_speed[idx] = state['speed_kmh']
                    lap_throttle[idx] = state['throttle']
                    lap_brake[idx] = state['brake']
                
                last_idx = idx

    if last_state:
        state = last_state
        
        # UI Updates
        dot.set_data([state['x']], [state['y']])
        dot_gg.set_data([state['g_lat']], [state['g_long']])
        
        txt_time_val.set_text(f"{state['last_lap_time']:.3f}")
        txt_curr_val.set_text(f"{state['lap_time']:.3f}")
        txt_sector.set_text(f"{state['sector_name']}")
        txt_gear.set_text(str(state['gear']))
        txt_rpm.set_text(f"{state['rpm']}")

        # Graph Lines 
        line_speed.set_ydata(lap_speed)
        line_thr.set_ydata(lap_throttle)
        line_brk.set_ydata(lap_brake)
        
        # Cursors
        current_dist = x_dist[tree.query([state['x'], state['y']])[1]]
        cursor_speed.set_xdata([current_dist])
        cursor_pedals.set_xdata([current_dist])

    return dot, txt_curr_val

ani = FuncAnimation(fig, efficient_update, interval=1000/VISUALIZATION_FPS, blit=False)
plt.tight_layout()
plt.show()
