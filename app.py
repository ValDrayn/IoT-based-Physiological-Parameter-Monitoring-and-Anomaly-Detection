import gradio as gr
import paho.mqtt.client as mqtt
import json
import math
import threading
import time
from collections import deque
from datetime import datetime
import plotly.graph_objects as go


# ============================================================
# MQTT CONFIG
# ============================================================

MQTT_BROKER = "broker.hivemq.com"
MQTT_PORT = 1883
MQTT_TOPIC = "skripsi/physiomonitor/data"


# ============================================================
# SYSTEM CONFIG
# ============================================================

DATA_TIMEOUT = 5
HISTORY_LENGTH = 60


# ============================================================
# THEME COLORS (single source of truth for CSS + Plotly)
# ============================================================

COLOR_BG = "#0b1120"
COLOR_PANEL = "#121a2e"
COLOR_PANEL_ALT = "#161f38"
COLOR_BORDER = "#22304f"
COLOR_TEXT = "#e7ecf7"
COLOR_MUTED = "#8993ab"

COLOR_HR = "#ff5c7a"
COLOR_SPO2 = "#3fd6ff"
COLOR_TEMP = "#ffb454"

COLOR_NORMAL = "#2bd47d"
COLOR_ANOMALY = "#ff5c5c"
COLOR_WAITING = "#ffcc4d"
COLOR_INACTIVE = "#6b7590"
COLOR_DETECTING = "#7c5cff"


# ============================================================
# SENSOR DATA
# ============================================================

sensor_data = {
    "hr": None,
    "spo2": None,
    "temp": None,

    "status": "MENUNGGU_DATA",
    "device_status": None,

    "connected": False,

    "last_data_time": None,
    "last_data_timestamp": None,
}


# ============================================================
# HISTORY
# ============================================================

hr_history = deque(maxlen=HISTORY_LENGTH)
spo2_history = deque(maxlen=HISTORY_LENGTH)
temp_history = deque(maxlen=HISTORY_LENGTH)
time_history = deque(maxlen=HISTORY_LENGTH)


# ============================================================
# LOCK
# ============================================================

data_lock = threading.Lock()


# ============================================================
# MQTT CONNECT
# ============================================================

def on_connect(client, userdata, flags, rc, properties=None):

    print(f"[MQTT] Connected. RC={rc}")

    if rc == 0:
        client.subscribe(MQTT_TOPIC)
        print(f"[MQTT] Subscribed: {MQTT_TOPIC}")
        with data_lock:
            sensor_data["connected"] = True
    else:
        print("[MQTT] Connection failed")
        with data_lock:
            sensor_data["connected"] = False


# ============================================================
# MQTT DISCONNECT
# ============================================================

def on_disconnect(client, userdata, disconnect_flags=None, rc=None, properties=None):

    print("[MQTT] Disconnected")

    with data_lock:
        sensor_data["connected"] = False


# ============================================================
# MQTT MESSAGE
# ============================================================

def on_message(client, userdata, msg):

    try:
        raw_payload = msg.payload.decode("utf-8")
        print(f"[MQTT] Received: {raw_payload}")

        data = json.loads(raw_payload)

        hr = data.get("hr")
        spo2 = data.get("spo2")
        temp = data.get("temp")
        device_status = data.get("status")

        if hr is not None:
            hr = float(hr)
        if spo2 is not None:
            spo2 = float(spo2)
        if temp is not None:
            temp = float(temp)

        timestamp = time.time()

        with data_lock:
            sensor_data["hr"] = hr
            sensor_data["spo2"] = spo2
            sensor_data["temp"] = temp
            sensor_data["device_status"] = device_status
            sensor_data["last_data_time"] = timestamp
            sensor_data["last_data_timestamp"] = timestamp

        if hr is not None:
            hr_history.append(hr)
        if spo2 is not None:
            spo2_history.append(spo2)
        if temp is not None:
            temp_history.append(temp)

        time_history.append(timestamp)

    except Exception as e:
        print(f"[MQTT] Message error: {e}")


# ============================================================
# MQTT CLIENT
# ============================================================

mqtt_client = mqtt.Client()

mqtt_client.on_connect = on_connect
mqtt_client.on_disconnect = on_disconnect
mqtt_client.on_message = on_message


# ============================================================
# MQTT THREAD
# ============================================================

def mqtt_worker():

    while True:
        try:
            print("[MQTT] Connecting...")
            mqtt_client.connect(MQTT_BROKER, MQTT_PORT, 60)
            mqtt_client.loop_forever()

        except Exception as e:
            print(f"[MQTT] Error: {e}")
            with data_lock:
                sensor_data["connected"] = False
            time.sleep(3)


mqtt_thread = threading.Thread(target=mqtt_worker, daemon=True)
mqtt_thread.start()


# ============================================================
# GET STATUS
#
# The ESP32 firmware already computes a status per reading
# ("MENUNGGU SENSOR", "MENDETEKSI", "ANOMALI", "NORMAL") and
# sends it in the MQTT payload, so we use it directly instead
# of re-deriving it here. We only add one thing on top of that:
# detecting when the device has stopped sending data at all
# (MQTT link is up but no fresh payload arrived recently).
# ============================================================

DEVICE_STATUS_MAP = {
    "MENUNGGU SENSOR": "MENUNGGU_SENSOR",
    "MENDETEKSI": "MENDETEKSI",
    "ANOMALI": "ANOMALI",
    "NORMAL": "NORMAL",
}


def get_status():

    with data_lock:
        device_status = sensor_data["device_status"]
        last_data_time = sensor_data["last_data_time"]

    if last_data_time is None:
        return "MENUNGGU_SENSOR"

    elapsed = time.time() - last_data_time

    if elapsed > DATA_TIMEOUT:
        return "TIDAK_AKTIF"

    return DEVICE_STATUS_MAP.get(device_status, "MENUNGGU_SENSOR")


# ============================================================
# FORMAT
# ============================================================

def format_value(value, decimals=1):

    if value is None:
        return "--"

    try:
        return f"{float(value):.{decimals}f}"
    except (TypeError, ValueError):
        return "--"


# ============================================================
# GRAPH BASE CONFIG
# ============================================================

def graph_layout(title, y_title):

    return dict(

        title=dict(
            text=title,
            font=dict(size=13, color=COLOR_TEXT, family="Inter, sans-serif"),
            x=0.02,
            xanchor="left",
        ),

        xaxis=dict(
            title=dict(text="Waktu", font=dict(size=10, color=COLOR_MUTED)),
            showgrid=True,
            gridcolor="rgba(255,255,255,0.06)",
            showticklabels=True,
            tickformat="%H:%M:%S",
            zeroline=False,
            color=COLOR_MUTED,
            tickfont=dict(size=9),
        ),

        yaxis=dict(
            title=dict(text=y_title, font=dict(size=10, color=COLOR_MUTED)),
            showgrid=True,
            gridcolor="rgba(255,255,255,0.06)",
            zeroline=False,
            color=COLOR_MUTED,
            tickfont=dict(size=10),
            autorange=True,
        ),

        height=200,

        margin=dict(l=40, r=15, t=38, b=15),

        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",

        font=dict(family="Inter, sans-serif", color=COLOR_TEXT),

        showlegend=False,

        hoverlabel=dict(
            bgcolor=COLOR_PANEL_ALT,
            font_color=COLOR_TEXT,
            bordercolor=COLOR_BORDER,
        ),
    )


def _hex_to_rgba(hex_color, alpha):

    hex_color = hex_color.lstrip("#")
    r, g, b = (int(hex_color[i:i + 2], 16) for i in (0, 2, 4))
    return f"rgba({r},{g},{b},{alpha})"


def build_line_graph(values, timestamps, title, y_title, line_color):

    fig = go.Figure()

    if values:

        x = [datetime.fromtimestamp(t) for t in timestamps]

        fig.add_trace(
            go.Scatter(
                x=x,
                y=values,
                mode="lines+markers",
                line=dict(width=2, color=line_color, shape="spline"),
                marker=dict(size=5, color=line_color),
                fill="tozeroy",
                fillcolor=_hex_to_rgba(line_color, 0.10),
                hovertemplate="%{y}<extra></extra>",
            )
        )

    fig.update_layout(**graph_layout(title, y_title))

    return fig


# ============================================================
# GRAPHS
# ============================================================

def create_hr_graph():
    with data_lock:
        values = list(hr_history)
        timestamps = list(time_history)[-len(values):]
    return build_line_graph(values, timestamps, "❤️ Heart Rate", "BPM", COLOR_HR)


def create_spo2_graph():
    with data_lock:
        values = list(spo2_history)
        timestamps = list(time_history)[-len(values):]
    return build_line_graph(values, timestamps, "🫁 SpO₂", "%", COLOR_SPO2)


def create_temp_graph():
    with data_lock:
        values = list(temp_history)
        timestamps = list(time_history)[-len(values):]
    return build_line_graph(values, timestamps, "🌡️ Suhu Tubuh", "°C", COLOR_TEMP)


# ============================================================
# DASHBOARD HTML
# ============================================================

def build_top_html(connected, last_data_time):

    if connected:
        connection = (
            '<span class="dot online"></span>'
            '<span class="conn-text online-text">MQTT TERHUBUNG</span>'
        )
    else:
        connection = (
            '<span class="dot offline"></span>'
            '<span class="conn-text offline-text">MQTT TERPUTUS</span>'
        )

    if last_data_time is None:
        last_update = "Belum ada data"
    else:
        elapsed = time.time() - last_data_time
        if elapsed < 1:
            last_update = "Baru saja"
        else:
            last_update = f"{elapsed:.0f} detik lalu"

    return f"""
    <div class="topbar">
        <div class="brand">
            <div class="brand-icon">🫀</div>
            <div>
                <div class="title">Physio Monitor</div>
                <div class="subtitle">IoT Physiological Monitoring System</div>
            </div>
        </div>

        <div class="connection">
            <div class="conn-row">{connection}</div>
            <div class="update">Update terakhir: {last_update}</div>
        </div>
    </div>
    """


def build_left_html(hr, spo2, temp, status):

    if status == "NORMAL":
        status_class = "normal"
        status_icon = "✓"
        status_description = "Parameter dalam kondisi normal"
    elif status == "ANOMALI":
        status_class = "anomaly"
        status_icon = "!"
        status_description = "Terdeteksi penyimpangan parameter"
    elif status == "MENDETEKSI":
        status_class = "detecting"
        status_icon = "◐"
        status_description = "Jari terdeteksi, mengumpulkan data sensor..."
    elif status == "TIDAK_AKTIF":
        status_class = "inactive"
        status_icon = "○"
        status_description = "Tidak menerima data sensor"
    else:
        status_class = "waiting"
        status_icon = "…"
        status_description = "Menunggu jari diletakkan di sensor"

    return f"""
    <div class="left-panel">

        <div class="status-card {status_class}">
            <div class="status-icon-wrap">
                <div class="status-icon">{status_icon}</div>
            </div>
            <div class="status-body">
                <div class="status-label">STATUS PASIEN</div>
                <div class="status-value">{status}</div>
                <div class="status-description">{status_description}</div>
            </div>
        </div>

        <div class="sensor-list">

            <div class="sensor-row">
                <div class="sensor-icon hr-icon">❤️</div>
                <div class="sensor-info">
                    <div class="sensor-name">Heart Rate</div>
                    <div class="sensor-value">{format_value(hr, 0)}<small>BPM</small></div>
                </div>
            </div>

            <div class="sensor-row">
                <div class="sensor-icon spo2-icon">🫁</div>
                <div class="sensor-info">
                    <div class="sensor-name">SpO₂</div>
                    <div class="sensor-value">{format_value(spo2, 0)}<small>%</small></div>
                </div>
            </div>

            <div class="sensor-row">
                <div class="sensor-icon temp-icon">🌡️</div>
                <div class="sensor-info">
                    <div class="sensor-name">Suhu Tubuh</div>
                    <div class="sensor-value">{format_value(temp, 1)}<small>°C</small></div>
                </div>
            </div>

        </div>

        <div class="system-info">
            <div class="info-title">SYSTEM INFO</div>

            <div class="info-row"><span>Device</span><strong>ESP32</strong></div>
            <div class="info-row"><span>Sensor PPG</span><strong>MAX30102</strong></div>
            <div class="info-row"><span>Sensor Suhu</span><strong>DS18B20</strong></div>
            <div class="info-row"><span>Protokol</span><strong>MQTT</strong></div>
        </div>

    </div>
    """


def update_dashboard():

    with data_lock:
        hr = sensor_data["hr"]
        spo2 = sensor_data["spo2"]
        temp = sensor_data["temp"]
        connected = sensor_data["connected"]
        last_data_time = sensor_data["last_data_time"]

    status = get_status()

    top_html = build_top_html(connected, last_data_time)
    left_html = build_left_html(hr, spo2, temp, status)

    return (
        top_html,
        left_html,
        create_hr_graph(),
        create_spo2_graph(),
        create_temp_graph(),
    )


# ============================================================
# CSS
# ============================================================

CSS = f"""

@import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800&display=swap');

* {{
    font-family: 'Inter', sans-serif;
    box-sizing: border-box;
}}

body, .gradio-container {{
    background: {COLOR_BG} !important;
}}

html, body {{
    width: 100% !important;
}}

.gradio-container {{
    max-width: 1320px !important;
    width: 100% !important;
    margin: 0 auto !important;
    padding: 18px !important;
}}

.topbar, .left-panel, .left-panel *, .status-card, .status-card *,
.sensor-list, .sensor-list *, .system-info, .system-info * {{
    color: {COLOR_TEXT};
}}

/* ---------- remove Gradio's own row/column chrome so our
   cards are the only visible surfaces ---------- */

.dash-row {{
    gap: 14px !important;
    margin-top: 14px;
}}

.left-col {{
    background: transparent !important;
    border: none !important;
    padding: 0 !important;
    gap: 14px !important;
}}

.right-col {{
    background: transparent !important;
    border: none !important;
    padding: 0 !important;
    gap: 14px !important;
}}

.right-col > .gap {{
    gap: 14px !important;
}}

.topbar {{
    display: flex;
    justify-content: space-between;
    align-items: center;
    background: linear-gradient(135deg, {COLOR_PANEL} 0%, {COLOR_PANEL_ALT} 100%);
    border: 1px solid {COLOR_BORDER};
    border-radius: 16px;
    padding: 18px 24px;
}}

.brand {{
    display: flex;
    align-items: center;
    gap: 14px;
}}

.brand-icon {{
    width: 46px;
    height: 46px;
    border-radius: 14px;
    background: linear-gradient(135deg, #ff5c7a33, #3fd6ff33);
    display: flex;
    align-items: center;
    justify-content: center;
    font-size: 22px;
}}

.title {{
    font-size: 20px;
    font-weight: 800;
    letter-spacing: -0.3px;
    color: {COLOR_TEXT} !important;
}}

.subtitle {{
    font-size: 12px;
    color: {COLOR_MUTED} !important;
    margin-top: 2px;
}}

.connection {{
    display: flex;
    flex-direction: column;
    align-items: flex-end;
    gap: 4px;
}}

.conn-row {{
    display: flex;
    align-items: center;
    gap: 7px;
}}

.dot {{
    width: 8px;
    height: 8px;
    border-radius: 50%;
    display: inline-block;
}}

.dot.online {{
    background: {COLOR_NORMAL};
    box-shadow: 0 0 0 0 rgba(43, 212, 125, 0.6);
    animation: pulse-dot 1.8s infinite;
}}

.dot.offline {{
    background: {COLOR_ANOMALY};
}}

@keyframes pulse-dot {{
    0% {{ box-shadow: 0 0 0 0 rgba(43, 212, 125, 0.55); }}
    70% {{ box-shadow: 0 0 0 8px rgba(43, 212, 125, 0); }}
    100% {{ box-shadow: 0 0 0 0 rgba(43, 212, 125, 0); }}
}}

.conn-text {{
    font-size: 12px;
    font-weight: 700;
    letter-spacing: 0.3px;
}}

.online-text {{ color: {COLOR_NORMAL} !important; }}
.offline-text {{ color: {COLOR_ANOMALY} !important; }}

.update {{
    color: {COLOR_MUTED} !important;
    font-size: 11px;
}}

.left-panel {{
    display: flex;
    flex-direction: column;
    gap: 14px;
    width: 100%;
}}

.status-card {{
    display: flex;
    align-items: center;
    gap: 14px;
    padding: 18px;
    border-radius: 16px;
    border: 1px solid {COLOR_BORDER};
    background: {COLOR_PANEL};
    transition: all 0.3s ease;
}}

.status-card.normal {{
    background: linear-gradient(135deg, #2bd47d1a, {COLOR_PANEL});
    border-color: #2bd47d40;
}}

.status-card.anomaly {{
    background: linear-gradient(135deg, #ff5c5c22, {COLOR_PANEL});
    border-color: #ff5c5c55;
    animation: alert-glow 1.6s ease-in-out infinite;
}}

@keyframes alert-glow {{
    0%, 100% {{ box-shadow: 0 0 0 0 rgba(255, 92, 92, 0.0); }}
    50% {{ box-shadow: 0 0 22px 2px rgba(255, 92, 92, 0.25); }}
}}

.status-card.waiting {{
    background: linear-gradient(135deg, #ffcc4d1a, {COLOR_PANEL});
    border-color: #ffcc4d40;
}}

.status-card.detecting {{
    background: linear-gradient(135deg, #7c5cff22, {COLOR_PANEL});
    border-color: #7c5cff55;
    animation: detect-pulse 1.3s ease-in-out infinite;
}}

@keyframes detect-pulse {{
    0%, 100% {{ box-shadow: 0 0 0 0 rgba(124, 92, 255, 0.0); }}
    50% {{ box-shadow: 0 0 18px 2px rgba(124, 92, 255, 0.22); }}
}}

.status-card.inactive {{
    background: {COLOR_PANEL};
    opacity: 0.75;
}}

.status-icon-wrap {{
    width: 50px;
    height: 50px;
    min-width: 50px;
    border-radius: 14px;
    display: flex;
    align-items: center;
    justify-content: center;
    background: rgba(255,255,255,0.05);
}}

.status-card.normal .status-icon-wrap {{ background: {COLOR_NORMAL}22; color: {COLOR_NORMAL}; }}
.status-card.anomaly .status-icon-wrap {{ background: {COLOR_ANOMALY}22; color: {COLOR_ANOMALY}; }}
.status-card.waiting .status-icon-wrap {{ background: {COLOR_WAITING}22; color: {COLOR_WAITING}; }}
.status-card.inactive .status-icon-wrap {{ background: {COLOR_INACTIVE}22; color: {COLOR_INACTIVE}; }}
.status-card.detecting .status-icon-wrap {{ background: {COLOR_DETECTING}22; color: {COLOR_DETECTING}; }}

.status-icon {{
    font-size: 22px;
    font-weight: 800;
}}

.status-label {{
    font-size: 10px;
    color: {COLOR_MUTED} !important;
    font-weight: 700;
    letter-spacing: 0.6px;
}}

.status-value {{
    font-size: 20px;
    font-weight: 800;
    letter-spacing: -0.2px;
    margin-top: 2px;
    color: {COLOR_TEXT} !important;
}}

.status-card.normal .status-value {{ color: {COLOR_NORMAL} !important; }}
.status-card.anomaly .status-value {{ color: {COLOR_ANOMALY} !important; }}
.status-card.waiting .status-value {{ color: {COLOR_WAITING} !important; }}
.status-card.inactive .status-value {{ color: {COLOR_INACTIVE} !important; }}
.status-card.detecting .status-value {{ color: {COLOR_DETECTING} !important; }}

.status-description {{
    font-size: 11px;
    color: {COLOR_MUTED} !important;
    margin-top: 3px;
}}

.sensor-list {{
    background: {COLOR_PANEL};
    border: 1px solid {COLOR_BORDER};
    border-radius: 16px;
    padding: 6px 16px;
}}

.sensor-row {{
    display: flex;
    align-items: center;
    gap: 14px;
    padding: 14px 0;
    border-bottom: 1px solid {COLOR_BORDER};
}}

.sensor-row:last-child {{
    border-bottom: none;
}}

.sensor-icon {{
    width: 36px;
    height: 36px;
    min-width: 36px;
    border-radius: 10px;
    display: flex;
    align-items: center;
    justify-content: center;
    font-size: 16px;
    background: rgba(255,255,255,0.05);
}}

.hr-icon {{ background: {COLOR_HR}1f; }}
.spo2-icon {{ background: {COLOR_SPO2}1f; }}
.temp-icon {{ background: {COLOR_TEMP}1f; }}

.sensor-info {{
    display: flex;
    justify-content: space-between;
    align-items: center;
    flex: 1;
}}

.sensor-name {{
    font-size: 13px;
    color: {COLOR_MUTED} !important;
    font-weight: 500;
}}

.sensor-value {{
    font-size: 21px;
    font-weight: 800;
    color: {COLOR_TEXT} !important;
}}

.sensor-value small {{
    font-size: 10px;
    color: {COLOR_MUTED} !important;
    font-weight: 500;
    margin-left: 3px;
}}

.system-info {{
    background: {COLOR_PANEL};
    border: 1px solid {COLOR_BORDER};
    border-radius: 16px;
    padding: 14px 16px;
}}

.info-title {{
    font-size: 10px;
    font-weight: 700;
    color: {COLOR_MUTED} !important;
    letter-spacing: 0.6px;
    margin-bottom: 8px;
}}

.info-row {{
    display: flex;
    justify-content: space-between;
    padding: 5px 0;
    font-size: 12px;
}}

.info-row span {{ color: {COLOR_MUTED} !important; }}
.info-row strong {{ color: {COLOR_TEXT} !important; font-weight: 600; }}

/* style the actual gr.Plot containers to look like cards */
#hr-plot, #spo2-plot, #temp-plot {{
    background: {COLOR_PANEL} !important;
    border: 1px solid {COLOR_BORDER} !important;
    border-radius: 16px !important;
    padding: 6px !important;
    margin: 0 !important;
}}

#hr-plot {{
    margin-bottom: 14px !important;
}}

footer {{
    display: none !important;
}}

@media (max-width: 900px) {{
    .dash-row {{
        flex-direction: column !important;
    }}
    .left-col, .right-col {{
        width: 100% !important;
        flex: 1 1 100% !important;
    }}
}}

@media (max-width: 600px) {{
    .topbar {{
        flex-direction: column;
        align-items: flex-start;
        gap: 10px;
    }}
    .connection {{
        align-items: flex-start;
    }}
}}
"""


# ============================================================
# GRADIO UI
# ============================================================

with gr.Blocks(title="Physio Monitor", css=CSS, theme=gr.themes.Base()) as demo:

    dashboard_top = gr.HTML(
        value="""
        <div style="padding:24px; text-align:center; color:#8993ab;">
            Menghubungkan ke MQTT...
        </div>
        """
    )

    with gr.Row(elem_classes=["dash-row"]):

        with gr.Column(scale=0, min_width=320, elem_classes=["left-col"]):
            dashboard_left = gr.HTML(value="")

        with gr.Column(scale=3, elem_classes=["right-col"]):
            graph_hr = gr.Plot(label="", elem_id="hr-plot", container=False)

            with gr.Row():
                graph_spo2 = gr.Plot(label="", elem_id="spo2-plot", container=False)
                graph_temp = gr.Plot(label="", elem_id="temp-plot", container=False)

    timer = gr.Timer(value=1)

    timer.tick(
        fn=update_dashboard,
        outputs=[dashboard_top, dashboard_left, graph_hr, graph_spo2, graph_temp],
    )


# ============================================================
# LAUNCH
# ============================================================

if __name__ == "__main__":
    demo.launch(
        server_name="0.0.0.0",
        server_port=7860,
    )