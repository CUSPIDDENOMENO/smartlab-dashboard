import os
import ssl
import json
import threading
import time
import logging
from typing import Dict, Any, Optional, Tuple

from flask import Flask, jsonify, request, render_template
from flask_socketio import SocketIO
import paho.mqtt.client as mqtt

# ============================================================
# Configuration
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

MQTT_BROKER = os.getenv("MQTT_BROKER", "4bf17dcbb8254da4a64405092f313960.s1.eu.hivemq.cloud")
MQTT_PORT = int(os.getenv("MQTT_PORT", "8883"))
MQTT_USERNAME = os.getenv("MQTT_USERNAME", "smartlab_user")
MQTT_PASSWORD = os.getenv("MQTT_PASSWORD", "SmartLab2026!")

MQTT_STATE_TOPIC = "smartlab/state"
MQTT_CMD_SUB_TOPIC = "smartlab/cmd/#"

# ============================================================
# Flask + SocketIO
# ============================================================

app = Flask(__name__)
app.config["SECRET_KEY"] = "smart_lab_mqtt_dashboard"
socketio = SocketIO(app, cors_allowed_origins="*", async_mode="threading")

mqtt_client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id="smartlab-dashboard")

latest_state: Dict[str, Any] = {
    "connected": False,
    "mqtt_connected": False,
    "source": "mqtt",
    "timestamp": time.time(),
    "message": "Waiting for MQTT data..."
}

state_lock = threading.Lock()

# ============================================================
# Command mapping
# Dashboard command names -> MQTT topic/payload
# ============================================================

def map_command_to_mqtt(name: str, value: bool) -> Optional[Tuple[str, str]]:
    """
    Convert dashboard command requests to MQTT commands expected by smartlab_gateway.py

    Note:
    - Some PLC commands in your gateway are pulse-style actions, not ON/OFF maintained states.
    - For those, only value=True is meaningful.
    """
    mapping = {
        "Cmd_StartTool": ("smartlab/cmd/tool", "START"),
        "Cmd_StopTool": ("smartlab/cmd/tool", "STOP"),
        "Cmd_ResetAlarm": ("smartlab/cmd/alarm", "RESET"),
        "Cmd_FanEnable": ("smartlab/cmd/fan", "ON"),
        "Cmd_UVEnable": ("smartlab/cmd/uv", "ON"),
        "Cmd_DoorUnlock": ("smartlab/cmd/door", "UNLOCK"),
        "Cmd_AutoMode": ("smartlab/cmd/mode", "AUTO"),
        "Cmd_ManualMode": ("smartlab/cmd/mode", "MANUAL"),
        "Cmd_EmergencyStop": ("smartlab/cmd/estop", "ON"),
        "Cmd_Dehumidifier": ("smartlab/cmd/dehumidifier", "ON"),
    }

    if name not in mapping:
        return None

    # For this MQTT architecture, these are action commands.
    # value=False is ignored for pulse/action commands.
    if value is False:
        return None

    return mapping[name]

# ============================================================
# MQTT functions
# ============================================================

def update_latest_state(data: Dict[str, Any]) -> None:
    with state_lock:
        latest_state.clear()
        latest_state.update(data)
        latest_state["mqtt_connected"] = True
        latest_state["source"] = "mqtt"
        if "timestamp" not in latest_state:
            latest_state["timestamp"] = time.time()

def emit_state() -> None:
    with state_lock:
        socketio.emit("state_update", dict(latest_state))

def on_mqtt_connect(client, userdata, flags, reason_code, properties=None):
    if reason_code == 0:
        logging.info("MQTT connected successfully")
        client.subscribe(MQTT_STATE_TOPIC, qos=1)
        logging.info("Subscribed to %s", MQTT_STATE_TOPIC)

        with state_lock:
            latest_state["mqtt_connected"] = True
            latest_state["timestamp"] = time.time()

        emit_state()
    else:
        logging.error("MQTT connection failed with code %s", reason_code)
        with state_lock:
            latest_state["mqtt_connected"] = False
            latest_state["timestamp"] = time.time()
            latest_state["message"] = f"MQTT connect failed: {reason_code}"
        emit_state()

def on_mqtt_disconnect(client, userdata, disconnect_flags, reason_code, properties=None):
    logging.warning("MQTT disconnected: %s", reason_code)
    with state_lock:
        latest_state["mqtt_connected"] = False
        latest_state["timestamp"] = time.time()
        latest_state["message"] = "MQTT disconnected"
    emit_state()

def on_mqtt_message(client, userdata, msg):
    try:
        payload = msg.payload.decode()
        logging.info("MQTT message received on %s", msg.topic)

        if msg.topic == MQTT_STATE_TOPIC:
            data = json.loads(payload)
            update_latest_state(data)
            emit_state()

    except Exception as exc:
        logging.exception("MQTT message handling error: %s", exc)

def init_mqtt():
    mqtt_client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)
    mqtt_client.tls_set(tls_version=ssl.PROTOCOL_TLS_CLIENT)

    mqtt_client.on_connect = on_mqtt_connect
    mqtt_client.on_disconnect = on_mqtt_disconnect
    mqtt_client.on_message = on_mqtt_message

    mqtt_client.connect(MQTT_BROKER, MQTT_PORT, 60)
    mqtt_client.loop_start()

def publish_command(name: str, value: bool) -> Dict[str, Any]:
    mapped = map_command_to_mqtt(name, value)

    if mapped is None:
        return {
            "ok": False,
            "error": f"Unsupported or ignored command: {name}={value}"
        }

    topic, payload = mapped
    result = mqtt_client.publish(topic, payload=payload, qos=1)

    return {
        "ok": True,
        "name": name,
        "value": value,
        "mqtt_topic": topic,
        "mqtt_payload": payload,
        "mid": result.mid
    }

# ============================================================
# Flask routes
# ============================================================

@app.route("/")
def index():
    return render_template("index.html")

@app.route("/api/state", methods=["GET"])
def api_state():
    with state_lock:
        return jsonify(dict(latest_state))

@app.route("/api/command", methods=["POST"])
def api_command():
    payload = request.get_json(force=True)
    name = payload.get("name")
    value = payload.get("value")

    if not isinstance(name, str):
        return jsonify({"ok": False, "error": "Missing or invalid 'name'"}), 400

    if not isinstance(value, bool):
        return jsonify({"ok": False, "error": "Missing or invalid 'value' (must be true/false)"}), 400

    try:
        result = publish_command(name, value)
        status_code = 200 if result["ok"] else 400
        return jsonify(result), status_code
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc)}), 500

# ============================================================
# SocketIO
# ============================================================

@socketio.on("connect")
def handle_connect():
    emit_state()

@socketio.on("set_command")
def handle_set_command(data):
    try:
        name = data.get("name")
        value = data.get("value")

        if not isinstance(name, str) or not isinstance(value, bool):
            socketio.emit("command_result", {
                "ok": False,
                "error": "Invalid command payload"
            })
            return

        result = publish_command(name, value)
        socketio.emit("command_result", result)

    except Exception as exc:
        socketio.emit("command_result", {
            "ok": False,
            "error": str(exc)
        })

# ============================================================
# Main
# ============================================================

if __name__ == "__main__":
    init_mqtt()
    socketio.run(app, host="0.0.0.0", port=5000, debug=True, use_reloader=False, allow_unsafe_werkzeug=True)
