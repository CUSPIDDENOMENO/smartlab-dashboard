import json
import logging
import random
import ssl
import struct
import threading
import time
from dataclasses import dataclass
from typing import Dict, Any, Tuple

from flask import Flask, jsonify, request, render_template
from flask_socketio import SocketIO
import paho.mqtt.client as mqtt
import snap7


# ============================================================
# Configuration
# ============================================================

PLC_IP = "192.168.1.101"
PLC_RACK = 0
PLC_SLOT = 1
DB_NUMBER = 100
DB_SIZE = 26

SIMULATION_CYCLE_SEC = 0.5
RECONNECT_DELAY_SEC = 2.0

# ============================================================
# MQTT Cloud Configuration
# ============================================================

MQTT_BROKER = "4bf17dcbb8254da4a64405092f313960.s1.eu.hivemq.cloud"
MQTT_PORT = 8883
MQTT_USERNAME = "smartlab_user"
MQTT_PASSWORD = "Smartlab2026!"   # use the exact real password you want

MQTT_STATE_TOPIC = "smartlab/state"
MQTT_CMD_TOPIC = "smartlab/cmd/#"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)


# ============================================================
# DB Mapping
# ============================================================

class DBMap:
    CMD_START_TOOL = (0, 0)
    CMD_STOP_TOOL = (0, 1)
    CMD_RESET_ALARM = (0, 2)
    CMD_FAN_ENABLE = (0, 3)
    CMD_UV_ENABLE = (0, 4)
    CMD_DOOR_UNLOCK = (0, 5)
    CMD_AUTO_MODE = (0, 6)
    CMD_MANUAL_MODE = (0, 7)

    CMD_EMERGENCY_STOP = (1, 0)
    CMD_DEHUMIDIFIER = (1, 1)

    STS_TOOL_RUNNING = (1, 2)
    STS_DOOR_OPEN = (1, 3)
    STS_FAN_RUNNING = (1, 4)
    STS_UV_ON = (1, 5)
    STS_ALARM_ACTIVE = (1, 6)
    STS_ESTOP = (1, 7)

    STS_SYSTEM_READY = (2, 0)
    STS_AUTO_MODE = (2, 1)
    STS_MANUAL_MODE = (2, 2)

    VAL_TEMPERATURE = 4
    VAL_HUMIDITY = 8
    VAL_PRESSURE = 12
    VAL_AIRFLOW = 16
    VAL_PARTICLE_COUNT = 20

    ALM_DOOR_OPEN_DURING_RUN = (24, 0)
    ALM_LOW_PRESSURE = (24, 1)
    ALM_HIGH_PARTICLES = (24, 2)
    ALM_FAN_FAILURE = (24, 3)
    ALM_EMERGENCY_STOP = (24, 4)
    ALM_HIGH_TEMPERATURE = (24, 5)
    ALM_HIGH_HUMIDITY = (24, 6)
    ALM_LOW_AIRFLOW = (24, 7)

    PERM_TOOL_RUN = (25, 0)
    PERM_START_ALLOWED = (25, 1)
    PERM_SAFE_STATE = (25, 2)


COMMAND_BITS: Dict[str, Tuple[int, int]] = {
    "Cmd_StartTool": DBMap.CMD_START_TOOL,
    "Cmd_StopTool": DBMap.CMD_STOP_TOOL,
    "Cmd_ResetAlarm": DBMap.CMD_RESET_ALARM,
    "Cmd_FanEnable": DBMap.CMD_FAN_ENABLE,
    "Cmd_UVEnable": DBMap.CMD_UV_ENABLE,
    "Cmd_DoorUnlock": DBMap.CMD_DOOR_UNLOCK,
    "Cmd_AutoMode": DBMap.CMD_AUTO_MODE,
    "Cmd_ManualMode": DBMap.CMD_MANUAL_MODE,
    "Cmd_EmergencyStop": DBMap.CMD_EMERGENCY_STOP,
    "Cmd_Dehumidifier": DBMap.CMD_DEHUMIDIFIER,
}

PULSE_COMMANDS = {
    "Cmd_StartTool",
    "Cmd_StopTool",
    "Cmd_ResetAlarm",
    "Cmd_AutoMode",
    "Cmd_ManualMode",
}

MQTT_COMMANDS = {
    "smartlab/cmd/tool": {
        "START": "Cmd_StartTool",
        "STOP": "Cmd_StopTool",
    },
    "smartlab/cmd/alarm": {
        "RESET": "Cmd_ResetAlarm",
    },
    "smartlab/cmd/fan": {
        "ON": ("Cmd_FanEnable", True),
        "OFF": ("Cmd_FanEnable", False),
    },
    "smartlab/cmd/uv": {
        "ON": ("Cmd_UVEnable", True),
        "OFF": ("Cmd_UVEnable", False),
    },
    "smartlab/cmd/door": {
        "UNLOCK": ("Cmd_DoorUnlock", True),
        "LOCK": ("Cmd_DoorUnlock", False),
        "OPEN": ("Cmd_DoorUnlock", True),
        "CLOSE": ("Cmd_DoorUnlock", False),
    },
    "smartlab/cmd/mode": {
        "AUTO": "Cmd_AutoMode",
        "MANUAL": "Cmd_ManualMode",
    },
    "smartlab/cmd/estop": {
        "ON": ("Cmd_EmergencyStop", True),
        "OFF": ("Cmd_EmergencyStop", False),
    },
    "smartlab/cmd/dehumidifier": {
        "ON": ("Cmd_Dehumidifier", True),
        "OFF": ("Cmd_Dehumidifier", False),
    },
}


# ============================================================
# Utility helpers
# ============================================================

def get_bool(data: bytes, byte_index: int, bit_index: int) -> bool:
    return bool((data[byte_index] >> bit_index) & 0x01)


def set_bool_in_buffer(buf: bytearray, byte_index: int, bit_index: int, value: bool) -> None:
    if value:
        buf[byte_index] |= (1 << bit_index)
    else:
        buf[byte_index] &= ~(1 << bit_index)


def get_real(data: bytes, byte_index: int) -> float:
    return struct.unpack(">f", data[byte_index:byte_index + 4])[0]


def pack_real(value: float) -> bytes:
    return struct.pack(">f", float(value))


def get_dint(data: bytes, byte_index: int) -> int:
    return struct.unpack(">i", data[byte_index:byte_index + 4])[0]


def pack_dint(value: int) -> bytes:
    return struct.pack(">i", int(value))


def clamp(value: float, low: float, high: float) -> float:
    return max(low, min(high, value))


def move_towards(current: float, target: float, speed: float, dt: float, noise: float = 0.0) -> float:
    alpha = clamp(speed * dt, 0.0, 1.0)
    new_value = current + (target - current) * alpha
    if noise > 0.0:
        new_value += random.uniform(-noise, noise)
    return new_value


# ============================================================
# Simulation
# ============================================================

@dataclass
class SimState:
    temperature: float = 22.0
    humidity: float = 45.0
    pressure: float = 15.0
    airflow: float = 0.50
    particle_count: int = 50

    door_open: bool = False
    fan_running: bool = False
    estop_active: bool = False
    system_ready: bool = True

    fan_start_timer: float = 0.0
    fan_stop_timer: float = 0.0
    door_open_timer: float = 0.0
    door_close_timer: float = 0.0

    def update(self, plc_state: Dict[str, Any], dt: float) -> None:
        cmd_fan_enable = plc_state["Cmd_FanEnable"]
        cmd_door_unlock = plc_state["Cmd_DoorUnlock"]
        cmd_dehumidifier = plc_state["Cmd_Dehumidifier"]
        cmd_estop = plc_state["Cmd_EmergencyStop"]

        sts_tool_running = plc_state["Sts_ToolRunning"]
        sts_uv_on = plc_state["Sts_UVOn"]

        self.estop_active = cmd_estop

        if cmd_door_unlock:
            self.door_open_timer += dt
            self.door_close_timer = 0.0
            if self.door_open_timer >= 0.5:
                self.door_open = True
        else:
            self.door_close_timer += dt
            self.door_open_timer = 0.0
            if self.door_close_timer >= 0.8:
                self.door_open = False

        if cmd_fan_enable:
            self.fan_start_timer += dt
            self.fan_stop_timer = 0.0
            if self.fan_start_timer >= 1.2:
                self.fan_running = True
        else:
            self.fan_stop_timer += dt
            self.fan_start_timer = 0.0
            if self.fan_stop_timer >= 0.4:
                self.fan_running = False

        self.system_ready = not self.estop_active

        target_temp = 22.0
        target_humidity = 45.0
        target_pressure = 15.0
        target_airflow = 0.50
        target_particles = 50.0

        if self.door_open:
            target_temp += 0.8
            target_humidity += 10.0
            target_pressure -= 8.0
            target_airflow -= 0.10
            target_particles += 130.0

        if self.fan_running:
            target_pressure += 2.0
            target_airflow += 0.12
            target_particles -= 15.0
            target_humidity -= 1.0
        else:
            target_pressure -= 6.5
            target_airflow -= 0.35
            target_particles += 25.0

        if sts_tool_running:
            target_temp += 4.0
            target_humidity += 3.0
            target_particles += 40.0

        if cmd_dehumidifier:
            target_humidity -= 12.0

        if sts_uv_on and not self.door_open:
            target_particles -= 20.0

        target_temp = clamp(target_temp, 18.0, 35.0)
        target_humidity = clamp(target_humidity, 25.0, 80.0)
        target_pressure = clamp(target_pressure, 2.0, 20.0)
        target_airflow = clamp(target_airflow, 0.05, 1.20)
        target_particles = clamp(target_particles, 20.0, 450.0)

        self.temperature = move_towards(self.temperature, target_temp, 0.35, dt, 0.04)
        self.humidity = move_towards(self.humidity, target_humidity, 0.30, dt, 0.12)
        self.pressure = move_towards(self.pressure, target_pressure, 0.55, dt, 0.08)
        self.airflow = move_towards(self.airflow, target_airflow, 0.60, dt, 0.015)

        particles_float = move_towards(float(self.particle_count), target_particles, 0.40, dt, 2.0)
        self.particle_count = int(round(clamp(particles_float, 0.0, 1000.0)))

        self.temperature = round(clamp(self.temperature, 0.0, 100.0), 2)
        self.humidity = round(clamp(self.humidity, 0.0, 100.0), 2)
        self.pressure = round(clamp(self.pressure, 0.0, 100.0), 2)
        self.airflow = round(clamp(self.airflow, 0.0, 10.0), 3)


# ============================================================
# Backend
# ============================================================

class SmartLabBackend:
    def __init__(self, socketio: SocketIO) -> None:
        self.client = snap7.client.Client()
        self.connected = False
        self.lock = threading.Lock()
        self.sim = SimState()
        self.socketio = socketio

        self.latest_state: Dict[str, Any] = {
            "connected": False,
            "plc_ip": PLC_IP,
            "timestamp": time.time(),
        }

        self.mqtt_client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id="smartlab-gateway")
        self._configure_mqtt()

    def _configure_mqtt(self) -> None:
        self.mqtt_client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)
        self.mqtt_client.tls_set(tls_version=ssl.PROTOCOL_TLS_CLIENT)
        self.mqtt_client.on_connect = self._on_mqtt_connect
        self.mqtt_client.on_message = self._on_mqtt_message

    def start_mqtt(self) -> None:
        try:
            self.mqtt_client.connect(MQTT_BROKER, MQTT_PORT, 60)
            self.mqtt_client.loop_start()
            logging.info("MQTT initialized")
        except Exception as exc:
            logging.warning("MQTT init error: %s", exc)

    def _on_mqtt_connect(self, client, userdata, flags, reason_code, properties=None):
        if reason_code == 0:
            logging.info("MQTT connected successfully")
            client.subscribe(MQTT_CMD_TOPIC, qos=1)
            logging.info("MQTT subscribed to %s", MQTT_CMD_TOPIC)
        else:
            logging.warning("MQTT connection failed with code %s", reason_code)

    def _on_mqtt_message(self, client, userdata, msg):
        try:
            topic = msg.topic
            payload = msg.payload.decode().strip().upper()

            if topic not in MQTT_COMMANDS or payload not in MQTT_COMMANDS[topic]:
                logging.warning("Unknown MQTT command: topic=%s payload=%s", topic, payload)
                return

            mapped = MQTT_COMMANDS[topic][payload]
            if isinstance(mapped, str):
                self.write_or_pulse_command(mapped, True)
            else:
                name, value = mapped
                self.write_or_pulse_command(name, value)

        except Exception as exc:
            logging.warning("MQTT message error: %s", exc)

    def publish_state(self) -> None:
        try:
            self.mqtt_client.publish(MQTT_STATE_TOPIC, payload=json.dumps(self.latest_state), qos=1, retain=True)
        except Exception as exc:
            logging.warning("MQTT publish error: %s", exc)

    def connect(self) -> None:
        if self.connected:
            return
        try:
            self.client.connect(PLC_IP, PLC_RACK, PLC_SLOT)
            self.connected = self.client.get_connected()
            logging.info("Connected to PLC: %s", self.connected)
        except Exception as exc:
            self.connected = False
            logging.warning("PLC connection failed: %s", exc)

    def disconnect(self) -> None:
        try:
            if self.client.get_connected():
                self.client.disconnect()
        except Exception:
            pass
        self.connected = False

    def read_db(self) -> bytes:
        return self.client.db_read(DB_NUMBER, 0, DB_SIZE)

    def write_db_slice(self, start: int, data: bytes) -> None:
        self.client.db_write(DB_NUMBER, start, data)

    def write_command_bit(self, name: str, value: bool) -> None:
        if name not in COMMAND_BITS:
            raise ValueError(f"Unknown command: {name}")

        byte_index, bit_index = COMMAND_BITS[name]
        with self.lock:
            snapshot = bytearray(self.read_db())
            set_bool_in_buffer(snapshot, byte_index, bit_index, value)
            self.write_db_slice(byte_index, bytes([snapshot[byte_index]]))

    def pulse_command_bit(self, name: str, duration: float = 0.8) -> None:
        if name not in COMMAND_BITS:
            raise ValueError(f"Unknown command: {name}")

        byte_index, bit_index = COMMAND_BITS[name]

        with self.lock:
            snapshot = bytearray(self.read_db())
            set_bool_in_buffer(snapshot, byte_index, bit_index, True)
            self.write_db_slice(byte_index, bytes([snapshot[byte_index]]))

        time.sleep(duration)

        with self.lock:
            snapshot = bytearray(self.read_db())
            set_bool_in_buffer(snapshot, byte_index, bit_index, False)
            self.write_db_slice(byte_index, bytes([snapshot[byte_index]]))

    def write_or_pulse_command(self, name: str, value: bool) -> None:
        if name in PULSE_COMMANDS and value:
            self.pulse_command_bit(name)
        else:
            self.write_command_bit(name, value)

    def parse_plc_state(self, data: bytes) -> Dict[str, Any]:
        return {
            "Cmd_StartTool": get_bool(data, *DBMap.CMD_START_TOOL),
            "Cmd_StopTool": get_bool(data, *DBMap.CMD_STOP_TOOL),
            "Cmd_ResetAlarm": get_bool(data, *DBMap.CMD_RESET_ALARM),
            "Cmd_FanEnable": get_bool(data, *DBMap.CMD_FAN_ENABLE),
            "Cmd_UVEnable": get_bool(data, *DBMap.CMD_UV_ENABLE),
            "Cmd_DoorUnlock": get_bool(data, *DBMap.CMD_DOOR_UNLOCK),
            "Cmd_AutoMode": get_bool(data, *DBMap.CMD_AUTO_MODE),
            "Cmd_ManualMode": get_bool(data, *DBMap.CMD_MANUAL_MODE),
            "Cmd_EmergencyStop": get_bool(data, *DBMap.CMD_EMERGENCY_STOP),
            "Cmd_Dehumidifier": get_bool(data, *DBMap.CMD_DEHUMIDIFIER),

            "Sts_ToolRunning": get_bool(data, *DBMap.STS_TOOL_RUNNING),
            "Sts_DoorOpen": get_bool(data, *DBMap.STS_DOOR_OPEN),
            "Sts_FanRunning": get_bool(data, *DBMap.STS_FAN_RUNNING),
            "Sts_UVOn": get_bool(data, *DBMap.STS_UV_ON),
            "Sts_AlarmActive": get_bool(data, *DBMap.STS_ALARM_ACTIVE),
            "Sts_EStop": get_bool(data, *DBMap.STS_ESTOP),
            "Sts_SystemReady": get_bool(data, *DBMap.STS_SYSTEM_READY),
            "Sts_AutoMode": get_bool(data, *DBMap.STS_AUTO_MODE),
            "Sts_ManualMode": get_bool(data, *DBMap.STS_MANUAL_MODE),

            "Val_Temperature": get_real(data, DBMap.VAL_TEMPERATURE),
            "Val_Humidity": get_real(data, DBMap.VAL_HUMIDITY),
            "Val_Pressure": get_real(data, DBMap.VAL_PRESSURE),
            "Val_Airflow": get_real(data, DBMap.VAL_AIRFLOW),
            "Val_ParticleCount": get_dint(data, DBMap.VAL_PARTICLE_COUNT),

            "Alm_DoorOpenDuringRun": get_bool(data, *DBMap.ALM_DOOR_OPEN_DURING_RUN),
            "Alm_LowPressure": get_bool(data, *DBMap.ALM_LOW_PRESSURE),
            "Alm_HighParticles": get_bool(data, *DBMap.ALM_HIGH_PARTICLES),
            "Alm_FanFailure": get_bool(data, *DBMap.ALM_FAN_FAILURE),
            "Alm_EmergencyStop": get_bool(data, *DBMap.ALM_EMERGENCY_STOP),
            "Alm_HighTemperature": get_bool(data, *DBMap.ALM_HIGH_TEMPERATURE),
            "Alm_HighHumidity": get_bool(data, *DBMap.ALM_HIGH_HUMIDITY),
            "Alm_LowAirFlow": get_bool(data, *DBMap.ALM_LOW_AIRFLOW),

            "Perm_ToolRun": get_bool(data, *DBMap.PERM_TOOL_RUN),
            "Perm_StartAllowed": get_bool(data, *DBMap.PERM_START_ALLOWED),
            "Perm_SafeState": get_bool(data, *DBMap.PERM_SAFE_STATE),
        }

    def write_simulated_feedback(self, snapshot: bytes) -> None:
        buf = bytearray(snapshot)

        set_bool_in_buffer(buf, *DBMap.STS_DOOR_OPEN, self.sim.door_open)
        set_bool_in_buffer(buf, *DBMap.STS_FAN_RUNNING, self.sim.fan_running)
        set_bool_in_buffer(buf, *DBMap.STS_ESTOP, self.sim.estop_active)
        set_bool_in_buffer(buf, *DBMap.STS_SYSTEM_READY, self.sim.system_ready)

        self.write_db_slice(1, bytes([buf[1]]))
        self.write_db_slice(2, bytes([buf[2]]))
        self.write_db_slice(DBMap.VAL_TEMPERATURE, pack_real(self.sim.temperature))
        self.write_db_slice(DBMap.VAL_HUMIDITY, pack_real(self.sim.humidity))
        self.write_db_slice(DBMap.VAL_PRESSURE, pack_real(self.sim.pressure))
        self.write_db_slice(DBMap.VAL_AIRFLOW, pack_real(self.sim.airflow))
        self.write_db_slice(DBMap.VAL_PARTICLE_COUNT, pack_dint(self.sim.particle_count))

    def cycle(self, dt: float) -> None:
        snapshot = self.read_db()
        plc_state = self.parse_plc_state(snapshot)
        self.sim.update(plc_state, dt)
        self.write_simulated_feedback(snapshot)

        final_snapshot = self.read_db()
        final_state = self.parse_plc_state(final_snapshot)

        self.latest_state = {
            "connected": True,
            "plc_ip": PLC_IP,
            "timestamp": time.time(),
            **final_state,
            "Sim_DoorOpen": self.sim.door_open,
            "Sim_FanRunning": self.sim.fan_running,
            "Sim_EStop": self.sim.estop_active,
            "Sim_SystemReady": self.sim.system_ready,
        }

    def run_forever(self) -> None:
        while True:
            try:
                if not self.connected:
                    self.connect()
                    if not self.connected:
                        self.latest_state = {
                            "connected": False,
                            "plc_ip": PLC_IP,
                            "timestamp": time.time(),
                        }
                        self.socketio.emit("state_update", self.latest_state)
                        self.publish_state()
                        time.sleep(RECONNECT_DELAY_SEC)
                        continue

                self.cycle(SIMULATION_CYCLE_SEC)
                self.socketio.emit("state_update", self.latest_state)
                self.publish_state()
                time.sleep(SIMULATION_CYCLE_SEC)

            except Exception as exc:
                logging.exception("Backend loop error: %s", exc)
                self.disconnect()
                self.latest_state = {
                    "connected": False,
                    "plc_ip": PLC_IP,
                    "timestamp": time.time(),
                    "error": str(exc),
                }
                self.socketio.emit("state_update", self.latest_state)
                self.publish_state()
                time.sleep(RECONNECT_DELAY_SEC)


# ============================================================
# Flask + SocketIO
# ============================================================

app = Flask(__name__)
app.config["SECRET_KEY"] = "smart_lab_secret_key"
socketio = SocketIO(app, cors_allowed_origins="*", async_mode="threading")
backend = SmartLabBackend(socketio)


@app.route("/")
def index():
    return render_template("index.html")


@app.route("/api/state", methods=["GET"])
def api_state():
    return jsonify(backend.latest_state)


@app.route("/api/command", methods=["POST"])
def api_command():
    payload = request.get_json(force=True)
    name = payload.get("name")
    value = payload.get("value")

    if not isinstance(name, str):
        return jsonify({"ok": False, "error": "Missing or invalid 'name'"}), 400

    if not isinstance(value, bool):
        return jsonify({"ok": False, "error": "Missing or invalid 'value'"}), 400

    try:
        if not backend.connected:
            return jsonify({"ok": False, "error": "PLC not connected"}), 503

        backend.write_or_pulse_command(name, value)
        return jsonify({"ok": True, "name": name, "value": value})

    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc)}), 500


@socketio.on("connect")
def handle_connect():
    socketio.emit("state_update", backend.latest_state)


@socketio.on("set_command")
def handle_set_command(data):
    try:
        name = data.get("name")
        value = data.get("value")

        if not isinstance(name, str) or not isinstance(value, bool):
            socketio.emit("command_result", {"ok": False, "error": "Invalid command payload"})
            return

        if not backend.connected:
            socketio.emit("command_result", {"ok": False, "error": "PLC not connected"})
            return

        backend.write_or_pulse_command(name, value)
        socketio.emit("command_result", {"ok": True, "name": name, "value": value})

    except Exception as exc:
        socketio.emit("command_result", {"ok": False, "error": str(exc)})


def start_backend_thread() -> None:
    thread = threading.Thread(target=backend.run_forever, daemon=True)
    thread.start()


if __name__ == "__main__":
    backend.start_mqtt()
    start_backend_thread()
    socketio.run(app, host="0.0.0.0", port=5000, debug=True, use_reloader=False)
