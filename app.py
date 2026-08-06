from decimal import Decimal

from flask import Flask, jsonify
from flask_cors import CORS
from flask_socketio import SocketIO

import config
import database as db
from mqtt_listener import iniciar_listener_en_hilo

app = Flask(__name__)
CORS(app)

socketio = SocketIO(app, cors_allowed_origins="*", async_mode="eventlet")


def _json_safe(value):
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, dict):
        return {k: _json_safe(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_json_safe(v) for v in value]
    return value


def build_dashboard_payload():
    ultima = db.get_last_reading() or {}
    estado = db.get_system_status() or {}
    grafica = db.get_soil_moisture_per_hour(horas=12)

    payload = {
        "soil_moisture": ultima.get("soil_moisture"),
        "temperature": ultima.get("temperature"),
        "pump_status": ultima.get("pump_status", "OFF"),
        "water_deposit": ultima.get("water_level", "Desconocido"),
        "soil_moisture_per_hour": grafica,
        "system_status": {
            "esp32_online": estado.get("esp32") == "online",
            "mqtt_connected": estado.get("mqtt") == "online",
            "pump_available": estado.get("pump_available") == "true",
            "sensors_working": estado.get("sensors_ok") == "true",
        },
    }
    return _json_safe(payload)


def notify_dashboard_update():
    socketio.emit("dashboard_update", build_dashboard_payload())


@app.route("/api/dashboard", methods=["GET"])
def dashboard():
    return jsonify(build_dashboard_payload())


@app.route("/api/overview", methods=["GET"])
def overview():
    ultima = db.get_last_reading() or {}
    ultimo_riego = db.get_last_irrigation() or {}

    return jsonify(_json_safe({
        "run_time_min": ultimo_riego.get("duration"),
        "started": ultimo_riego.get("start_time"),
        "ends": ultimo_riego.get("end_time"),
        "water_deposit": ultima.get("water_level"),
        "temperature": ultima.get("temperature"),
        "soil_moisture": ultima.get("soil_moisture"),
    }))


@app.route("/api/history", methods=["GET"])
def history():
    return jsonify(_json_safe(db.get_history(limite=20)))


@app.route("/api/status", methods=["GET"])
def status():
    return jsonify(_json_safe(db.get_system_status() or {}))


@socketio.on("connect")
def handle_connect():
    socketio.emit("dashboard_update", build_dashboard_payload())


@socketio.on("disconnect")
def handle_disconnect():
    pass


if __name__ == "__main__":
    iniciar_listener_en_hilo()
    socketio.run(
        app,
        host=config.API_HOST,
        port=config.API_PORT,
        debug=True,
        use_reloader=False,
    )
