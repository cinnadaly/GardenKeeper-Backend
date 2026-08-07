from decimal import Decimal

from flask import Flask, jsonify, request
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
    """Se llama cada vez que llega un mensaje MQTT que puede cambiar el
    dashboard, overview o history: emite el payload actualizado a todos
    los clientes conectados por Socket.IO."""
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


PROFILE_PRESETS = {
    "small":  {"name": "Small",  "moisture_threshold": 35, "duration_min": 2, "water_usage": "Low"},
    "medium": {"name": "Medium", "moisture_threshold": 41, "duration_min": 4, "water_usage": "Medium"},
    "large":  {"name": "Large",  "moisture_threshold": 55, "duration_min": 6, "water_usage": "High"},
}

@app.route("/api/plant", methods=["GET"])
def get_plant():
    perfil = db.get_plant_profile()
    if perfil is None:
        return jsonify({"configured": False})
    return jsonify({"configured": True, **perfil})


@app.route("/api/plant", methods=["POST"])
def save_plant():
    if db.get_plant_profile() is not None:
        return jsonify({"error": "The plant has already been set and cannot be modified"}), 409

    data = request.json or {}
    size = data.get("size")

    if size not in PROFILE_PRESETS:
        return jsonify({"error": "Invalid profile size"}), 400

    perfil = PROFILE_PRESETS[size]
    db.create_plant_profile({"size": size, **perfil})
    return jsonify({"status": "ok", "size": size, **perfil})


if __name__ == "__main__":
    iniciar_listener_en_hilo(on_data_change=notify_dashboard_update)
    socketio.run(
        app,
        host=config.API_HOST,
        port=config.API_PORT,
        debug=True,
        use_reloader=False,
    )
