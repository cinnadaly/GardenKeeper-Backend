"""
Server principal: arranca el listener MQTT y expone un API REST
con los datos que necesitan las 3 pantallas del dashboard.

Correr con:  python app.py
"""

from flask import Flask, jsonify
from flask_cors import CORS

import config
import database as db
from mqtt_listener import iniciar_listener_en_hilo

app = Flask(__name__)
CORS(app)  # para que el frontend (en otro puerto/dominio) pueda consumir el API


# ==================================================
# GET /api/dashboard
# Pantalla: Dashboard
# ==================================================
@app.route("/api/dashboard", methods=["GET"])
def dashboard():
    ultima = db.get_last_reading() or {}
    estado = db.get_system_status() or {}
    grafica = db.get_soil_moisture_per_hour(horas=12)

    return jsonify({
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
    })


# ==================================================
# GET /api/overview
# Pantalla: System Overview
# ==================================================
@app.route("/api/overview", methods=["GET"])
def overview():
    ultima = db.get_last_reading() or {}
    ultimo_riego = db.get_last_irrigation() or {}

    return jsonify({
        "run_time_min": ultimo_riego.get("duration"),
        "started": ultimo_riego.get("start_time"),
        "ends": ultimo_riego.get("end_time"),
        "water_deposit": ultima.get("water_level"),
        "temperature": ultima.get("temperature"),
        "soil_moisture": ultima.get("soil_moisture"),
    })


# ==================================================
# GET /api/history
# Pantalla: System History
# ==================================================
@app.route("/api/history", methods=["GET"])
def history():
    return jsonify(db.get_history(limite=20))


# ==================================================
# GET /api/status
# Util para debug rapido del estado crudo del sistema
# ==================================================
@app.route("/api/status", methods=["GET"])
def status():
    return jsonify(db.get_system_status() or {})


if __name__ == "__main__":
    #db.init_db()
    iniciar_listener_en_hilo()
    app.run(host=config.API_HOST, port=config.API_PORT, debug=True, use_reloader=False)
    # use_reloader=False para que no se abran dos conexiones MQTT duplicadas
