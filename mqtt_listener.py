import json
import ssl
import time
import threading
import paho.mqtt.client as mqtt

import config
import database as db


print("=== mqtt_listener.py LOADED - FIXED VERSION v3 (duracion en ESP32) ===")

_last_watering_ts = 0
_watering_timer = None
_watering_in_progress = False
_watering_duration_sec = 0

# Margen que le damos al ESP32 antes de considerar que algo salio mal y que
# nuestro propio Timer tiene que intervenir. El ESP32 ahora se autoapaga con
# la duracion que le mandamos, asi que este Timer es solo un respaldo.
_WATCHDOG_MARGIN_SEC = 30


def _init_last_watering_ts():
    global _last_watering_ts
    ts = db.get_last_watering_ts()
    if ts is not None:
        _last_watering_ts = ts
        print(f"[AUTO-WATER] Restored last watering ts from DB: {ts}")
    else:
        _last_watering_ts = 0


def _safe_notify(on_data_change):
    if on_data_change is None:
        return
    try:
        on_data_change()
    except Exception as e:
        print(f"[MQTT] Error notifying update: {e}")


def _stop_automatic_watering(client, reason="Duration complete"):
    global _watering_in_progress, _watering_timer
    print(f"[AUTO-WATER] {reason}, stopping pump")
    client.publish(config.TOPIC_COMANDOS, "OFF")
    _watering_in_progress = False

    if _watering_timer:
        _watering_timer.cancel()
        _watering_timer = None


def _evaluate_automatic_watering(data, client):
    global _last_watering_ts, _watering_timer, _watering_in_progress, _watering_duration_sec
    soil = data.get("soil_moisture")
    profile = db.get_plant_profile()
    water_level = data.get("water_level")
    pump_status = data.get("pump_status")

    if water_level == "Empty" and pump_status == "ON":
        _stop_automatic_watering(client, reason="Pump was ON with Empty tank (safety stop)")
        return

    now = time.time()
    hours_since_last = (now - _last_watering_ts) / 3600

    if profile is None:
        if _watering_in_progress:
            pass
        else:
            return

    if _watering_in_progress:
        if water_level == "Empty":
            _stop_automatic_watering(client, reason="Water ran out mid-cycle")
            return

        # Watchdog: el ESP32 ya se autoapaga con la duracion que le mandamos,
        # asi que esto solo deberia disparar si el ESP32 nunca recibio el
        # comando ON con duracion (mensaje corrupto, etc.) y se quedo
        # regando sin limite propio mas alla del safety timeout del firmware.
        elapsed = now - _last_watering_ts
        if elapsed >= _watering_duration_sec + _WATCHDOG_MARGIN_SEC:
            _stop_automatic_watering(
                client,
                reason=f"Watchdog: {elapsed:.0f}s regados, el ESP32 no confirmo el apagado"
            )
        return

    if profile is None or soil is None:
        return

    if water_level == "Empty":
        print("[AUTO-WATER] Water level empty, skipping watering.")
        return

    if soil < profile["moisture_threshold"] and hours_since_last >= profile["min_interval_hours"]:
        try:
            duration_min = float(profile["duration_min"])
        except (TypeError, ValueError):
            print(f"[AUTO-WATER] duration_min invalido en el perfil: {profile.get('duration_min')!r}, usando 2 min por defecto")
            duration_min = 2.0

        duration_sec = duration_min * 60

        print(f"[AUTO-WATER] Soil at {soil}%, threshold {profile['moisture_threshold']}%. "
              f"Starting watering for {duration_sec:.0f}s (duracion enviada al ESP32)...")

        # La duracion viaja en el comando: el ESP32 es quien se autoapaga
        # exactamente a tiempo, sin depender de que le llegue un segundo
        # mensaje OFF por la red.
        comando = json.dumps({"cmd": "ON", "duration_sec": int(duration_sec)})
        client.publish(config.TOPIC_COMANDOS, comando)

        _watering_in_progress = True
        _last_watering_ts = now
        _watering_duration_sec = duration_sec

        # Respaldo: si por lo que sea nunca vemos confirmacion de que se
        # apago, forzamos un OFF nosotros.
        _watering_timer = threading.Timer(
            duration_sec + _WATCHDOG_MARGIN_SEC, _stop_automatic_watering, args=(client,)
        )
        _watering_timer.daemon = True
        _watering_timer.start()


_SYNC_GRACE_SECONDS = 5


def _sync_watering_state(data):
    """Detecta cuando el ESP32 (o Alexa) apago la bomba, para limpiar nuestro
    estado interno. Con el ESP32 autoapagandose por duracion programada, esta
    es ahora la via normal por la que nos enteramos de que el riego termino."""
    global _watering_in_progress, _watering_timer

    if data.get("pump_status") == "OFF" and _watering_in_progress:
        elapsed_since_start = time.time() - _last_watering_ts
        if elapsed_since_start < _SYNC_GRACE_SECONDS:
            print(f"[AUTO-WATER] Ignoring OFF in riego/estado, "
                  f"It's been {elapsed_since_start:.1f}s since started")
            return

        if _watering_timer:
            _watering_timer.cancel()
            _watering_timer = None
        _watering_in_progress = False
        print("[AUTO-WATER] Watering finished/interrupted, state cleared")


def _on_connect(client, userdata, flags, reason_code, properties=None):
    if reason_code == 0:
        print("[MQTT] Connected to broker")
        client.subscribe(config.TOPIC_WILDCARD)
        print(f"[MQTT] Subscribed to: {config.TOPIC_WILDCARD}")
    else:
        print(f"[MQTT] Connection error, code: {reason_code}")


def _on_message(client, userdata, msg):

    on_data_change = userdata
    topic = msg.topic
    payload_raw = msg.payload.decode("utf-8", errors="ignore")
    print(payload_raw)

    try:
        if topic == config.TOPIC_TELEMETRIA:
            data = json.loads(payload_raw)
            db.insert_sensor_reading(data)
            _evaluate_automatic_watering(data, client)
            _safe_notify(on_data_change)

        elif topic == config.TOPIC_ESTADO:
            data = json.loads(payload_raw)
            db.upsert_system_status(data)
            _sync_watering_state(data)
            _safe_notify(on_data_change)

        elif topic == config.TOPIC_ESTADO_ESP:
            db.set_esp32_status(payload_raw)
            _safe_notify(on_data_change)

        elif topic == config.TOPIC_RIEGO_LOG:
            data = json.loads(payload_raw)
            db.insert_irrigation_log(data)
            _safe_notify(on_data_change)

        else:
            print(f"[MQTT] Unhandled topic: {topic} -> {payload_raw}")

    except json.JSONDecodeError:
        print(f"[MQTT] Invalid payload on {topic}: {payload_raw}")
    except Exception as e:
        print(f"[MQTT] Error processing message from {topic}: {e}")


def crear_cliente_mqtt(on_data_change=None):
    client = mqtt.Client(
        client_id=config.MQTT_CLIENT_ID,
        callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
    )
    client.username_pw_set(config.MQTT_USER, config.MQTT_PASSWORD)
    client.tls_set(cert_reqs=ssl.CERT_NONE)
    client.tls_insecure_set(True)
    client.user_data_set(on_data_change)
    client.on_connect = _on_connect
    client.on_message = _on_message
    return client


def iniciar_listener_en_hilo(on_data_change=None):
    _init_last_watering_ts()
    client = crear_cliente_mqtt(on_data_change)
    client.connect(config.MQTT_HOST, config.MQTT_PORT, keepalive=60)
    client.loop_start()
    return client