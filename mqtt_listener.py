import json
import ssl
import time
import threading
import paho.mqtt.client as mqtt

import config
import database as db


print("=== mqtt_listener.py LOADED - FIXED VERSION v2 ===")

_last_watering_ts = 0
_watering_timer = None
_watering_in_progress = False


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

    # Cancel any pending timer so a delayed stop can't fire again later
    if _watering_timer:
        _watering_timer.cancel()
        _watering_timer = None

def _evaluate_automatic_watering(data, client):
    global _last_watering_ts, _watering_timer, _watering_in_progress
    soil = data.get("soil_moisture")
    profile = db.get_plant_profile()
    water_level = data.get("water_level")  # telemetria now sends this key directly
    pump_status = data.get("pump_status")

    # Safety net: if the pump is physically ON and the tank is Empty,
    # force it off regardless of what our internal state thinks happened
    # (covers restarts, desync, or a cycle started before this fix was loaded).
    if water_level == "Empty" and pump_status == "ON":
        _stop_automatic_watering(client, reason="Pump was ON with Empty tank (safety stop)")
        return

    now = time.time()
    hours_since_last = (now - _last_watering_ts) / 3600

    print("MESSAGE: soil" + str(soil) + " - moisture threshold " + str(profile["moisture_threshold"]) +
          " - hours since last " + str(hours_since_last) + " - min-interval-hours: " + str(profile["min_interval_hours"]))

    # If already watering, check whether water ran out mid-cycle
    if _watering_in_progress:
        if water_level == "Empty":
            _stop_automatic_watering(client, reason="Water ran out mid-cycle")
        return  # already watering (or just stopped), skip further evaluation

    if profile is None:
        return  # plant not configured yet

    if soil is None:
        return

    print("WATER LEVEL CURRENT: " + str(water_level))

    # Don't even consider starting if there's no water
    if water_level == "Empty":
        print("[AUTO-WATER] Water level empty, skipping watering.")
        return

    if soil < profile["moisture_threshold"] and hours_since_last >= profile["min_interval_hours"]:
        print(f"[AUTO-WATER] Soil at {soil}%, threshold {profile['moisture_threshold']}%. Starting watering...")

        client.publish(config.TOPIC_COMANDOS, "ON")
        _watering_in_progress = True
        _last_watering_ts = now

        duration_sec = profile["duration_min"] * 60
        _watering_timer = threading.Timer(duration_sec, _stop_automatic_watering, args=(client,))
        _watering_timer.daemon = True
        _watering_timer.start()


def _sync_watering_state(data):
    """If Alexa or anyone else stops the pump before the automatic timer
    finishes, cancel the timer to avoid an inconsistent state."""
    global _watering_in_progress, _watering_timer

    if data.get("pump_status") == "OFF" and _watering_in_progress:
        if _watering_timer:
            _watering_timer.cancel()
        _watering_in_progress = False
        print("[AUTO-WATER] Watering interrupted externally, timer cancelled")


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

def printMyMessage():
    print("WE ARE ON MESSAGE")

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
    #client.on_message = printMyMessage()
    return client


def iniciar_listener_en_hilo(on_data_change=None):
    client = crear_cliente_mqtt(on_data_change)
    client.connect(config.MQTT_HOST, config.MQTT_PORT, keepalive=60)
    client.loop_start()
    return client