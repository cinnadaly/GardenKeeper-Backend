import json
import ssl
import paho.mqtt.client as mqtt

import config
import database as db


def _safe_notify(on_data_change):
    """Llama al callback protegiendo el listener MQTT: si el emit falla
    (por ejemplo si Socket.IO no está listo todavía), no debe tumbar la
    conexión MQTT."""
    if on_data_change is None:
        return
    try:
        on_data_change()
    except Exception as e:
        print(f"[MQTT] Error notificando actualizacion: {e}")


def _on_connect(client, userdata, flags, reason_code, properties=None):
    if reason_code == 0:
        print("[MQTT] Conectado al broker")
        client.subscribe(config.TOPIC_WILDCARD)
        print(f"[MQTT] Suscrito a: {config.TOPIC_WILDCARD}")
    else:
        print(f"[MQTT] Error de conexion, codigo: {reason_code}")


def _on_message(client, userdata, msg):
    on_data_change = userdata  # callback pasado en client.user_data_set()
    topic = msg.topic
    payload_raw = msg.payload.decode("utf-8", errors="ignore")
    print(payload_raw)
    try:
        if topic == config.TOPIC_TELEMETRIA:
            data = json.loads(payload_raw)
            db.insert_sensor_reading(data)
            _safe_notify(on_data_change)

        elif topic == config.TOPIC_ESTADO:
            data = json.loads(payload_raw)
            db.upsert_system_status(data)
            _safe_notify(on_data_change)

        elif topic == config.TOPIC_ESTADO_ESP:
            db.set_esp32_status(payload_raw)
            _safe_notify(on_data_change)

        elif topic == config.TOPIC_RIEGO_LOG:
            data = json.loads(payload_raw)
            db.insert_irrigation_log(data)
            _safe_notify(on_data_change)

        else:
            print(f"[MQTT] Topico sin manejar: {topic} -> {payload_raw}")

    except json.JSONDecodeError:
        print(f"[MQTT] Payload invalido en {topic}: {payload_raw}")
    except Exception as e:
        print(f"[MQTT] Error procesando mensaje de {topic}: {e}")


def crear_cliente_mqtt(on_data_change=None):
    client = mqtt.Client(
        client_id=config.MQTT_CLIENT_ID,
        callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
    )
    client.username_pw_set(config.MQTT_USER, config.MQTT_PASSWORD)

    client.tls_set(cert_reqs=ssl.CERT_NONE)
    client.tls_insecure_set(True)

    # Guardamos el callback como "user_data" para poder leerlo dentro de
    # _on_message sin depender de variables globales.
    client.user_data_set(on_data_change)

    client.on_connect = _on_connect
    client.on_message = _on_message

    return client


def iniciar_listener_en_hilo(on_data_change=None):
    client = crear_cliente_mqtt(on_data_change)
    client.connect(config.MQTT_HOST, config.MQTT_PORT, keepalive=60)
    client.loop_start()
    return client