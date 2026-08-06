import mysql.connector
from contextlib import contextmanager
from datetime import datetime

from config import DB_HOST, DB_USER, DB_PASSWORD, DB_NAME


@contextmanager
def get_conn():
    conn = mysql.connector.connect(
        host=DB_HOST,
        user=DB_USER,
        password=DB_PASSWORD,
        database=DB_NAME
    )
    try:
        yield conn
        conn.commit()
    finally:
        conn.close()

'''
def init_db():
    with get_conn() as conn:
        cursor = conn.cursor()

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS sensor_readings (
                reading_id INT AUTO_INCREMENT PRIMARY KEY,
                timestamp DATETIME NOT NULL,      -- viene del ESP32 (NTP)
                hour VARCHAR(20),                 -- formato "06:00 AM" para mostrar directo
                temperature FLOAT,
                hum_ambient FLOAT,
                soil_moisture INT,
                water_level VARCHAR(10),          -- "Full" / "Empty"
                pump_status VARCHAR(10),          -- "ON" / "OFF" (snapshot al momento de la lectura)
                received_at DATETIME NOT NULL      -- cuando lo recibio el server
            )
        """)

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS irrigation_log (
                event_id INT AUTO_INCREMENT PRIMARY KEY,
                start_time DATETIME,
                end_time DATETIME,
                duration FLOAT,                    -- minutos
                created_at DATETIME NOT NULL
            )
        """)

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS system_status (
                id INT PRIMARY KEY CHECK (id = 1),  -- una sola fila, siempre se actualiza
                esp32 VARCHAR(20),
                mqtt VARCHAR(20),
                pump_available VARCHAR(10),
                sensors_ok VARCHAR(10),
                pump_status VARCHAR(10),
                updated_at DATETIME
            )
        """)

        cursor.close()
'''

def _now():
    return datetime.now().isoformat(timespec="seconds")

def insert_sensor_reading(data: dict):
    with get_conn() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO sensor_readings
                (timestamp, hour, temperature, hum_ambient, soil_moisture, water_level, pump_status, received_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            data.get("timestamp"),
            data.get("hour"),
            data.get("temp"),
            data.get("hum_ambient"),
            data.get("soil_moisture"),
            data.get("water_deposit"),   
            data.get("pump_status"),
            _now(),
        ))
        cursor.close()


def upsert_system_status(data: dict):
    with get_conn() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO system_status (id, esp32, mqtt, pump_available, sensors_ok, pump_status, updated_at)
            VALUES (1, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                esp32=VALUES(esp32),
                mqtt=VALUES(mqtt),
                pump_available=VALUES(pump_available),
                sensors_ok=VALUES(sensors_ok),
                pump_status=VALUES(pump_status),
                updated_at=VALUES(updated_at)
        """, (
            data.get("esp32"),
            data.get("mqtt"),
            data.get("bomba_disponible"),
            data.get("sensores_ok"),
            data.get("bomba_estado"),
            _now(),
        ))
        cursor.close()


def set_esp32_status(valor: str):
    """Usado por el mensaje de Last Will (riego/estado/esp32 -> 'online'/'offline')."""
    with get_conn() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO system_status (id, esp32, updated_at)
            VALUES (1, %s, %s)
            ON DUPLICATE KEY UPDATE
                esp32=VALUES(esp32),
                updated_at=VALUES(updated_at)
        """, (valor, _now()))
        cursor.close()


def insert_irrigation_log(data: dict):
    with get_conn() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO irrigation_log (start_time, end_time, duration, created_at)
            VALUES (%s, %s, %s, %s)
        """, (
            data.get("started"),
            data.get("ended"),
            data.get("duracion_min"),
            _now(),
        ))
        cursor.close()

def get_last_reading():
    with get_conn() as conn:
        cursor = conn.cursor(dictionary=True) 
        cursor.execute("""
            SELECT * FROM sensor_readings ORDER BY reading_id DESC LIMIT 1
        """)
        row = cursor.fetchone()
        cursor.close()
        return row 


def get_system_status():
    with get_conn() as conn:
        cursor = conn.cursor(dictionary=True)
        cursor.execute("SELECT * FROM system_status WHERE id = 1")
        row = cursor.fetchone()
        cursor.close()
        return row


def get_last_irrigation():
    with get_conn() as conn:
        cursor = conn.cursor(dictionary=True)
        cursor.execute("""
            SELECT * FROM irrigation_log ORDER BY event_id DESC LIMIT 1
        """)
        row = cursor.fetchone()
        cursor.close()
        return row


def get_soil_moisture_per_hour(horas=12):
    with get_conn() as conn:
        cursor = conn.cursor(dictionary=True)
        cursor.execute("""
            SELECT
                HOUR(timestamp) AS hora_num,         
                AVG(soil_moisture) AS promedio
            FROM sensor_readings
            WHERE timestamp >= NOW() - INTERVAL %s HOUR
              AND soil_moisture IS NOT NULL
            GROUP BY hora_num
            ORDER BY hora_num ASC
        """, (horas,))
        rows = cursor.fetchall()
        cursor.close()
        return [{"hora": int(r["hora_num"]), "soil_moisture": round(r["promedio"], 1)} for r in rows]


def get_history(limite=20):
    with get_conn() as conn:
        cursor = conn.cursor(dictionary=True)
        cursor.execute("""
            SELECT
                DATE_FORMAT(timestamp, '%%Y-%%m-%%d %%H:00') AS bucket,
                hour,
                soil_moisture,
                temperature,
                pump_status,
                timestamp
            FROM sensor_readings sr
            WHERE sr.reading_id IN (
                SELECT MAX(reading_id) FROM sensor_readings GROUP BY DATE_FORMAT(timestamp, '%%Y-%%m-%%d %%H')
            )
            ORDER BY timestamp DESC
            LIMIT %s
        """, (limite,))
        rows = cursor.fetchall()

        historial = []
        for r in rows:
            sub_cursor = conn.cursor(dictionary=True)
            sub_cursor.execute("""
                SELECT COALESCE(SUM(duration), 0) AS total
                FROM irrigation_log
                WHERE DATE_FORMAT(creado_en, '%%Y-%%m-%%d %%H:00') = %s
            """, (r["bucket"],))
            watered_for = sub_cursor.fetchone()["total"]
            sub_cursor.close()

            historial.append({
                "time": r["hora"] or "N/A",
                "soil_moisture": r["soil_moisture"],
                "temperature": r["temperature"],
                "watered_for_min": round(watered_for, 1),
                "system": r["pump_status"] or "OFF",
            })

        cursor.close()
        return historial