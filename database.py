import mysql.connector
from contextlib import contextmanager
from datetime import datetime, timedelta
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
                duration FLOAT,                     -- minutos, real (reportado por el ESP32)
                planned_duration_min FLOAT,          -- minutos, lo que el perfil pedia (NULL si fue manual)
                source VARCHAR(20) DEFAULT 'auto',   -- 'auto' / 'manual' / 'safety_timeout' / etc
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
    # espacio en vez de "T" -- formato que MySQL/MariaDB si acepta
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _to_mysql_dt(ts_str):
    """El ESP32 manda timestamps en ISO con 'T' (ej. 2026-08-13T18:26:09).
    MySQL/MariaDB espera espacio en vez de 'T'. Sin esto, la fila se
    guarda con fecha 0000-00-00 sin dar error."""
    if not ts_str or ts_str == "N/A":
        return None
    return ts_str.replace("T", " ", 1)


def insert_sensor_reading(data: dict):
    print("my timestamp: " + str(data.get("timestamp")))
    with get_conn() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO sensor_readings
                (timestamp, hour, temperature, hum_ambient, soil_moisture, water_level, pump_status, received_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            _to_mysql_dt(data.get("timestamp")),
            data.get("hour"),
            data.get("temp"),
            data.get("hum_ambient"),
            data.get("soil_moisture"),
            data.get("water_level"),
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
            data.get("pump_status"),
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


# create datetime for logs
def _parse_time_to_datetime(time_str, reference_date=None):

    if not time_str or time_str == "N/A":
        return None

    reference_date = reference_date or datetime.now().date()
    try:
        parsed_time = datetime.strptime(time_str, "%I:%M %p").time()
        return datetime.combine(reference_date, parsed_time)
    except ValueError:
        return None


def insert_irrigation_log(data: dict):
    with get_conn() as conn:
        cursor = conn.cursor()

        start_dt = _parse_time_to_datetime(data.get("started"))
        end_dt = _parse_time_to_datetime(data.get("ended"))

        if start_dt and end_dt and end_dt < start_dt:
            end_dt += timedelta(days=1)

        # El ESP32 manda "source" ('auto' / 'manual' / 'duracion_programada_completa'
        # / 'safety_timeout') y, cuando aplica, "duracion_programada_min" con lo
        # que el perfil pedia -- asi queda visible lo planeado vs lo real.
        source = data.get("source") or "auto"
        planned = data.get("duracion_programada_min")
        if planned in (None, 0, 0.0):
            planned = None

        cursor.execute("""
            INSERT INTO irrigation_log (start_time, end_time, duration, planned_duration_min, source, created_at)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (
            start_dt,
            end_dt,
            data.get("duracion_min"),
            planned,
            source,
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

def get_history(hours=12):
    now = datetime.now()
    buckets = []

    for i in range(hours):
        bucket_dt = (
            now - timedelta(hours=i)
        ).replace(
            minute=0,
            second=0,
            microsecond=0
        )

        buckets.append(bucket_dt)

    oldest = buckets[-1]

    with get_conn() as conn:
        cursor = conn.cursor(dictionary=True)

        cursor.execute("""
            SELECT
                DATE_FORMAT(timestamp, '%Y-%m-%d %H:00') AS bucket,
                AVG(soil_moisture) AS avg_soil_moisture,
                AVG(temperature) AS avg_temperature,
                COUNT(*) AS reading_count
            FROM sensor_readings
            WHERE timestamp >= %s
            GROUP BY bucket
            ORDER BY bucket ASC
        """, (oldest,))

        readings = cursor.fetchall()

        cursor.execute("""
            SELECT
                start_time,
                end_time,
                duration,
                planned_duration_min,
                source
            FROM irrigation_log
            WHERE start_time >= %s
            ORDER BY start_time ASC
        """, (oldest,))

        events = cursor.fetchall()

        cursor.close()

    readings_by_bucket = {
        r["bucket"]: r
        for r in readings
    }

    events_by_bucket = {}

    for e in events:
        if not e["start_time"]:
            continue

        key = e["start_time"].strftime("%Y-%m-%d %H:00")

        events_by_bucket.setdefault(key, []).append(e)

    history = []

    for bucket_dt in buckets:

        bucket_key = bucket_dt.strftime("%Y-%m-%d %H:00")

        reading = readings_by_bucket.get(bucket_key)
        hour_events = events_by_bucket.get(bucket_key, [])

        start_label = bucket_dt.strftime("%I %p").lstrip("0")

        end_dt = bucket_dt + timedelta(hours=1)
        end_label = end_dt.strftime("%I %p").lstrip("0")

        if bucket_dt == buckets[0]:
            time_label = f"{start_label} – Now"
        else:
            time_label = f"{start_label} – {end_label}"

        watered_total = round(
            sum(float(e["duration"] or 0) for e in hour_events),
            1
        )

        if watered_total > 0:
            system_state = "WATERING"

        elif reading:
            system_state = "ONLINE"

        else:
            system_state = "OFFLINE"

        if reading:

            soil_moisture = (
                round(float(reading["avg_soil_moisture"]))
                if reading["avg_soil_moisture"] is not None
                else None
            )

            temperature = (
                round(float(reading["avg_temperature"]))
                if reading["avg_temperature"] is not None
                else None
            )

        else:
            soil_moisture = None
            temperature = None

        event_list = [
            {
                "start": (
                    e["start_time"].strftime("%I:%M %p")
                    if e["start_time"]
                    else None
                ),

                "end": (
                    e["end_time"].strftime("%I:%M %p")
                    if e["end_time"]
                    else None
                ),

                "duration": e["duration"],

                "planned_duration": e["planned_duration_min"],

                "matches_profile": (
                    e["planned_duration_min"] is not None
                    and e["duration"] is not None
                    and abs(
                        float(e["duration"])
                        - float(e["planned_duration_min"])
                    ) <= 0.5
                ),

                "source": e["source"] or "auto",
            }

            for e in hour_events
        ]

        history.append({
            "time": time_label,

            "soil_moisture": soil_moisture,

            "temperature": temperature,

            "watered_for_min": (
                watered_total
                if watered_total > 0
                else None
            ),

            "events": event_list,

            "system": system_state,
        })

    return history

def get_plant_profile():
    with get_conn() as conn:
        cursor = conn.cursor(dictionary=True)
        cursor.execute("SELECT * FROM plant_profile WHERE id = 1")
        row = cursor.fetchone()
        cursor.close()
        return row


def create_plant_profile(data: dict):
    with get_conn() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO plant_profile
                (id, size, name, moisture_threshold, duration_min, water_usage, min_interval_hours, updated_at)
            VALUES (1, %s, %s, %s, %s, %s, %s, %s)
        """, (
            data["size"], data["name"], data["moisture_threshold"],
            data["duration_min"], data.get("water_usage"),
            data.get("min_interval_hours", 2), datetime.now()
        ))
        cursor.close()


def get_last_watering_ts():
    """Epoch timestamp (float) del último riego iniciado, o None si no hay registro."""
    with get_conn() as conn:
        cursor = conn.cursor(dictionary=True)
        cursor.execute("""
            SELECT start_time FROM irrigation_log
            WHERE start_time IS NOT NULL
            ORDER BY event_id DESC LIMIT 1
        """)
        row = cursor.fetchone()
        cursor.close()
        return row["start_time"].timestamp() if row and row["start_time"] else None