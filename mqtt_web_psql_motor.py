import paho.mqtt.client as mqtt
import psycopg2
from psycopg2 import pool
import json
import time

MQTT_BROKER = "localhost"
MQTT_PORT = 1884
MQTT_SENSOR_TOPIC = "esp32/pub"
MQTT_ACK_TOPIC_FROM_ESP32 = "esp32/motor/ack"

PG_HOST = "10.144.0.142"
PG_PORT = 5432
PG_DATABASE = "mydb"
PG_USER = "agri"
PG_PASSWORD = "1234"


while True:
    try:
        pg_pool = psycopg2.pool.SimpleConnectionPool(
            1, 5,  # min 1, max 5 connections
            host=PG_HOST,
            port=PG_PORT,
            dbname=PG_DATABASE,
            user=PG_USER,
            password=PG_PASSWORD
        )
        break
    except Exception as e:
        print(f"Failed to create connection pool: {e}")
        time.sleep(5)

def init_db():
    conn = pg_pool.getconn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS sensor_data (
                    id SERIAL PRIMARY KEY,
                    temperature REAL,
                    humidity REAL,
                    s_moist REAL,
                    s_temp REAL,
                    s_ph REAL,
                    esp_id TEXT,
                    received_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
                CREATE TABLE IF NOT EXISTS motor_data (
                    id SERIAL PRIMARY KEY,
                    motor_status BOOLEAN DEFAULT FALSE,
                    esp_id TEXT,
                    motor_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """)
            conn.commit()
    except Exception as e:
        print(f"Error initializing DB: {e}")
    finally:
        pg_pool.putconn(conn)

init_db()

def execute_query(query, params=None):
    conn = None
    try:
        conn = pg_pool.getconn()
        with conn.cursor() as cur:
            cur.execute(query, params)
            conn.commit()
    except Exception as e:
        print(f"Database query error: {e}")
    finally:
        if conn:
            pg_pool.putconn(conn)

def on_connect(client, userdata, flags, rc, properties=None):
    if rc == 0:
        client.subscribe(MQTT_SENSOR_TOPIC)
        client.subscribe(MQTT_ACK_TOPIC_FROM_ESP32)
    else:
        print(f"Failed to connect to MQTT broker. Return code {rc}")

def on_message(client, userdata, msg):
    topic = msg.topic
    try:
        payload = msg.payload.decode()
        data = json.loads(payload)
        # print(f"Received on topic [{topic}]: {payload}")

        # Sensor data topic
        if topic == MQTT_SENSOR_TOPIC:
            ESP_ID = data.get("espClientID")
            temperature = data.get("tempVal")
            humidity = data.get("humVal")
            s_moisture = data.get("sHumVal")
            s_temperature = data.get("sTempVal")
            s_ph_val = data.get("sPhVal")

            if temperature is not None and humidity is not None:
                execute_query("""
                    INSERT INTO sensor_data (temperature, humidity, s_moist, s_temp, s_ph, esp_id)
                    VALUES (%s, %s, %s, %s, %s, %s)
                """, (temperature, humidity, s_moisture, s_temperature, s_ph_val, ESP_ID))
                # print(f"Sensor data: {temperature}, {humidity}, {s_moisture}, {s_temperature}, {s_ph_val}, {ESP_ID}")
            else:
                print("Missing temperature or humidity in payload.")

        # Motor ack topic
        elif topic == MQTT_ACK_TOPIC_FROM_ESP32:
            esp_id = data.get("espClientID")
            status_str = data.get("status")

            if status_str in ("True", "False"):
                motor_status = status_str == "True"
                execute_query("""
                    INSERT INTO motor_data (motor_status, esp_id)
                    VALUES (%s, %s)
                """, (motor_status, esp_id))
                # print(f"Motor ack: {esp_id} -> {motor_status}")
            else:
                print(f"Invalid 'status' value: {status_str}")

    except Exception as e:
        print(f"Error handling message on topic [{topic}]: {e}")


client = mqtt.Client(protocol=mqtt.MQTTv5)
client.on_connect = on_connect
client.on_message = on_message

try:
    client.connect(MQTT_BROKER, MQTT_PORT, 60)
    print("Listening for MQTT messages...")
    client.loop_forever()
except Exception as e:
    print(f"MQTT connection error: {e}")
