import paho.mqtt.client as mqtt
import psycopg2
import json
import time

# MQTT Broker Configuration
MQTT_BROKER = "localhost"
MQTT_PORT = 1884

# MQTT Topics
MQTT_SENSOR_TOPIC = "esp32/pub"
MQTT_ACK_TOPIC_FROM_ESP32 = "esp32/motor/ack"

# PostgreSQL Configuration
PG_HOST = "10.144.0.142"
PG_PORT = 5432
PG_DATABASE = "mydb"
PG_USER = "agri"
PG_PASSWORD = "1234"

# Establish PostgreSQL connection with retry logic
def connect_to_db():
    while True:
        try:
            conn = psycopg2.connect(
                host=PG_HOST,
                port=PG_PORT,
                dbname=PG_DATABASE,
                user=PG_USER,
                password=PG_PASSWORD
            )
            conn.autocommit = True
            cur = conn.cursor()
            cur.execute("""
                CREATE TABLE IF NOT EXISTS sensor_data (
                    id SERIAL PRIMARY KEY,
                    temperature REAL,
                    humidity REAL,
                    s_moist REAL,
                    s_temp REAL,
                    s_ph REAL,
                    received_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
                CREATE TABLE IF NOT EXISTS motor_data (
                    id SERIAL PRIMARY KEY,
                    motor_status BOOLEAN DEFAULT FALSE,
                    motor_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """)
            print("Connected to PostgreSQL and ensured tables exist.")
            return conn, cur
        except Exception as e:
            print(f"Database connection failed: {e}")
            print("Retrying in 5 seconds...")
            time.sleep(5)

conn, cur = connect_to_db()

# MQTT Callbacks
def on_connect(client, userdata, flags, rc, properties=None):
    if rc == 0:
        print("Connected to MQTT broker")
        # Subscribe to relevant topics
        client.subscribe(MQTT_SENSOR_TOPIC)
        client.subscribe(MQTT_ACK_TOPIC_FROM_ESP32)
    else:
        print(f"Failed to connect to MQTT broker, return code {rc}")

def on_message(client, userdata, msg):
    topic = msg.topic
    try:
        payload = msg.payload.decode()
        # print(f"Received on topic [{topic}]: {payload}")
        data = json.loads(payload)

        # Sensor data handler
        if topic == MQTT_SENSOR_TOPIC:
            ESP_ID = data.get("espClientID")
            temperature = data.get("tempVal")
            humidity = data.get("humVal")
            s_moisture = data.get("sHumVal")
            s_temperature = data.get("sTempVal")
            s_ph_val = data.get("sPhVal")

            if temperature is not None and humidity is not None:
                cur.execute(
                    "INSERT INTO sensor_data (temperature, humidity, s_moist, s_temp, s_ph, esp_id) VALUES (%s, %s, %s, %s, %s, %s)",
                    (temperature, humidity, s_moisture, s_temperature, s_ph_val, ESP_ID)
                )
                print(temperature, humidity, s_moisture, s_temperature, s_ph_val, ESP_ID)
            else:
                print("Missing temperature or humidity in payload.")

        # Handle acknowledgment from ESP32 → store in motor_data table
        elif topic == MQTT_ACK_TOPIC_FROM_ESP32:
            # print("Forwarding ACK to PostgreSQL motor_data table...")
            esp_id = data.get("espClientID")
            status_str = data.get("status")  # Will be "True" or "False" (as string)
            if status_str in ("True", "False"):
                motor_status = status_str == "True"  # Convert to actual bool
                cur.execute(
                    "INSERT INTO motor_data (motor_status, esp_id) VALUES (%s, %s)",
                    (motor_status, esp_id)
                )

            else:
                print(f"Invalid 'status' value in payload: {status_str}")

    except Exception as e:
        print(f"Error handling message on topic [{topic}]: {e}")

# Initialize MQTT client
client = mqtt.Client(protocol=mqtt.MQTTv5)
client.on_connect = on_connect
client.on_message = on_message

try:
    client.connect(MQTT_BROKER, MQTT_PORT, 60)
    client.loop_forever()
except Exception as e:
    print(f"Error connecting to MQTT Broker: {e}")
    if conn:
        conn.close()
