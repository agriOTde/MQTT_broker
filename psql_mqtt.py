import paho.mqtt.client as mqtt
import psycopg2
import json
import time

# MQTT Broker Configuration
MQTT_BROKER = "localhost"  # Or the actual IP if needed
MQTT_PORT = 1883
MQTT_TOPIC = "esp32/pub"

# PostgreSQL Configuration (Linux PC IP)
PG_HOST = "192.168.1.105"  # <-- Replace with Linux PC IP address
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
            # Create table if it doesn't exist
            cur.execute("""
                CREATE TABLE IF NOT EXISTS sensor_data (
                    id SERIAL PRIMARY KEY,
                    temperature REAL,
                    humidity REAL,
                    received_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            print("Connected to PostgreSQL and ensured table exists.")
            return conn, cur
        except Exception as e:
            print(f"Database connection failed: {e}")
            print("Retrying in 5 seconds...")
            time.sleep(5)

# Initialize database connection
conn, cur = connect_to_db()

# Callback for MQTT connection
def on_connect(client, userdata, flags, rc, properties=None):
    if rc == 0:
        print("Connected to MQTT broker")
        client.subscribe(MQTT_TOPIC)
    else:
        print(f"Failed to connect to MQTT broker, return code {rc}")

# Callback for MQTT messages
def on_message(client, userdata, msg):
    try:
        payload = msg.payload.decode()
        print(f"Received message: {payload}")
        data = json.loads(payload)

        temperature = data.get("tempVal")
        humidity = data.get("humVal")
        s_moisture = data.get("sHumVal")
        s_temperature = data.get("sTempVal")
        s_ph_val = data.get("sPhVal")

        if temperature is not None and humidity is not None:
            cur.execute(
                "INSERT INTO sensor_data (temperature, humidity, s_moist, s_temp, s_ph) VALUES (%s, %s, %s, %s, %s)",
                (temperature, humidity, s_moisture, s_temperature, s_ph_val)
            )
            print("Data inserted into PostgreSQL.")
        else:
            print("Missing temperature or humidity in payload.")

    except Exception as e:
        print(f"Error handling message: {e}")

# Initialize MQTT client
client = mqtt.Client(protocol=mqtt.MQTTv5)
client.on_connect = on_connect
client.on_message = on_message

# Connect and loop
try:
    client.connect(MQTT_BROKER, MQTT_PORT, 60)
    client.loop_forever()
except Exception as e:
    print(f"Error connecting to MQTT Broker: {e}")
    if conn:
        conn.close()  # Close the database connection if MQTT connection fails
