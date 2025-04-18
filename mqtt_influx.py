import paho.mqtt.client as mqtt
import requests
import json

# MQTT Broker Configuration
MQTT_BROKER = "localhost"  # Change to your broker's IP
MQTT_PORT = 1883  # Default non-TLS MQTT port
MQTT_TOPIC = "esp32/pub"

# InfluxDB Configuration
INFLUXDB_URL = "https://dz7l63mf8k-uit6ldnaw72vug.timestream-influxdb.eu-north-1.on.aws:8086/api/v2/write"
ORG = "agriNewDB"
BUCKET = "testAgri"
TOKEN = "cDYSvHUSOZRxIqI_5SQQFUTanoS0MKuBzU5HWHCn5qVfYO31UOy0twu54kc-4KrcF4OdzWxPkPHpxjNbthBELQ=="

# Define headers for InfluxDB request
HEADERS = {
    "Authorization": f"Token {TOKEN}",
    "Content-Type": "application/x-www-form-urlencoded; charset=utf-8",
    "Accept": "application/json"
}

# Callback for MQTT connection
def on_connect(client, userdata, flags, rc, properties=None):
    if rc == 0:
        print("Connected to MQTT broker")
        client.subscribe(MQTT_TOPIC)
    else:
        print(f"Failed to connect, return code {rc}")

# Callback for received MQTT messages
def on_message(client, userdata, msg):
    try:
        payload = msg.payload.decode()
        print(f"Received message: {payload}")

        # Assuming payload is a JSON format, e.g., {"temperature": 22.5, "humidity": 60}
        data = json.loads(payload)  # Safely parse the JSON payload
        temperature = data.get("Temp")
        humidity = data.get("moistureVal")

        # Prepare data for InfluxDB line protocol
        influx_data = f"device_data,device=esp32 temperature={temperature},humidity={humidity}"

        # Send data to InfluxDB via HTTP POST request
        response = requests.post(
            f"{INFLUXDB_URL}?org={ORG}&bucket={BUCKET}&precision=s",
            headers=HEADERS,
            data=influx_data
        )

        if response.status_code == 204:
            print("Data successfully written to InfluxDB")
        else:
            print(f"Failed to write data: {response.text}")

    except Exception as e:
        print(f"Error processing message: {e}")

# Initialize MQTT client
client = mqtt.Client(protocol=mqtt.MQTTv5)  # Set protocol to MQTTv5 for the latest API
client.on_connect = on_connect
client.on_message = on_message

# Connect to MQTT Broker
client.connect(MQTT_BROKER, MQTT_PORT, 60)

# Loop to listen for messages
client.loop_forever()
