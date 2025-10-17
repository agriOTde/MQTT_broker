# MQTT_broker
Python scripts for MQTT broker hosted on RPi

## Scripts

* MQTT <--> ESP32
* Web app <--> MQTT <--> ESP32

## Dummy Commands for Testing
mosquitto_sub -h 1127.0.0.1 -p 1884 -t "esp32/motor/ack"
mosquitto_sub -h 127.0.0.1 -p 1884 -t "esp32/pub"

mosquitto_pub -h 127.0.0.1 -p 1884 -t "esp32/motor/schedule" -m '{"Duration": 20, "TimePeriod": 12}'

mosquitto_pub -h 127.0.0.1 -p 1884 -t "esp32/motor/command" -m '{"cmd": 1}'
mosquitto_pub -h 127.0.0.1 -p 1884 -t "esp32/ota/command" -m '{"cmd": 1}'



