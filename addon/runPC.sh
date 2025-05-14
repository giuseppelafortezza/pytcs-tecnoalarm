#!/bin/sh

export MQTT_HOST="192.168.1.123"
export MQTT_PORT="1883"
export MQTT_USERNAME="mqtt-user"
export MQTT_PASSWORD="Cla11Giu07"
export TCS_USERNAME="giuseppe.lafortezza@gmail.com"
export TCS_PASSWORD="Plutonio_77"
export TCS_SERIAL="003231673"
export TCS_CAN_ARM="false"
export TCS_CAN_DISARM="true"

python3 -tt ./mainSingle.py
