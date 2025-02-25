# Gateway Device Manager

This repository is part of the [LoRaBridge](https://github.com/lorabridge2/lorabridge) project.

The Device Manager is a self-provided Python3 application that translates flows created in the ui to lorabridge automation commands. These commands are then sent to the bridge and rebuilt into a nodered flow.

## Environment Variables

- `DEV_MQTT_HOST`: IP or hostname of MQTT host
- `DEV_MQTT_PORT`: Port used by MQTT
- `DEV_MQTT_USERNAME`: MQTT username if used (can be a file as well)
- `DEV_MQTT_PASSWORD`: MQTT password if used (can be a file as well)
- `DEV_REDIS_HOST`: IP or hostname of Redis host
- `DEV_REDIS_PORT`: Port used by Redis
- `DEV_REDIS_DB`: Number of the database used inside Redis
- `DEV_EUI`: Device eui of bridge

## License

All the LoRaBridge software components and the documentation are licensed under GNU General Public License 3.0.

## Acknowledgements

The financial support from Internetstiftung/Netidee is gratefully acknowledged. The mission of Netidee is to support development of open-source tools for more accessible and versatile use of the Internet in Austria.
