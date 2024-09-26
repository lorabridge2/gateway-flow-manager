#!/usr/bin/env python3
# -*- coding=utf-8 -*-

import struct
import paho.mqtt.client as mqtt
import json
import os
import sys
import redis
import logging
import device_classes
import paho.mqtt.publish as publish


def get_fileenv(var: str):
    """Tries to read the provided env var name + _FILE first and read the file at the path of env var value.
    If that fails, it looks at /run/secrets/<env var>, otherwise uses the env var itself.
    Args:
        var (str): Name of the provided environment variable.

    Returns:
        Content of the environment variable file if exists, or the value of the environment variable.
        None if the environment variable does not exist.
    """
    if path := os.environ.get(var + "_FILE"):
        with open(path) as file:
            return file.read().strip()
    else:
        try:
            with open(os.path.join("run", "secrets", var.lower())) as file:
                return file.read().strip()
        except IOError:
            # mongo username needs to be string and not empty (fix for sphinx)
            if "sphinx" in sys.modules:
                return os.environ.get(var, "fail")
            else:
                return os.environ.get(var)


MQTT_HOST = os.environ.get("DEV_MQTT_HOST", "127.0.0.1")
MQTT_PORT = int(os.environ.get("DEV_MQTT_PORT", 1883))
MQTT_USERNAME = get_fileenv("DEV_MQTT_USERNAME") or "lorabridge"
MQTT_PASSWORD = get_fileenv("DEV_MQTT_PASSWORD") or "lorabridge"
# DEV_MAN_TOPIC = os.environ.get("DEV_DEV_MAN_TOPIC", "devicemanager")
REDIS_HOST = os.environ.get("DEV_REDIS_HOST", "localhost")
REDIS_PORT = int(os.environ.get("DEV_REDIS_PORT", 6379))
REDIS_DB = int(os.environ.get("DEV_REDIS_DB", 0))
# DISCOVERY_TOPIC = os.environ.get("DEV_DISCOVERY_TOPIC", "lorabridge/discovery")
# STATE_TOPIC = os.environ.get("DEV_STATE_TOPIC", "lorabridge/state")

REDIS_SEPARATOR = ":"
REDIS_PREFIX = "lorabridge:flowman"

NODE_TYPES = {
    "binarydevice": 1,
    "binarysensor": 3,
    "alert": 5,
    "logicand": 6,
    "logicor": 7,
    "timer": 8,
    "hysteresis": 9,
    "countdown": 10,
}


# The callback for when the client receives a CONNACK response from the server.
# def on_connect(client, userdata, flags, rc):
#     logging.info("Connected with result code " + str(rc))

#     # Subscribing in on_connect() means that if we lose the connection and
#     # reconnect then subscriptions will be renewed.
#     client.subscribe(userdata['topic'] + '/#')


# The callback for when a PUBLISH message is received from the server.
# def on_message(client, userdata, msg):
#     rclient = userdata['r_client']
#     data = json.loads(msg.payload)

#     if not rclient.exists(
#         (rkey := REDIS_SEPARATOR.join([REDIS_PREFIX, (ieee := msg.topic.removeprefix(DEV_MAN_TOPIC + "/"))]))):
#         index = rclient.incr(REDIS_SEPARATOR.join([REDIS_PREFIX, "dev_index"]))
#         dev_data = {"ieee": ieee, "id": index, "measurement": json.dumps(list(data.keys()))}
#         rclient.hset(rkey, mapping=dev_data)
#         rclient.sadd(REDIS_SEPARATOR.join([REDIS_PREFIX, "devices"]), rkey)
#         dev_data['measurement'] = json.loads(dev_data['measurement'])
#         dev_data.update({"value": data})
#         client.publish(DISCOVERY_TOPIC, json.dumps(dev_data))
#         client.publish(STATE_TOPIC, json.dumps(dev_data))
#         # discovery
#     else:
#         # state update
#         dev_data = rclient.hgetall(rkey)
#         str_data = json.dumps(list(data.keys()))
#         if dev_data['measurement'] != str_data:
#             dev_data['measurement'] = str_data
#             rclient.hset(rkey, mapping=dev_data)
#         dev_data['measurement'] = json.loads(dev_data['measurement'])
#         dev_data.update({"value": data})
#         if dev_data['measurement'] != str_data:
#             client.publish(DISCOVERY_TOPIC, json.dumps(dev_data))
#         client.publish(STATE_TOPIC, json.dumps(dev_data))

flow_ui_key = REDIS_SEPARATOR.join([REDIS_PREFIX, "flow-ui-key"])
flow_lb_key = REDIS_SEPARATOR.join([REDIS_PREFIX, "flow-lb-key"])


class IDsExhaustedError(RuntimeError):
    pass


class UnknownAttributeError(ValueError):
    pass


class UnknownDeviceError(ValueError):
    pass


def main():
    logging.basicConfig(
        format="%(asctime)s - %(levelname)s - %(message)s", level=logging.INFO
    )
    # client = mqtt.Client()
    # client.on_connect = on_connect
    # client.on_message = on_message

    # client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)
    # client.connect(MQTT_HOST, MQTT_PORT, 60)
    # client.user_data_set({"topic": DEV_MAN_TOPIC})

    # msgs = [
    #     {"topic": "paho/test/topic", "payload": "multiple 1"},
    #     ("paho/test/topic", "multiple 2", 0, False),
    # ]

    r_client = redis.Redis(
        host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB, decode_responses=True
    )
    pubsub = r_client.pubsub(ignore_subscribe_messages=True)
    pubsub.subscribe(
        "__keyspace@0__:" + REDIS_SEPARATOR.join([REDIS_PREFIX, "flow-queue"])
    )

    # res = r_client.llen(REDIS_SEPARATOR.join([REDIS_PREFIX, "flow-queue"]))
    # print(res)

    # TODO grab everything that is already in the flow-queue
    for msg in pubsub.listen():
        if msg["type"] == "message" and msg["data"] == "lpush":
            print(msg)
            flow = r_client.rpop(REDIS_SEPARATOR.join([REDIS_PREFIX, "flow-queue"]))
            # print(flow)
            if flow:
                flow = json.loads(flow)
                flow["nodes"] = json.loads(flow["nodes"])
                flow["edges"] = json.loads(flow["edges"])
                print("b")
                commands = parse_new_flow(flow, r_client)

                # if not flow_exists(flow["id"], r_client):
                #     print("c")
                #     commands = parse_new_flow(flow, r_client)
                # else:
                #     print("d")
                #     commands = diff_flow(flow)

                if commands:
                    msgs = [
                        {
                            "topic": "application/LoRaBridge2_Datapipe/device/2000000000000001/command/down",
                            "payload": x,
                        }
                        for x in commands
                    ]
                    for cmd in commands:
                        temp_hex_string = "".join("{:02x}".format(x) for x in cmd)
                        r_client.lpush("lbcommands", temp_hex_string)
                    # publish.multiple(
                    #     msgs,
                    #     hostname=MQTT_HOST,
                    #     port=MQTT_PORT,
                    #     auth={"username": MQTT_USERNAME, "password": MQTT_PASSWORD},
                    # )
    # client.user_data_set({"r_client": r_client, "topic": DEV_MAN_TOPIC})

    # Blocking call that processes network traffic, dispatches callbacks and
    # handles reconnecting.
    # Other loop*() functions are available that give a threaded interface and a
    # manual interface.
    # client.loop_forever()


def get_node_key(flow_id: str, node_id: str, r_client: redis.Redis) -> int:
    lb_node_dict = REDIS_SEPARATOR.join([REDIS_PREFIX, "flow", flow_id, "lb-nodes"])
    ui_node_dict = REDIS_SEPARATOR.join([REDIS_PREFIX, "flow", flow_id, "ui-nodes"])
    return int(_get_key(node_id, r_client, lb_node_dict, ui_node_dict))


def get_flow_key(id: str, r_client: redis.Redis) -> int:
    return int(_get_key(id, r_client, flow_lb_key, flow_ui_key))


def _get_key(id: str, r_client: redis.Redis, lb_dict: str, ui_dict: str):
    lb_id = r_client.hget(ui_dict, id)
    print(lb_id)
    print("aas")
    while lb_id is None:
        keys = r_client.hkeys(lb_dict)
        if not keys:
            if r_client.hsetnx(lb_dict, 0, id):
                r_client.hsetnx(ui_dict, id, 0)
                lb_id = 0
            else:
                # raise RuntimeError("key was taken in the meanwhile")
                continue
        else:
            keys = [int(x) for x in keys]
            keys.sort()
            # lb_id = None
            for i in range(256):
                if i not in keys:
                    lb_id = i
                    break
            else:
                raise IDsExhaustedError("no free IDs")
            if r_client.hsetnx(lb_dict, lb_id, id):
                r_client.hsetnx(ui_dict, id, lb_id)
            else:
                # raise RuntimeError("key was taken in the meanwhile")
                # id stolen by someone else in the meantime
                lb_id = None
    return lb_id


def del_flow_id(id: str, r_client: redis.Redis):
    lb_node_dict = REDIS_SEPARATOR.join([REDIS_PREFIX, "flow", id, "lb-nodes"])
    ui_node_dict = REDIS_SEPARATOR.join([REDIS_PREFIX, "flow", id, "ui-nodes"])
    lb_id = r_client.hget(flow_ui_key, id)
    if lb_id:
        r_client.hdel(flow_ui_key, [id])
        r_client.hdel(flow_lb_key, [lb_id])
        r_client.delete(ui_node_dict)
        r_client.delete(lb_node_dict)
        raise NotImplementedError("Edge deletion missing")


def del_node_id(id: str, r_client: redis.Redis):
    lb_node_dict = REDIS_SEPARATOR.join([REDIS_PREFIX, "flow", id, "lb-nodes"])
    ui_node_dict = REDIS_SEPARATOR.join([REDIS_PREFIX, "flow", id, "ui-nodes"])
    lb_id = r_client.hget(ui_node_dict, id)
    if lb_id:
        r_client.hdel(ui_node_dict, [id])
        r_client.hdel(lb_node_dict, [lb_id])
        raise NotImplementedError("Edge deletion missing")


def parse_new_flow(flow: any, r_client: redis.Redis):
    # - save flow for diff
    #
    commands = []
    # TODO autoremove flow ONLY for now
    if flow_exists(flow["id"], r_client):
        commands.append([11, get_flow_key(flow["id"], r_client)])
    print("hy")
    flow_id_lb = get_flow_key(flow["id"], r_client)
    print(flow)
    # add flow
    commands.append([9, flow_id_lb])
    for node in flow["nodes"]:
        node_id_lb = get_node_key(flow["id"], node["id"], r_client)
        node_type_lb = NODE_TYPES[node["type"]]
        if node["type"] in ["binarysensor", "binarydevice"]:
            print(node)
            # add device
            # add device, flow id, node id, Binarysensor=3||Numericsensor=4, LB Device number, attribute
            try:
                attr_id = int(
                    device_classes.DEVICE_CLASSES.index(node["data"]["attribute"])
                )
            except ValueError:
                raise UnknownAttributeError("attribute unknown")
            try:
                dev_id = int(node["data"]["device"])
            except ValueError:
                raise UnknownDeviceError("device unknown")
            # TODO change direkt number to name and lookup (also get list of available sensors and devices)
            commands.append([2, flow_id_lb, node_id_lb, node_type_lb, dev_id, attr_id])
        else:
            # add node
            # add node, flow id, node id, node type
            commands.append([1, flow_id_lb, node_id_lb, node_type_lb])
            match node["type"]:
                case "hysteresis":
                    # min value
                    # add param, flow id, node id, param index 0, 4 bytes, float 2, min value as 4 ints representing the bytes
                    commands.append(
                        [
                            3,
                            flow_id_lb,
                            node_id_lb,
                            0,
                            4,
                            2,
                            *list(struct.pack("!f", float(node["data"]["min"]))),
                        ]
                    )
                    # max value
                    # add param, flow id, node id, param index 1, 4 bytes, float 2, max value as 4 ints representing the bytes
                    commands.append(
                        [
                            3,
                            flow_id_lb,
                            node_id_lb,
                            1,
                            4,
                            2,
                            *list(struct.pack("!f", float(node["data"]["max"]))),
                        ]
                    )
                case "countdown":
                    # add param, flow id, node id, param index 0, 4 bytes, integer 1, counter as 4 byte integer value
                    commands.append(
                        [
                            3,
                            flow_id_lb,
                            node_id_lb,
                            0,
                            4,
                            1,
                            *list(struct.pack("!i", int(node["data"]["counter"]))),
                        ]
                    )
                case "timer":
                    start = node["data"]["start"].split(":")
                    stop = node["data"]["stop"].split(":")
                    # hour min
                    # add param, flow id, node id, param index 0, 1 byte, integer 1, hour min as int
                    commands.append(
                        [
                            3,
                            flow_id_lb,
                            node_id_lb,
                            0,
                            1,
                            1,
                            int(start[0]),
                        ]
                    )
                    # hour max
                    # add param, flow id, node id, param index 1, 1 byte, integer 1, hour max as int
                    commands.append(
                        [
                            3,
                            flow_id_lb,
                            node_id_lb,
                            1,
                            1,
                            1,
                            int(stop[0]),
                        ]
                    )
                    # minute min
                    # add param, flow id, node id, param index 2, 1 byte, integer 1, minute min as int
                    commands.append(
                        [
                            3,
                            flow_id_lb,
                            node_id_lb,
                            2,
                            1,
                            1,
                            int(start[1]),
                        ]
                    )
                    # minute max
                    # add param, flow id, node id, param index 3, 1 byte, integer 1, minute max as int
                    commands.append(
                        [
                            3,
                            flow_id_lb,
                            node_id_lb,
                            3,
                            1,
                            1,
                            int(stop[1]),
                        ]
                    )

    for edge in flow["edges"]:
        # connect node, flow id, output node id (source), output id, input node id (target), input id
        commands.append(
            [
                4,
                flow_id_lb,
                get_node_key(flow["id"], edge["source"], r_client),
                0,
                get_node_key(flow["id"], edge["target"], r_client),
                0,
            ]
        )
    # flow complete
    commands.append([10, flow_id_lb])
    # upload flow
    commands.append([12, flow_id_lb])
    # enable flow
    commands.append([6, flow_id_lb])
    from pprint import pprint

    pprint(commands)
    return commands


def diff_flow(flow: any):
    # - new nodes
    # - deleted nodes
    # - new edges
    # - deleted edges
    # - changed params
    pass


def flow_exists(id: str, r_client: redis.Redis) -> bool:
    return r_client.hexists(flow_ui_key, id)


def tranform_commands(compressed_commands: list):
    res = []
    for cmd in compressed_commands:
        temp_hex_string = "".join("{:02x}".format(x) for x in cmd)
        res.append(temp_hex_string)
        # print(temp_hex_string)
        # redis_client.lpush("lbcommands", temp_hex_string)
    return temp_hex_string


if __name__ == "__main__":
    main()
