#!/usr/bin/env python3
# -*- coding=utf-8 -*-

import base64
import binascii
import hashlib
import json
import logging
import os
import struct
import sys
from enum import IntEnum
from pprint import pprint

import device_classes
import paho.mqtt.publish as publish
import redis


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
COMMANDS_PREFIX = "commands"
LAST_COMMANDS_PREFIX = "last_commands"

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


class action_bytes(IntEnum):
    REMOVE_NODE = 0
    ADD_NODE = 1
    ADD_DEVICE = 2
    PARAMETER_UPDATE = 3
    CONNECT_NODE = 4
    DISCONNECT_NODE = 5
    ENABLE_FLOW = 6
    DISABLE_FLOW = 7
    TIME_SYNC_RESPONSE = 8
    ADD_FLOW = 9
    FLOW_COMPLETE = 10
    REMOVE_FLOW = 11
    UPLOAD_FLOW = 12


flow_ui_key = REDIS_SEPARATOR.join([REDIS_PREFIX, "flow-ui-key"])
flow_lb_key = REDIS_SEPARATOR.join([REDIS_PREFIX, "flow-lb-key"])


class IDsExhaustedError(RuntimeError):
    pass


class UnknownAttributeError(ValueError):
    pass


class UnknownDeviceError(ValueError):
    pass


def main():
    logging.basicConfig(format="%(asctime)s - %(levelname)s - %(message)s", level=logging.INFO)

    r_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB, decode_responses=True)

    while item := r_client.rpop(REDIS_SEPARATOR.join([REDIS_PREFIX, "flow-queue"])):
        process_flow(item, r_client)

    pubsub = r_client.pubsub(ignore_subscribe_messages=True)
    pubsub.subscribe(
        "__keyspace@0__:" + REDIS_SEPARATOR.join([REDIS_PREFIX, "flow-queue"]),
        "__keyspace@0__:" + REDIS_SEPARATOR.join([REDIS_PREFIX, "hash-check"]),
    )

    for msg in pubsub.listen():
        if msg["type"] == "message" and msg["data"] == "lpush":
            print(msg)
            if msg["channel"] == "__keyspace@0__:" + REDIS_SEPARATOR.join(
                [REDIS_PREFIX, "flow-queue"]
            ):
                flow = r_client.rpop(REDIS_SEPARATOR.join([REDIS_PREFIX, "flow-queue"]))
                # print(flow)
                if flow:
                    process_flow(flow, r_client)
            elif msg["channel"] == "__keyspace@0__:" + REDIS_SEPARATOR.join(
                [REDIS_PREFIX, "hash-check"]
            ):
                check = r_client.rpop(REDIS_SEPARATOR.join([REDIS_PREFIX, "hash-check"]))
                commands = check_hash(check, r_client)
                send_commands(flow["id"], commands, r_client, history=False)


def check_hash(check: str, r_client: redis.Redis) -> list:
    print(check)
    check = json.loads(check)
    print(check)
    ui_key = lookup_ui_key(check["id"], r_client)
    hash_commands = []
    if tmp := r_client.get(REDIS_SEPARATOR.join([REDIS_PREFIX, COMMANDS_PREFIX, ui_key])):
        hash_commands = json.loads(tmp)

    print(hash_commands)
    commands = []
    hash = bytes(bytearray.fromhex(hashlib.sha1(repr(hash_commands).encode()).hexdigest()[-16:]))
    print(hash)
    if binascii.unhexlify(check["hash"]) == hash:
        print("correct")
        if flow := get_flow(ui_key, r_client):
            commands.extend(upload_flow(flow, r_client))
            commands.extend(enable_flow(flow, r_client))
        return commands
    else:
        print("incorrect")
        last_commands = json.loads(
            r_client.get(REDIS_SEPARATOR.join([REDIS_PREFIX, LAST_COMMANDS_PREFIX, flow["id"]]))
        )
        return last_commands


def send_commands(id, commands, r_client, history=True):
    msgs = [
        {
            "topic": "application/c42cfa44-9586-4266-834b-bd412c33c488/device/2000000000000001/command/down",
            # "topic": "eu868/gateway/aa555a0000000101/command/down",
            "payload": json.dumps(
                {
                    "confirmed": True,
                    "fPort": 10,
                    "devEui": "2000000000000001",
                    "data": base64.b64encode(bytes(cmd)).decode(),
                }
            ),
        }
        for cmd in commands
    ]

    # save last commands for hash retry
    r_client.set(
        REDIS_SEPARATOR.join([REDIS_PREFIX, LAST_COMMANDS_PREFIX, id]),
        json.dumps(commands),
    )

    if history:
        prev_cmds = []
        if cmds := r_client.get(REDIS_SEPARATOR.join([REDIS_PREFIX, COMMANDS_PREFIX, id])):
            prev_cmds = json.loads(cmds)

        prev_cmds.extend(commands)
        r_client.set(
            REDIS_SEPARATOR.join([REDIS_PREFIX, COMMANDS_PREFIX, id]), json.dumps(prev_cmds)
        )

    pprint(commands)
    for msg in msgs:
        publish.single(
            msg["topic"],
            msg["payload"],
            hostname=MQTT_HOST,
            port=MQTT_PORT,
            auth={"username": MQTT_USERNAME, "password": MQTT_PASSWORD},
        )


def process_flow(task, r_client):
    task = json.loads(task)
    flow = task["flow"]
    if not all([x in flow for x in ["nodes", "edges"]]):
        return
    flow["nodes"] = json.loads(flow["nodes"])
    flow["edges"] = json.loads(flow["edges"])
    print("b")
    commands = []
    match task["task"]:
        case "deploy":
            print("deploy")
            # commands = parse_new_flow(flow, r_client)
            if not flow_exists(flow["id"], r_client):
                print("add new")
                commands = parse_new_flow(flow, r_client)
            else:
                print("diff")
                commands = diff_flow(flow, r_client)
        case "enable":
            print("enable")
            commands = enable_flow(flow, r_client)
        case "disable":
            print("disable")
            commands = disable_flow(flow, r_client)
        case "delete":
            print("delete")
            commands = del_flow(flow, r_client)
        case _:
            print("unknown task command")

    if commands:
        send_commands(flow["id"], commands, r_client)
        # msgs = [
        #     {
        #         "topic": "application/c42cfa44-9586-4266-834b-bd412c33c488/device/2000000000000001/command/down",
        #         # "topic": "eu868/gateway/aa555a0000000101/command/down",
        #         "payload": json.dumps(
        #             {
        #                 "confirmed": True,
        #                 "fPort": 10,
        #                 "devEui": "2000000000000001",
        #                 "data": base64.b64encode(bytes(cmd)).decode(),
        #             }
        #         ),
        #     }
        #     for cmd in commands
        # ]

        # # save last commands for hash retry
        # r_client.set(
        #     REDIS_SEPARATOR.join([REDIS_PREFIX, LAST_COMMANDS_PREFIX, flow["id"]]),
        #     json.dumps(commands),
        # )

        # prev_cmds = []
        # if cmds := r_client.get(REDIS_SEPARATOR.join([REDIS_PREFIX, COMMANDS_PREFIX, flow["id"]])):
        #     prev_cmds = json.loads(cmds)

        # prev_cmds.extend(commands)
        # r_client.set(
        #     REDIS_SEPARATOR.join([REDIS_PREFIX, COMMANDS_PREFIX, flow["id"]]), json.dumps(prev_cmds)
        # )
        # if task["task"] == "delete":
        #     r_client.delete([REDIS_PREFIX, COMMANDS_PREFIX, flow["id"]])

        # pprint(commands)
        # for msg in msgs:
        #     publish.single(
        #         msg["topic"],
        #         msg["payload"],
        #         hostname=MQTT_HOST,
        #         port=MQTT_PORT,
        #         auth={"username": MQTT_USERNAME, "password": MQTT_PASSWORD},
        #     )

        if task["task"] == "delete":
            r_client.delete([REDIS_PREFIX, COMMANDS_PREFIX, flow["id"]])


def get_node_key(flow_id: str, node_id: str, r_client: redis.Redis) -> int:
    lb_node_dict = REDIS_SEPARATOR.join([REDIS_PREFIX, "flow", flow_id, "lb-nodes"])
    ui_node_dict = REDIS_SEPARATOR.join([REDIS_PREFIX, "flow", flow_id, "ui-nodes"])
    return int(_get_key(node_id, r_client, lb_node_dict, ui_node_dict))


def get_flow_key(id: str, r_client: redis.Redis) -> int:
    return int(_get_key(id, r_client, flow_lb_key, flow_ui_key))


def lookup_ui_key(id: int, r_client: redis.Redis) -> str:
    return r_client.hget(flow_lb_key, id)


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
    lb_node_dict = REDIS_SEPARATOR.join([REDIS_PREFIX, "flow", str(id), "lb-nodes"])
    ui_node_dict = REDIS_SEPARATOR.join([REDIS_PREFIX, "flow", str(id), "ui-nodes"])
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


type_parameter_commands = {
    "hysteresis": {
        "min": lambda flow_id_lb, node_id_lb, value: [
            action_bytes.PARAMETER_UPDATE,
            flow_id_lb,
            node_id_lb,
            0,
            4,
            2,
            *list(struct.pack("!f", float(value))),
        ],
        "max": lambda flow_id_lb, node_id_lb, value: [
            action_bytes.PARAMETER_UPDATE,
            flow_id_lb,
            node_id_lb,
            1,
            4,
            2,
            *list(struct.pack("!f", float(value))),
        ],
    },
    "countdown": {
        "counter": lambda flow_id_lb, node_id_lb, value: [
            action_bytes.PARAMETER_UPDATE,
            flow_id_lb,
            node_id_lb,
            0,
            4,
            1,
            *list(struct.pack("!i", int(value))),
        ]
    },
    "timer": {
        "start_hour": lambda flow_id_lb, node_id_lb, value: [
            action_bytes.PARAMETER_UPDATE,
            flow_id_lb,
            node_id_lb,
            0,
            1,
            1,
            int(value),
        ],
        "stop_hour": lambda flow_id_lb, node_id_lb, value: [
            action_bytes.PARAMETER_UPDATE,
            flow_id_lb,
            node_id_lb,
            1,
            1,
            1,
            int(value),
        ],
        "start_minute": lambda flow_id_lb, node_id_lb, value: [
            action_bytes.PARAMETER_UPDATE,
            flow_id_lb,
            node_id_lb,
            2,
            1,
            1,
            int(value),
        ],
        "stop_minute": lambda flow_id_lb, node_id_lb, value: [
            action_bytes.PARAMETER_UPDATE,
            flow_id_lb,
            node_id_lb,
            3,
            1,
            1,
            int(value),
        ],
    },
}


def _add_node(flow_id_lb, flow_id_ui, node, r_client) -> list:
    node_id_lb = get_node_key(flow_id_ui, node["id"], r_client)
    node_type_lb = NODE_TYPES[node["type"]]
    commands = []
    if node["type"] in ["binarysensor", "binarydevice"]:
        print(node)
        # add device
        # add device, flow id, node id, Binarysensor=3||Numericsensor=4, LB Device number, attribute
        try:
            attr_id = int(device_classes.DEVICE_CLASSES.index(node["data"]["attribute"]))
        except ValueError:
            raise UnknownAttributeError("attribute unknown")
        try:
            dev_id = int(node["data"]["device"])
        except ValueError:
            raise UnknownDeviceError("device unknown")
        # TODO change direkt number to name and lookup (also get list of available sensors and devices)
        commands.append(
            [
                action_bytes.ADD_DEVICE,
                flow_id_lb,
                node_id_lb,
                node_type_lb,
                dev_id,
                attr_id,
            ]
        )
    else:
        # add node
        # add node, flow id, node id, node type
        commands.append([action_bytes.ADD_NODE, flow_id_lb, node_id_lb, node_type_lb])
        match node["type"]:
            case "hysteresis":
                # min value
                # add param, flow id, node id, param index 0, 4 bytes, float 2, min value as 4 ints representing the bytes
                commands.append(
                    type_parameter_commands["hysteresis"]["min"](
                        flow_id_lb, node_id_lb, node["data"]["min"]
                    )
                )
                # max value
                # add param, flow id, node id, param index 1, 4 bytes, float 2, max value as 4 ints representing the bytes
                commands.append(
                    type_parameter_commands["hysteresis"]["max"](
                        flow_id_lb, node_id_lb, node["data"]["max"]
                    )
                )
            case "countdown":
                # add param, flow id, node id, param index 0, 4 bytes, integer 1, counter as 4 byte integer value
                commands.append(
                    type_parameter_commands["countdown"]["counter"](
                        flow_id_lb, node_id_lb, node["data"]["counter"]
                    )
                )
            case "timer":
                start = node["data"]["start"].split(":")
                stop = node["data"]["stop"].split(":")
                # hour min
                # add param, flow id, node id, param index 0, 1 byte, integer 1, hour min as int
                commands.append(
                    type_parameter_commands["timer"]["start_hour"](flow_id_lb, node_id_lb, start[0])
                )
                # hour max
                # add param, flow id, node id, param index 1, 1 byte, integer 1, hour max as int
                commands.append(
                    type_parameter_commands["timer"]["stop_hour"](flow_id_lb, node_id_lb, stop[0])
                )
                # minute min
                # add param, flow id, node id, param index 2, 1 byte, integer 1, minute min as int
                commands.append(
                    type_parameter_commands["timer"]["start_minute"](
                        flow_id_lb, node_id_lb, start[1]
                    )
                )
                # minute max
                # add param, flow id, node id, param index 3, 1 byte, integer 1, minute max as int
                commands.append(
                    type_parameter_commands["timer"]["stop_minute"](flow_id_lb, node_id_lb, stop[1])
                )
    return commands


def parse_new_flow(flow: any, r_client: redis.Redis):
    # - save flow for diff
    #
    commands = []
    # TODO autoremove flow ONLY for now
    # if flow_exists(flow["id"], r_client):
    #     commands.append([action_bytes.REMOVE_FLOW, get_flow_key(flow["id"], r_client)])
    print("hy")
    flow_id_lb = get_flow_key(flow["id"], r_client)
    print(flow)
    # add flow
    commands.append([action_bytes.ADD_FLOW, flow_id_lb])
    for node in flow["nodes"]:
        commands.extend(_add_node(flow_id_lb, flow["id"], node, r_client))

    for edge in flow["edges"]:
        # connect node, flow id, output node id (source), output id, input node id (target), input id
        commands.append(
            [
                action_bytes.CONNECT_NODE,
                flow_id_lb,
                get_node_key(flow["id"], edge["source"], r_client),
                0,
                get_node_key(flow["id"], edge["target"], r_client),
                0,
            ]
        )
    # flow complete
    commands.append([action_bytes.FLOW_COMPLETE, flow_id_lb])

    # save current flow for diff
    r_client.set(REDIS_SEPARATOR.join([REDIS_PREFIX, "flow", flow["id"]]), json.dumps(flow))
    return commands


def upload_flow(flow: any, r_client: redis.Redis) -> list:
    commands = []
    if flow_exists(flow["id"], r_client):
        flow_id_lb = get_flow_key(flow["id"], r_client)
        # upload flow
        commands.append([action_bytes.UPLOAD_FLOW, flow_id_lb])
    return commands


def enable_flow(flow: any, r_client: redis.Redis) -> list:
    commands = []
    if flow_exists(flow["id"], r_client):
        flow_id_lb = get_flow_key(flow["id"], r_client)
        # enable flow
        commands.append([action_bytes.ENABLE_FLOW, flow_id_lb])
    return commands


def get_flow(id: str, r_client: redis.Redis) -> any:
    if tmp := r_client.get(REDIS_SEPARATOR.join([REDIS_PREFIX, "flow", id])):
        return json.loads(tmp)


def diff_flow(flow: any, r_client: redis.Redis):
    # - new nodes
    # - deleted nodes
    # - new edges
    # - deleted edges
    # - changed params
    # raise NotImplementedError("Edge deletion missing")
    commands = []
    flow_id_lb = get_flow_key(flow["id"], r_client)
    # old_flow = r_client.get(REDIS_SEPARATOR.join([REDIS_PREFIX, "flow", flow["id"]]))
    old_flow = get_flow(flow["id"], r_client)
    if not old_flow:
        # no old flow, means that parse_new_flow likely crashed before
        # so try to readd
        commands.extend(del_flow(flow, r_client))
        commands.extend(parse_new_flow(flow, r_client))
        return commands
    # old_flow = json.loads(old_flow)
    nodes = set(x["id"] for x in flow["nodes"])
    old_nodes = {x["id"]: x for x in old_flow["nodes"]}
    edges = set(x["id"] for x in flow["edges"])
    old_edges = {x["id"]: x for x in old_flow["edges"]}
    new_edges = []
    removed_edges = []
    new_nodes = []
    removed_nodes = []
    changed_nodes = []
    for node in flow["nodes"]:
        if node["id"] not in old_nodes:
            new_nodes.append(node)
        else:
            if node["data"] != old_nodes[node["id"]]["data"]:
                changed_nodes.append(node)
    for node in old_flow["nodes"]:
        if node["id"] not in nodes:
            removed_nodes.append(node)

    for node in removed_nodes:
        node_id_lb = get_node_key(flow["id"], node["id"], r_client)
        commands.append([action_bytes.REMOVE_NODE, flow_id_lb, node_id_lb])

    for node in new_nodes:
        commands.extend(_add_node(flow_id_lb, flow["id"], node, r_client))

    for node in changed_nodes:
        print("changed")
        print(changed_nodes)
        node_id_lb = get_node_key(flow["id"], node["id"], r_client)
        match node["type"]:
            case "binarysensor" | "binarydevice":
                commands.append([action_bytes.REMOVE_NODE, flow_id_lb, node_id_lb])
                commands.extend(_add_node(flow_id_lb, flow["id"], node, r_client))
            case "hysteresis":
                if node["data"]["min"] != old_nodes[node["id"]]["data"]["min"]:
                    commands.append(
                        type_parameter_commands["hysteresis"]["min"](
                            flow_id_lb, node_id_lb, node["data"]["min"]
                        )
                    )

                if node["data"]["max"] != old_nodes[node["id"]]["data"]["max"]:
                    commands.append(
                        type_parameter_commands["hysteresis"]["max"](
                            flow_id_lb, node_id_lb, node["data"]["max"]
                        )
                    )
            case "countdown":
                commands.append(
                    type_parameter_commands["countdown"]["counter"](
                        flow_id_lb, node_id_lb, node["data"]["counter"]
                    )
                )
            case "timer":
                start = node["data"]["start"].split(":")
                stop = node["data"]["stop"].split(":")
                start_old = old_nodes[node["id"]]["data"]["start"].split(":")
                stop_old = old_nodes[node["id"]]["data"]["stop"].split(":")
                if start[0] != start_old[0]:
                    commands.append(
                        type_parameter_commands["timer"]["start_hour"](
                            flow_id_lb, node_id_lb, start[0]
                        )
                    )
                if stop[0] != stop_old[0]:
                    commands.append(
                        type_parameter_commands["timer"]["stop_hour"](
                            flow_id_lb, node_id_lb, stop[0]
                        )
                    )
                if start[1] != start_old[1]:
                    commands.append(
                        type_parameter_commands["timer"]["start_minute"](
                            flow_id_lb, node_id_lb, start[1]
                        )
                    )
                if stop[1] != stop_old[1]:
                    commands.append(
                        type_parameter_commands["timer"]["stop_minute"](
                            flow_id_lb, node_id_lb, stop[1]
                        )
                    )

    for edge in flow["edges"]:
        if edge["id"] not in old_edges:
            new_edges.append(edge)

    for edge in old_flow["edges"]:
        if edge["id"] not in edges:
            removed_edges.append(edge)

    for edge in new_edges:
        commands.append(
            [
                action_bytes.CONNECT_NODE,
                flow_id_lb,
                get_node_key(flow["id"], edge["source"], r_client),
                0,
                get_node_key(flow["id"], edge["target"], r_client),
                0,
            ]
        )

    for edge in removed_edges:
        commands.append(
            [action_bytes.DISCONNECT_NODE, get_node_key(flow["id"], edge["source"], r_client), 0]
        )

    # save current flow for diff
    r_client.set(REDIS_SEPARATOR.join([REDIS_PREFIX, "flow", flow["id"]]), json.dumps(flow))

    if commands:
        # flow complete
        commands.append([action_bytes.FLOW_COMPLETE, flow_id_lb])
        # # upload flow
        # commands.append([action_bytes.UPLOAD_FLOW, flow_id_lb])
        # # enable flow
        # commands.append([action_bytes.ENABLE_FLOW, flow_id_lb])
    return commands


def flow_exists(id: str, r_client: redis.Redis) -> bool:
    return r_client.hexists(flow_ui_key, id)


def del_flow(flow: any, r_client: redis.Redis) -> list:
    commands = []
    if flow_exists(flow["id"], r_client):
        flow_id_lb = get_flow_key(flow["id"], r_client)
        commands.append([action_bytes.REMOVE_FLOW, flow_id_lb])
        del_flow_id(flow_id_lb, r_client)
        r_client.delete(REDIS_SEPARATOR.join([REDIS_PREFIX, "flow", flow["id"]]))
    return commands


def disable_flow(flow: any, r_client: redis.Redis) -> list:
    commands = []
    if flow_exists(flow["id"], r_client):
        commands.append([action_bytes.DISABLE_FLOW, get_flow_key(flow["id"], r_client)])
    return commands


def enable_flow(flow: any, r_client: redis.Redis) -> list:
    commands = []
    if flow_exists(flow["id"], r_client):
        commands.append([action_bytes.ENABLE_FLOW, get_flow_key(flow["id"], r_client)])
    return commands


if __name__ == "__main__":
    main()
