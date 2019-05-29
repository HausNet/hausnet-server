from typing import Dict, Any, List
import os
import socket
import logging

import paho.mqtt.client as mqttc
from aioreactive.core import AsyncStream
from aioreactive.core.observables import T
import janus

from hausnet.config import conf

logger = logging.getLogger(__name__)

# The namespace prefix for all topics
TOPIC_NAMESPACE = 'hausnet/'

# The topics to be subscribed to - e.g. hausnet/sonoff_switch/ABC123/upstream
TOPICS_SUBSCRIBED_TO = f"{TOPIC_NAMESPACE}+/+/upstream"


class BufferedAsyncStream(AsyncStream):
    """An AsyncStream with an internal buffer, that accepts data synchronously, and sends data asynchronously."""
    def __init__(self, loop) -> None:
        super().__init__()
        self.queue = janus.Queue(loop=loop)

    async def stream_from_queue(self):
        """Permanent loop to send data as it arrives"""
        while True:
            await self.send_from_queue()

    async def send_from_queue(self):
        """Send one value from async queue to async observers"""
        message = await self.queue.async_q.get()
        await self.asend(message)
        self.queue.async_q.task_done()

    def buffer(self, value: T):
        """Place a value in the buffer"""
        self.queue.sync_q.put(value)


class TestableBufferedAsyncStream(BufferedAsyncStream):
    """ A buffered stream for testing that will only transmit a certain number of messages before stopping. Note that
    users of this should inject the expected number of messages.
    """
    def __init__(self, loop, max_messages: int = 0) -> None:
        super().__init__(loop)
        self.max_messages = max_messages
        self.message_count = 0

    async def stream_from_queue(self):
        while True:
            await self.send_from_queue()
            self.message_count += 1
            if self.message_count >= self.max_messages:
                self.queue.close()
                break


class MqttClient(mqttc.Client):
    """ Manages MQTT communication for the HausNet environment. Constrains the Pentaho client to just those
    functions needed to support the needed functionality.
    """

    def __init__(self, up_stream: BufferedAsyncStream, host: str = conf.MQTT_BROKER, port: int = conf.MQTT_PORT):
        """ Initializes client.

            :param host:     Host device_id of broker.
            :param port:     Port to use, defaults to standard.
        """
        super().__init__()
        self.host = host
        self.port = port
        self.on_connect = self.connect_cb
        self.on_disconnect = self.disconnect_cb
        self.on_message = self.message_cb
        self.on_subscribe = self.subscribe_cb
        self.up_stream = up_stream
        self.loop_start()
        logger.info("Connecting to MQTT: host=%s; port=%s", host, str(port))
        self.connected = False
        self.connect(host, port)

    def subscribe_to_network(self):
        """Subscribes to all topics nodes in the network will publish to"""
        logger.debug("Connecting to topic: %s", TOPICS_SUBSCRIBED_TO)
        self.subscribe(TOPICS_SUBSCRIBED_TO)

    # noinspection PyUnusedLocal
    def connect_cb(self, client: mqttc.Client, user_data: Dict[str, Any], flags: Dict[str, Any], rc: str):
        """On connection failure, reconnect"""
        if rc == mqttc.CONNACK_ACCEPTED:
            logger.info("MQTT connected.")
            self.connected = True
            self.subscribe_to_network()
            return
        logger.error("MQTT connection failed: code=%s; text=%s. Retrying...", str(rc), mqttc.connack_string(rc))
        self.connected = False
        self.reconnect()

    # noinspection PyUnusedLocal
    def disconnect_cb(self, client: mqttc.Client, user_data: Dict[str, Any], rc: str):
        """Just set the manager's connected flag and log the reason"""
        logger.error(
            "MQTT unexpectedly disconnected: code=%s; text=%s. Retrying...",
            str(rc),
            mqttc.connack_string(rc)
        )
        self.connected = False
        self.reconnect()

    # noinspection PyUnusedLocal
    def message_cb(self, client: mqttc.Client, user_data: Dict[str, Any], message: mqttc.MQTTMessage):
        """Called when a message is received on a subscribed-to topic. Places the topic + message in the up stream"""
        logger.debug("Message received: topic=%s; message=%s", message.topic, message.payload)
        self.up_stream.buffer({'topic': message.topic, 'message': message.payload})

    # noinspection PyUnusedLocal,PyMethodMayBeStatic
    def subscribe_cb(self, client: mqttc.Client, user_data: Dict[str, Any], mid: Any, granted_qos: List[int]):
        """Called when subscription succeeds"""
        logger.debug("Subscription succeeded")
