import socket
import os
import time
import logging
import threading
from typing import Callable, Dict, Any
import paho.mqtt.client as mqttc

logger = logging.getLogger(__name__)


# noinspection PyUnusedLocal
def connect_cb(client: mqttc.Client, user_data: Dict[str, Any], flags: Dict[str, Any], rc: str):
    """ On connection, subscribe to registered listeners, and handle errors (TBD)
    """
    if rc != mqttc.CONNACK_ACCEPTED:
        logger.critical("Connection failed with code %s: %s", rc, mqttc.connack_string(rc))
        return
    manager = user_data['manager']
    manager.connected = True


# noinspection PyUnusedLocal
def disconnect_cb(client: mqttc.Client, user_data: Dict[str, Any], rc: str):
    """ Just set the manager's connected flag and log the reason
    """
    if rc != mqttc.MQTT_ERR_SUCCESS:
        logger.critical("Unexpected disconnection, code: %s", rc)
    manager = user_data['manager']
    manager.connected = False


# noinspection PyUnusedLocal
def message_cb(client: mqttc.Client, user_data: Dict[str, Any], message: mqttc.MQTTMessage):
    """ Called when a message is received on a subscribed-to topic. Relays it to the appropriate listener
    """
    logger.debug("Message received on topic %s: %s", message.topic, message.payload)
    manager = user_data['manager']
    manager.listenerLock.acquire()
    if message.topic not in manager.listeners:
        logger.error("No listener for topic '%s'", message.topic)
    else:
        manager.listeners[message.topic](message.payload.decode('ASCII'))
    manager.listenerLock.release()


# noinspection PyUnusedLocal
def subscribe_cb(client: mqttc.Client, user_data: Dict[str, Any], mid, granted_qos):
    logger.debug("Subscribed, message ID: %s", mid)


class MqttManager:
    """ Manages MQTT communication. Incoming message topics can be subscribed to by listeners, which will receive
        messages as they arrive. Only one listener per topic is allowed - registering a listener overwrites the old
        listener.
    """

    def __init__(self, host, port=1883):
        """ Initializes manager. Creates a Paho MQTT client with a connect_async, which needs a subsequent loop_start
            to complete.

            :param host: Host name of broker.
            :param port: Port to use, defaults to standard.
        """
        self.host = host
        self.port = port
        self.listeners = {}
        self.listenerLock = threading.Lock()
        self.connected = False
        self.client = mqttc.Client(self.client_id())
        self.client.on_connect = connect_cb
        self.client.on_disconnect = disconnect_cb
        self.client.on_subscribe = subscribe_cb
        self.client.on_message = message_cb
        self.client.user_data_set({'manager': self})

    def register_listeners(self, listeners: Dict[str, Callable[[str], None]]):
        """ Register callbacks to call when a message arrives on associated topics, and subscribe to each. May be
            called with the manager's list of listeners, as each listener in that list will be replaced by itself
            while being subscribed to.

            :param listeners: Dictionary, indexed by topic name, of callbacks taking a string argument.
        """
        self.listenerLock.acquire()
        for topic, listener in listeners.items():
            self.listeners[topic] = listener
            if not self.connected: continue
            self.client.subscribe(topic)
        self.listenerLock.release()

    def run(self):
        """ Connect to broker, then subscribe to topics.

            TODO: Use thread event instead of sleep loop
            TODO: Avoid long startup times by backgrounding connection on another thread / asyncio
        """
        self.client.loop_start()
        self.client.connect(self.host, self.port)
        while not self.connected:
            logging.debug("Waiting for connection...")
            time.sleep(0.1)
        self.register_listeners(self.listeners)

    @staticmethod
    def client_id():
        """ Generate a client ID
        """
        return "%s/hausnet/%d" % (socket.gethostname(), os.getpid())
