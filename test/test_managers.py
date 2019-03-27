import unittest
import unittest.mock as mock
import os
import subprocess
import time
from hausnet import manager
from hausnet import coder
from hausnet.config import conf


def send_mqtt_message(topic: str, payload: str):
    """ Convenience function to send an MQTT message via the external mosquitto publication client.

        :param topic:   Where to send message to.
        :param payload: The actual message.
    """
    subprocess.check_call([
        'mosquitto_pub',
        '-h', conf.MQTT_BROKER,
        '-t', topic,
        '-m', payload
    ])


class MqttClientTests(unittest.TestCase):
    """ Test the MQTT communications management
    """
    @staticmethod
    def test_message_receipt():
        """ Test that messages sent are received
        """
        mqtt_manager = manager.HausNetMqttClient(conf.MQTT_BROKER)
        listener = mock.MagicMock()
        mqtt_manager.set_listener(listener)
        mqtt_manager.run()
        mqtt_manager.subscribe('test')
        send_mqtt_message('test', 'hello')
        time.sleep(10)
        listener.assert_called_with('test', 'hello')


class MessageManagerTests(unittest.TestCase):
    """ Test the structured message communications management
    """
    @staticmethod
    def test_json_receipt():
        """ Test that structured messages from JSON are received and decoded correctly
        """
        msg_manager = manager.MessageCoder(
            manager.HausNetMqttClient(conf.MQTT_BROKER),
            decoder=coder.JsonDecoder()
            )
        listener = mock.MagicMock()
        msg_manager.set_listener(listener)
        msg_manager.mqtt_manager.handle_received_msg('test', '{ "id": 1, "value": "some_value" }')
        listener.assert_called_with('test', {'id': 1, 'value': 'some_value'})

