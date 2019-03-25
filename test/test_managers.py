import unittest
import unittest.mock as mock
import os
import subprocess
import asyncio
from hausnet.managers import MqttManager


MQTT_BROKER = 'localhost'


class TestManager(unittest.TestCase):
    """ Test the MQTT communications management
    """
    def test_registration_receipt(self):
        """ Test that messages sent are received
        """
        manager = MqttManager('localhost')
        listener = mock.MagicMock()
        manager.register_listeners({'test': listener})
        manager.run()
        subprocess.check_call([
            'mosquitto_pub',
            '-h', MQTT_BROKER,
            '-t', 'test',
            '-m', 'hello'
            ])
        while not listener.called:
            asyncio.sleep(0.1)
        listener.assert_called_with('hello')


def read_json(file_name: str) -> str:
    """ Read a JSON file and the applicable schema into strings, from the
        "json_schema" directory.

        :param file_name: The path of the file relative to the json directory
        :returns: The contents of the file as a string
    """
    json_path = os.path.dirname(os.path.abspath(__file__)) \
        + '/../json_schema/'
    json_file = open(json_path + file_name, 'r')
    content = json_file.read()
    json_file.close()
    return content
