import unittest
from hausnet.coders import *


class CoderTests(unittest.TestCase):
    """Test the structured message communications management"""

    def test_json_decode(self):
        """Test that structured messages in JSON are decoded correctly"""
        coder = JsonCoder()
        decoded_value = coder.decode('{"device_id": 1,"value": "some_value"}')
        self.assertEqual({'device_id': 1, 'value': 'some_value'}, decoded_value, "Incorrect decoding")

    def test_json_encode(self):
        """Test that an object is correctly encoded into JSON"""
        coder = JsonCoder()
        encoded_value = coder.encode({'device_id': 1, 'value': 'some_value'})
        self.assertEqual('{"device_id":1,"value":"some_value"}', encoded_value, "Incorrect encoding")


