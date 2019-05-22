from typing import cast
import unittest
import re

from hausnet.builders import DeviceTreeBuilder, StructureBuilder
from hausnet.devices import NodeDevice, BasicSwitch


class DeviceBuilderTests(unittest.TestCase):
    """ Test the building of the device tree
    """
    def test_can_build_single_node_with_single_device(self):
        tree = StructureBuilder().build({
            'test_node': {
                'type': 'node',
                'device_id': 'test/ABC123',
                'devices': {
                    'test_switch': {
                        'type': 'basic_switch',
                        'device_id': 'switch',
                        }
                    }
                }
            })
        self.assertEqual(len(tree), 1, "Expected one device at the root of the tree")
        self.assertIs(tree['test_node'].__class__, NodeDevice, "Top-level device should be a NodeDevice")
        node: NodeDevice = cast(NodeDevice, tree['test_node'])
        self.assertEqual(node.device_id, 'test/ABC123', "Expected 'test/ABC123' as device_id for node")
        self.assertEqual(len(node.sub_devices), 1, "Expected one sub-device")
        self.assertIn(node.sub_devices, 'test_switch', "Expected test_switch sub-device key")
        sub_device: BasicSwitch = cast(BasicSwitch, node.sub_devices['test_switch'])
        self.assertIs(sub_device.__class__, BasicSwitch, "Sub-device should be a BasicSwitch")
        self.assertEqual(sub_device.device_id, 'switch')

    def test_missing_type_reports_error(self):
        """ Test conditions where the type is omitted, or is not a valid type
        """
        tree, errors = DeviceTreeBuilder().build([
            {
                'device_id':  'test/ABC123',
                'limit': 7
            },
            {
                'type': 'NodeDevice',
                'device_id': 'test/456DEF',
            },
        ])
        self.assertEqual(len(tree), 1, "Expected one device to have been defined")
        self.assertEqual(len(errors), 1, "Expected one error found")
        self.assertRegex(errors[0], re.compile("Device type missing for:"))
        self.assertRegex(errors[0], re.compile("test/ABC123"))
        tree, errors = DeviceTreeBuilder().build([
            {
                'type':  'NonExistentType',
                'device_id':  'test/ABC123',
                'limit': 7
                },
            {
                'type': 'NodeDevice',
                'device_id': 'test/ABC123',
                },
            ])
        self.assertEqual(len(tree), 1, "Expected one device to have been defined")
        self.assertEqual(len(errors), 1, "Expected one error found")
        self.assertRegex(errors[0], re.compile("Non-existent device type for:"))
        self.assertRegex(errors[0], re.compile("test/ABC123"))

    def test_missing_required_param_raises_exception(self):
        """ Test that a missing required parameter raises an error
        """
        tree, errors = DeviceTreeBuilder().build([
            {
                'type':  'NodeDevice',
                'limit': 7
                },
            {
                'type':  'NodeDevice',
                'device_id':  'test/456DEF',
                },
            ])
        self.assertEqual(len(tree), 1, "Expected one device to have been defined")
        self.assertEqual(len(errors), 1, "Expected one error found")
        self.assertRegex(errors[0], re.compile("Missing required parameter 'device_id' for:"))
        self.assertRegex(errors[0], re.compile("limit"))
        self.assertNotRegex(errors[0], re.compile("test/ABC123"))

    def test_nodes_with_devices_constructed(self):
        """ Test that nodes with subsidiary devices are constructed
        """
        tree, errors = DeviceTreeBuilder().build([
            {
                'type':    'NodeDevice',
                'device_id':    'test/ABC123',
                'devices': [
                    {
                        'type': 'BasicSwitch',
                        'device_id': 'test_switch',
                        },
                    {
                        'type': 'BasicSwitch',
                        'device_id': 'test_switch_2',
                        },
                    ]
                },
            {
                'type':  'NodeDevice',
                'device_id':  'test/456DEF',
                },
        ])
        self.assertEqual(len(errors), 0, "Expected no errors.")
        self.assertEqual(len(tree['test/ABC123'].devices), 2, "Expected two subsidiary devices")
        self.assertIsInstance(tree['test/ABC123'].devices['test_switch'], BasicSwitch, "Expected a BasicSwitch object")
