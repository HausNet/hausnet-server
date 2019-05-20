import unittest
import re

from hausnet.api import DeviceTreeBuilder
from hausnet.device import NodeDevice, BasicSwitch


class DeviceBuilderTests(unittest.TestCase):
    """ Test the building of the device tree
    """
    def test_build_flat_class(self):
        tree, errors = DeviceTreeBuilder().build([
            {
                'type': NodeDevice.__name__,
                'name': 'test/ABC123',
                'limit': 7
                }
            ])
        self.assertEqual(len(tree), 1, "Expected one device at the root of the tree")
        self.assertIs(tree['test/ABC123'].__class__, NodeDevice, "node_A should be a Buildable")
        self.assertEqual(tree['test/ABC123'].name, 'test/ABC123', "Expected 'test/ABC123' as the device name property")
        # noinspection PyUnresolvedReferences
        self.assertEqual(tree['test/ABC123'].limit, 7, "Expected a limit of '7'")

    def test_missing_type_reports_error(self):
        """ Test conditions where the type is omitted, or is not a valid type
        """
        tree, errors = DeviceTreeBuilder().build([
            {
                'name':  'test/ABC123',
                'limit': 7
            },
            {
                'type': 'NodeDevice',
                'name': 'test/456DEF',
            },
        ])
        self.assertEqual(len(tree), 1, "Expected one device to have been defined")
        self.assertEqual(len(errors), 1, "Expected one error found")
        self.assertRegex(errors[0], re.compile("Device type missing for:"))
        self.assertRegex(errors[0], re.compile("test/ABC123"))
        tree, errors = DeviceTreeBuilder().build([
            {
                'type':  'NonExistentType',
                'name':  'test/ABC123',
                'limit': 7
                },
            {
                'type': 'NodeDevice',
                'name': 'test/ABC123',
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
                'name':  'test/456DEF',
                },
            ])
        self.assertEqual(len(tree), 1, "Expected one device to have been defined")
        self.assertEqual(len(errors), 1, "Expected one error found")
        self.assertRegex(errors[0], re.compile("Missing required parameter 'name' for:"))
        self.assertRegex(errors[0], re.compile("limit"))
        self.assertNotRegex(errors[0], re.compile("test/ABC123"))

    def test_nodes_with_devices_constructed(self):
        """ Test that nodes with subsidiary devices are constructed
        """
        tree, errors = DeviceTreeBuilder().build([
            {
                'type':    'NodeDevice',
                'name':    'test/ABC123',
                'devices': [
                    {
                        'type': 'BasicSwitch',
                        'name': 'test_switch',
                        },
                    {
                        'type': 'BasicSwitch',
                        'name': 'test_switch_2',
                        },
                    ]
                },
            {
                'type':  'NodeDevice',
                'name':  'test/456DEF',
                },
        ])
        self.assertEqual(len(errors), 0, "Expected no errors.")
        self.assertEqual(len(tree['test/ABC123'].devices), 2, "Expected two subsidiary devices")
        self.assertIsInstance(tree['test/ABC123'].devices['test_switch'], BasicSwitch, "Expected a BasicSwitch object")
