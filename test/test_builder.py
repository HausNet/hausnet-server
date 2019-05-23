from typing import cast, Dict, Union
import asyncio
import unittest

from aioreactive.core import subscribe, AsyncAnonymousObserver

from hausnet.builders import RootBuilder
from hausnet.devices import NodeDevice, BasicSwitch
from hausnet.flow import TestableBufferedAsyncSource
from hausnet.operators.operators import HausNetOperators as Op


class DeviceBuilderTests(unittest.TestCase):
    """Test the building of the device tree"""

    def test_can_build_single_node_with_single_device(self):
        """Can a basic node + device be built?"""
        tree = RootBuilder().build({
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
        self.assertIn('test_switch', node.sub_devices, "Expected test_switch sub-device key")
        sub_device: BasicSwitch = cast(BasicSwitch, node.sub_devices['test_switch'])
        self.assertIs(sub_device.__class__, BasicSwitch, "Sub-device should be a BasicSwitch")
        self.assertEqual(sub_device.device_id, 'switch')

    def test_can_build_multiple_nodes_with_multiple_devices(self):
        """Can a set of basic nodes, each with multiple devices be built?"""
        tree = RootBuilder().build({
            'test_node_1': {
                'type':      'node',
                'device_id': 'test/ABC123',
                'devices': {
                    'test_switch_A': {
                        'type':      'basic_switch',
                        'device_id': 'switch_1',
                    },
                    'test_switch_B': {
                        'type':      'basic_switch',
                        'device_id': 'switch_2',
                    }
                }
            },
            'test_node_2': {
                'type':      'node',
                'device_id': 'test/ABC124',
                'devices': {
                    'test_switch_A': {
                        'type':      'basic_switch',
                        'device_id': 'switch_1',
                    },
                    'test_switch_B': {
                        'type':      'basic_switch',
                        'device_id': 'switch_2',
                    },
                    'test_switch_C': {
                        'type':      'basic_switch',
                        'device_id': 'switch_3',
                    },
                }
            }
        })
        # Test nodes
        self.assertEqual(len(tree), 2, "Expected two nodes at the root of the tree")
        self.assertIs(tree['test_node_1'].__class__, NodeDevice, "Top-level device #1 should be a NodeDevice")
        self.assertIs(tree['test_node_2'].__class__, NodeDevice, "Top-level device #2 should be a NodeDevice")
        node_1: NodeDevice = cast(NodeDevice, tree['test_node_1'])
        self.assertEqual(node_1.device_id, 'test/ABC123', "Expected 'test/ABC123' as device_id for node")
        node_2 = cast(NodeDevice, tree['test_node_2'])
        self.assertEqual(node_2.device_id, 'test/ABC124', "Expected 'test/ABC124' as device_id for node")
        # Test sub-devices on node 1
        self.assertEqual(len(node_1.sub_devices), 2, "Expected two sub-devices")
        self.assertIn('test_switch_A', node_1.sub_devices, "Expected test_switch_A sub-device key")
        self.assertIn('test_switch_B', node_1.sub_devices, "Expected test_switch_B sub-device key")
        sub_device: BasicSwitch = cast(BasicSwitch, node_1.sub_devices['test_switch_A'])
        self.assertIs(sub_device.__class__, BasicSwitch, "Sub-device A should be a BasicSwitch")
        self.assertEqual(sub_device.device_id, 'switch_1', "Sub-device A's ID should be switch_1")
        sub_device = cast(BasicSwitch, node_1.sub_devices['test_switch_B'])
        self.assertIs(sub_device.__class__, BasicSwitch, "Sub-device B should be a BasicSwitch")
        self.assertEqual(sub_device.device_id, 'switch_2', "Sub-device B's ID should be switch_2")
        # Test sub-devices on node B
        self.assertEqual(len(node_2.sub_devices), 3, "Expected two sub-devices")
        self.assertIn('test_switch_A', node_2.sub_devices, "Expected test_switch_A sub-device key")
        self.assertIn('test_switch_B', node_2.sub_devices, "Expected test_switch_B sub-device key")
        self.assertIn('test_switch_C', node_2.sub_devices, "Expected test_switch_C sub-device key")
        sub_device: BasicSwitch = cast(BasicSwitch, node_2.sub_devices['test_switch_A'])
        self.assertIs(sub_device.__class__, BasicSwitch, "Sub-device A should be a BasicSwitch")
        self.assertEqual(sub_device.device_id, 'switch_1', "Sub-device A's ID should be switch_1")
        sub_device = cast(BasicSwitch, node_2.sub_devices['test_switch_B'])
        self.assertIs(sub_device.__class__, BasicSwitch, "Sub-device B should be a BasicSwitch")
        self.assertEqual(sub_device.device_id, 'switch_2', "Sub-device B's ID should be switch_2")
        sub_device = cast(BasicSwitch, node_2.sub_devices['test_switch_C'])
        self.assertIs(sub_device.__class__, BasicSwitch, "Sub-device C should be a BasicSwitch")
        self.assertEqual(sub_device.device_id, 'switch_3', "Sub-device C's ID should be switch_3")

    def test_switch_upstream_wiring_delivers(self):
        """Test that a basic switch state updates get delivered to it and the external world"""
        blueprint = {
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
        }
        source = TestableBufferedAsyncSource(2)
        device_tree, flat_devices = RootBuilder().build(blueprint, source)
        self.assertIn('test_node')
        self.assertIsInstance()

        device_messages = []

        async def stream_observer(message: Dict[str, Union[str, int, float]]):
            print(message)
            device_messages.append(message)

        async def main():
            source = (2)
            self.inject_messages(
                source,
                [
                    {
                        'topic':   'hausnet/test/ABC123/upstream',
                        'message': '{"switch": {"state": "OFF"}}'
                    },
                    {
                        'topic':   'hausnet/test/ABC123/upstream',
                        'message': '{"switch": {"state": "ON"}}'
                    },
                ]
                )
            streams = []
            for name, device in tree['test_node'].sub_devices.items():
                streams.append(
                        source
                        | Op.filter(lambda msg: msg['topic'].startswith(tree['test_node'].topic_prefix()))
                        | Op.map(lambda msg: node.coder.decode(msg['message']))
                        | Op.filter(lambda msg_dict, name=device.name: name in msg_dict)
                        | Op.map(lambda msg_dict, name=device.name: msg_dict[name])
                        | Op.tap(lambda dev_msg, dev=device: dev.state.set_value(dev_msg['state']))
                    )
            for stream in streams:
                await subscribe(stream, AsyncAnonymousObserver(stream_observer))

        loop = asyncio.get_event_loop()
        loop.run_until_complete(main())
        loop.close()
        self.assertEqual(3, len(device_messages), "Expected device messages")
        self.assertEqual({'state': 'OFF'}, device_messages[0], "switch_1 state should be 'OFF'")
        self.assertEqual({'state': 'ON'}, device_messages[1], "switch_2 state should be 'ON'")
        self.assertEqual({'state': 'UNDEFINED'}, device_messages[2], "switch_1 state should be 'UNDEFINED'")
        self.assertEqual('UNDEFINED', switch_1.state.value, "switch_1 state should be 'UNDEFINED'")
        self.assertEqual('ON', switch_2.state.value, "switch_2 state should be 'ON'")

    @staticmethod
    def inject_messages(source: TestableBufferedAsyncSource, messages: List[Dict[str, str]]):
        source.max_messages = len(messages)
        for message in messages:
            source.buffer(message)
