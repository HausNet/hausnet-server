from typing import cast
import asyncio
import unittest

from aioreactive.core import subscribe, AsyncAnonymousObserver, AsyncObservable, AsyncStream

from hausnet.builders import DevicePlantBuilder
from hausnet.devices import NodeDevice, BasicSwitch
from hausnet.flow import FixedSyncToAsyncBufferedStream
from hausnet.states import OnOffState


class DeviceBuilderTests(unittest.TestCase):
    """Test the building of the device tree"""

    def test_can_build_single_node_with_single_device(self):
        """Can a basic node + device be built?"""
        bundles = DevicePlantBuilder(AsyncObservable()).build({
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
        self.assertEqual(len(bundles), 3, "Expected 3 device bundles")
        root = bundles['root'].device
        self.assertEqual(len(root.sub_devices), 1, "Expected one device at the root of the tree")
        self.assertIs(root.sub_devices['test_node'].__class__, NodeDevice, "Top-level device should be a NodeDevice")
        node: NodeDevice = cast(NodeDevice, root.sub_devices['test_node'])
        self.assertEqual(node.device_id, 'test/ABC123', "Expected 'test/ABC123' as device_id for node")
        self.assertEqual(len(node.sub_devices), 1, "Expected one sub-device")
        self.assertIn('test_switch', node.sub_devices, "Expected test_switch sub-device key")
        sub_device: BasicSwitch = cast(BasicSwitch, node.sub_devices['test_switch'])
        self.assertIs(sub_device.__class__, BasicSwitch, "Sub-device should be a BasicSwitch")
        self.assertEqual(sub_device.device_id, 'switch')

    def test_can_build_multiple_nodes_with_multiple_devices(self):
        """Can a set of basic nodes, each with multiple devices be built?"""
        bundles = DevicePlantBuilder(AsyncObservable()).build({
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
        # Bundles
        self.assertEqual(len(bundles), 8, "Expected 8 device bundles")
        # Test nodes
        root = bundles['root'].device
        self.assertEqual(len(root.sub_devices), 2, "Expected two nodes at the root of the tree")
        node_1 = root.sub_devices['test_node_1']
        self.assertIs(node_1.__class__, NodeDevice, "Top-level device #1 should be a NodeDevice")
        node_2 = root.sub_devices['test_node_2']
        self.assertIs(node_2.__class__, NodeDevice, "Top-level device #2 should be a NodeDevice")
        node_1: NodeDevice = cast(NodeDevice, bundles['test_node_1'].device)
        self.assertEqual(node_1.device_id, 'test/ABC123', "Expected 'test/ABC123' as device_id for node")
        node_2 = cast(NodeDevice, bundles['test_node_2'].device)
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
        """Test that a basic switch state updates get delivered to the external world"""
        blueprint = {
            'test_node': {
                'type': 'node',
                'device_id': 'test/ABC123',
                'devices': {
                    'test_switch_1': {
                        'type': 'basic_switch',
                        'device_id': 'switch_1',
                    },
                    'test_switch_2': {
                        'type':      'basic_switch',
                        'device_id': 'switch_2',
                    }
                }
            }
        }
        loop = asyncio.new_event_loop()
        source = FixedSyncToAsyncBufferedStream(loop, 2)
        bundles = DevicePlantBuilder(source).build(blueprint)
        source.buffer({'topic': 'hausnet/test/ABC123/upstream', 'message': '{"switch_1": {"state": "OFF"}}'})
        source.buffer({'topic': 'hausnet/test/ABC123/upstream', 'message': '{"switch_2": {"state": "ON"}}'})

        async def message_dump(message):
            print(message)

        async def main():
            await subscribe(bundles['test_node.test_switch_1'].up_stream, AsyncAnonymousObserver(message_dump))
            await subscribe(bundles['test_node.test_switch_2'].up_stream, AsyncAnonymousObserver(message_dump))
            await source.stream()

        loop.run_until_complete(main())
        loop.close()
        self.assertEqual(
            bundles['test_node.test_switch_1'].device.state.value,
            OnOffState.OFF,
            "Expected switch 1 to be OFF"
        )
        self.assertEqual(
            bundles['test_node.test_switch_2'].device.state.value,
            OnOffState.ON,
            "Expected switch 1 to be ON"
        )

    def test_switch_downstream_wiring_delivers(self):
        """Test that a basic switch state changes get delivered to the MQTT end of the stream"""
        blueprint = {
            'test_node': {
                'type': 'node',
                'device_id': 'test/ABC123',
                'devices': {
                    'test_switch': {
                        'type': 'basic_switch',
                        'device_id': 'switch_1',
                    },
                }
            }
        }
        loop = asyncio.new_event_loop()
        source = AsyncStream()
        sink = AsyncStream()
        bundles = DevicePlantBuilder(source, sink).build(blueprint)
        msg_bucket = []

        async def message_dump(message):
            print(message)
            msg_bucket.append(message)

        async def main():
            await subscribe(bundles['test_node.test_switch'].down_stream, AsyncAnonymousObserver(message_dump))

            for msg in [{'state': 'ON'}, {'state': 'OFF'}]:
                await sink.asend(msg)

        loop.run_until_complete(main())
        loop.close()
        self.assertEqual(
            msg_bucket[0],
            {'topic': 'hausnet/test_node/ABC123/downstream', 'message': '{"test_switch":{"state": "OFF"}}'},
            "Expected an 'OFF' JSON message on topic ''hausnet/test_node/ABC123/downstream'"
        )
        self.assertEqual(
            msg_bucket[1],
            {'topic': 'hausnet/test_node/ABC123/downstream', 'message': '{"test_switch":{"state": "ON"}}'},
            "Expected an 'ON' JSON message on topic ''hausnet/test_node/ABC123/downstream'"
        )
