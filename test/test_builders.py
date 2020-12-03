from typing import cast

import asynctest

from hausnet.builders import PlantBuilder
from hausnet.devices import NodeDevice, BasicSwitch
from hausnet.states import OnOffState, FloatState


class DeviceBuilderTests(asynctest.TestCase):
    """Test the building of the device tree"""

    @asynctest.strict()
    async def test_can_build_single_node_with_single_device(self):
        """Can a basic node + device be built?"""
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
        plant = PlantBuilder().build(blueprint, self.loop)
        self.assertEqual(len(plant.device_assemblies), 3, "Expected 3 device interfaces")
        root = plant.device_assemblies['root'].device
        self.assertEqual(len(root.sub_devices), 1, "Expected one device at the root of the tree")
        self.assertIs(root.sub_devices['test_node'].__class__, NodeDevice, "Top-level device should be a NodeDevice")
        node: NodeDevice = cast(NodeDevice, root.sub_devices['test_node'])
        self.assertEqual(node.device_id, 'test/ABC123', "Expected 'test/ABC123' as device_id for node")
        self.assertEqual(len(node.sub_devices), 1, "Expected one sub-device")
        self.assertIn('test_switch', node.sub_devices, "Expected test_switch sub-device key")
        sub_device: BasicSwitch = cast(BasicSwitch, node.sub_devices['test_switch'])
        self.assertIs(sub_device.__class__, BasicSwitch, "Sub-device should be a BasicSwitch")
        self.assertEqual(sub_device.device_id, 'switch')

    @asynctest.strict()
    async def test_can_build_multiple_nodes_with_multiple_devices(self):
        """Can a set of basic nodes, each with multiple devices be built?"""
        blueprint = {
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
        }
        plant = PlantBuilder().build(blueprint, self.loop)
        self.assertEqual(len(plant.device_assemblies), 8, "Expected 8 device interfaces")
        root = plant.device_assemblies['root'].device
        self.assertEqual(len(root.sub_devices), 2, "Expected two nodes at the root of the tree")
        node_1 = root.sub_devices['test_node_1']
        self.assertIs(node_1.__class__, NodeDevice, "Top-level device #1 should be a NodeDevice")
        node_2 = root.sub_devices['test_node_2']
        self.assertIs(node_2.__class__, NodeDevice, "Top-level device #2 should be a NodeDevice")
        node_1: NodeDevice = cast(NodeDevice, plant.device_assemblies['test_node_1'].device)
        self.assertEqual(node_1.device_id, 'test/ABC123', "Expected 'test/ABC123' as device_id for node")
        node_2 = cast(NodeDevice, plant.device_assemblies['test_node_2'].device)
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

    @asynctest.strict()
    async def test_can_build_float_sensor(self):
        """Test that a floating-point sensor can be built from a blueprint."""
        blueprint = {
            'sensor_1': {
                'type':      'sensor',
                'device_id': 'thermo',
                'config': {
                    'state.type': 'float',
                    'state.unit': 'F',
                },
            },
            'sensor_2': {
                'type':      'sensor',
                'device_id': 'thermo',
                'config':    {
                    'state.type': 'float',
                },
            }
        }
        plant = PlantBuilder().build(blueprint, self.loop)
        self.assertEqual(3, len(plant.device_assemblies), "Expected two devices + root to be built")
        self.assertEqual('F', plant.device_assemblies['sensor_1'].device.state.unit, "Expected a unit 'F'")
        self.assertIsInstance(
            plant.device_assemblies['sensor_1'].device.state,
            FloatState,
            "Expected a floating point state"
        )
        self.assertIsNone(plant.device_assemblies['sensor_2'].device.state.unit, "Unit should not exist")
        self.assertIsInstance(
            plant.device_assemblies['sensor_1'].device.state,
            FloatState,
            "Expected a floating point state"
        )

    @asynctest.strict()
    async def test_switch_upstream_wiring_delivers(self):
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
        plant = PlantBuilder().build(blueprint, self.loop)
        messages = [
            {'topic': 'hausnet/test/ABC123/upstream', 'message': '{"switch_1": {"state": "OFF"}}'},
            {'topic': 'hausnet/test/ABC123/upstream', 'message': '{"switch_2": {"state": "ON"}}'}
        ]
        out_messages = []
        in_queue = plant.upstream_src_queue.sync_q
        for message in messages:
            in_queue.put(message)
        plant.upstream_source.start()
        await plant.upstream_source.queue.join()
        plant.upstream_source.stop()
        out_messages.append(await plant.device_assemblies['test_node.test_switch_1'].upstream_pipe.sink.queue.get())
        plant.device_assemblies['test_node.test_switch_1'].upstream_pipe.sink.queue.task_done()
        out_messages.append(await plant.device_assemblies['test_node.test_switch_2'].upstream_pipe.sink.queue.get())
        plant.device_assemblies['test_node.test_switch_2'].upstream_pipe.sink.queue.task_done()
        self.assertEqual(
            plant.device_assemblies['test_node.test_switch_1'].device.state.value,
            OnOffState.OFF,
            "Expected switch 1 to be OFF"
        )
        self.assertEqual(
            plant.device_assemblies['test_node.test_switch_2'].device.state.value,
            OnOffState.ON,
            "Expected switch 1 to be ON"
        )

    @asynctest.strict()
    async def test_switch_downstream_wiring_delivers(self):
        """Test that a basic switch state changes get delivered to the MQTT end of the stream"""
        blueprint = {
            'test_node': {
                'type':      'node',
                'device_id': 'test/ABC123',
                'devices': {
                    'test_switch': {
                        'type':      'basic_switch',
                        'device_id': 'switch_1',
                    },
                }
            }
        }
        plant = PlantBuilder().build(blueprint, self.loop)
        messages = [{'state': 'ON'}, {'state': 'OFF'}]
        out_messages = []
        source = plant.device_assemblies['test_node.test_switch'].downstream_pipe.source
        in_queue = source.queue
        for message in messages:
            await in_queue.put(message)
        source.start()
        await source.queue.join()
        source.stop()
        out_queue = plant.downstream_dest_queue
        out_messages.append(out_queue.sync_q.get())
        out_messages.append(out_queue.sync_q.get())
        self.assertEqual(
            out_messages[0],
            {'topic': 'hausnet/test/ABC123/downstream', 'message': '{"switch_1":{"state":"ON"}}'},
            "Expected an 'ON' JSON message on topic 'hausnet/test/ABC123/downstream'"
        )
        self.assertEqual(
            out_messages[1],
            {'topic': 'hausnet/test/ABC123/downstream', 'message': '{"switch_1":{"state":"OFF"}}'},
            "Expected an 'OFF' JSON message on topic 'hausnet/test/ABC123/downstream'"
        )
