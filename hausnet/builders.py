from abc import ABC, abstractmethod
from typing import Union

from aioreactive.core.streams import AsyncMultiStream
from class_registry import ClassRegistry

from hausnet.devices import NodeDevice, BasicSwitch, CompoundDevice, Device, SubDevice, RootDevice, Sensor
from hausnet.operators.operators import HausNetOperators as Op
from hausnet.flow import *
from hausnet.plant import DeviceAssembly, DevicePlant
from hausnet.states import FloatState, State

log = logging.getLogger(__name__)

builders = ClassRegistry()


class DeviceBuilder(ABC):
    """Builds a specific device from configuration. Each concrete device type should have a corresponding builder."""

    @abstractmethod
    def from_blueprint(self, plant: DevicePlant, blueprint: Dict[str, Any], owner: CompoundDevice = None) \
            -> DeviceAssembly:
        """ Given a structured build blueprint, build a device assembly.

            :param plant:     The device plant the assembly is being added to.
            :param blueprint: A dictionary containing the config values in the format above.
            :param owner:     The owner of this device, if any.
            :returns:         A completed device assembly, with a device of the type the builder builds.
        """
        pass

    @staticmethod
    def assemble(
            plant: DevicePlant,
            device: Device,
            upstream_ops: AsyncMultiStream,
            downstream_source: AsyncStreamFromQueue,
            downstream_ops: AsyncMultiStream
    ) -> DeviceAssembly:
        """ Given a device and the plant, create an assembly to slot into the plant. Used internally to the builder
            to make common operations DRY.

            :param plant:             The device plant the assembly is being added to.
            :param device:            A device that's been constructed, but has no owner yet.
            :param downstream_source: An async stream flowing up from downstream.
            :param upstream_ops:      Observable containing the result of an operations pipeline applied to data
                                      flowing upstream.
            :param downstream_ops:    Observable containing the result of an operations pipeline applied to data
                                      flowing downstream.
            :returns:                 A completed device assembly, with a device of the type the builder builds.
        """
        up_stream = MessagePipe(
            plant.upstream_source,
            upstream_ops,
            AsyncStreamToQueue(asyncio.Queue())
        )
        down_stream = MessagePipe(
            downstream_source,
            downstream_ops,
            plant.downstream_sink
        )
        return DeviceAssembly(device, up_stream, down_stream)


@builders.register('basic_switch')
class BasicSwitchBuilder(DeviceBuilder):
    """Builds a basic switch from a blueprint dictionary. Configuration structure:
            {
              'type':      'basic_switch',
              'device_id': 'switch',
            }
        The device_id of the basic switch is the device_id of the firmware device in the node that contains it.
    """
    def from_blueprint(self, plant: DevicePlant, blueprint: Dict[str, Any], owner: CompoundDevice = None) \
            -> DeviceAssembly:
        """ Given a plan dictionary as above, construct the device.

            The upstream is constructed with the following operations:
                1. The main data stream is filtered for messages on the switch's parent node's upstream topic;
                2. Then, messages are decoded to a dictionary format from, e.g, JSON;
                3. The resultant dictionary is further filtered by this device's ID (to separate out from possible
                   multiple devices in the message);
                4. Then, the message payload is extracted;
                5. Finally, the message state is set via a tap.
            At its end, the upstream flow presents an Observable for use by clients. This flow contains just messages
            from the specific device.

            The downstream is constructed with the following operations:
                1. The input payload is put in a dictionary with the device ID as the key.
                2. The result is encoded with the device's coder.
                3. A dictionary with the topic and the encoded message is created.

            :param plant:     The device plant the assembly is being added to.
            :param blueprint: A blueprint in the form of the dictionary above.
            :param owner:     The owner (usually the node) for this device
            :returns: A device bundle with the BasicSwitch device object and the up/downstream data sources/sinks.

            TODO: Currently just handles state. Add configuration too.
        """
        device = BasicSwitch(blueprint['device_id'])
        upstream_ops = (
            plant.upstream_source
            | Op.filter(lambda msg, dev=device: msg['topic'].startswith(dev.get_node().topic_prefix()))
            | Op.map(lambda msg, dev=device: dev.get_node().coder.decode(msg['message']))
            | Op.filter(lambda msg_dict, dev=device: dev.device_id in msg_dict)
            | Op.map(lambda msg_dict, dev=device: msg_dict[dev.device_id])
            | Op.tap(lambda dev_msg, dev=device: dev.state.set_value(dev_msg['state']))
        )
        downstream_source = AsyncStreamFromQueue(asyncio.Queue())
        downstream_ops = (
            downstream_source
            | Op.tap(lambda msg, dev=device: dev.state.set_value(msg['state']))
            | Op.map(lambda msg, dev=device: {dev.device_id: msg})
            | Op.map(lambda msg, dev=device: dev.get_node().coder.encode(msg))
            | Op.map(lambda msg, dev=device: {
                'topic': topic_name(f'{dev.get_node().topic_prefix()}', TOPIC_DIRECTION_DOWNSTREAM),
                'message': msg
            })
        )
        return self.assemble(plant, device, upstream_ops, downstream_source, downstream_ops)


@builders.register('sensor')
class SensorBuilder(DeviceBuilder):
    """Builds a sensor from a blueprint dictionary. Configuration structure:
            {
              'type':      'sensor',
              'device_id': 'thermo',
              'config':    {
                'state.type':  'float',
                'state.unit': 'F',
                'state.min':   '-50',       # TBD
                'state.max':   '50',        # TBD
                'model':       'DS18B20'    # TBD
              },
            }
        The device_id of the basic switch is the device_id of the firmware device in the node that contains it.
    """
    def from_blueprint(self, plant: DevicePlant, blueprint: Dict[str, Any], owner: CompoundDevice = None) \
            -> DeviceAssembly:
        # noinspection DuplicatedCode
        """ Given a plan dictionary as above, construct the device.

            The upstream is constructed with the following operations:
                1. The main data stream is filtered for messages on the switch's parent node's upstream topic;
                2. Then, messages are decoded to a dictionary format from, e.g, JSON;
                3. The resultant dictionary is further filtered by this device's ID (to separate out from possible
                   multiple devices in the message);
                4. Then, the message payload is extracted;
                5. Finally, the message state is set via a tap.
            At its end, the upstream flow presents an Observable for use by clients. This flow contains just
            messages from the specific device.

            The downstream is constructed with the following operations:
                1. The input payload is put in a dictionary with the device ID as the key.
                2. The result is encoded with the device's coder.
                3. A dictionary with the topic and the encoded message is created.

            :param plant:     The device plant the assembly is being added to.
            :param blueprint: A blueprint in the form of the dictionary above.
            :param owner:     The owner (usually the node) for this device
            :returns:         A device bundle with the BasicSwitch device object and the up/downstream data
                              sources/sinks.

            TODO: Currently just handles state. Add configuration too.
        """
        state = self.create_state(blueprint['config'])
        device = Sensor(blueprint['device_id'], state)
        device.owner_device = owner
        upstream_ops = (
            plant.upstream_source
            | Op.filter(lambda msg, dev=device: msg['topic'].startswith(dev.get_node().topic_prefix()))
            | Op.map(lambda msg, dev=device: dev.get_node().coder.decode(msg['message']))
            | Op.filter(lambda msg_dict, dev=device: dev.device_id in msg_dict)
            | Op.map(lambda msg_dict, dev=device: msg_dict[dev.device_id])
            | Op.tap(lambda dev_msg, dev=device: dev.state.set_value(dev_msg['state']))
        )
        downstream_source = AsyncStreamFromQueue(asyncio.Queue())
        downstream_ops = (
            downstream_source
            | Op.map(lambda msg, dev=device: {dev.device_id: msg})
            | Op.map(lambda msg, dev=device: dev.get_node().coder.encode(msg))
            | Op.map(lambda msg, dev=device: {
                'topic': topic_name(f'{dev.get_node().topic_prefix()}', TOPIC_DIRECTION_DOWNSTREAM),
                'message': msg
            })
        )
        return self.assemble(plant, device, upstream_ops, downstream_source, downstream_ops)

    @staticmethod
    def create_state(config: Dict[str, Union[str, float, bool]]) -> State:
        """Create the sensor's state variable, given its configuration. Note: Work in progress """
        unit = None
        state = None
        if 'state.unit' in config:
            unit = config['state.unit']
        if config['state.type'] == 'float':
            state = FloatState(unit)
        return state


@builders.register('node')
class NodeBuilder(DeviceBuilder):
    """Builds a node device from a blueprint dictionary. Configuration structure:
            {
              'type': 'node',
              '
              'devices':
                    {
                    'device1': {...(device blueprint)...}
                    ...
                    }
            }
        The device_id of the basic switch is the device_id of the firmware device in the node that contains it.

        Building the constituent devices is left to the routine that built the node device.
    """
    def from_blueprint(self, plant: DevicePlant, blueprint: Dict[str, Any], owner: CompoundDevice = None) \
            -> DeviceAssembly:
        """ Given a plan dictionary as above, construct the device. The operations on the input (MQTT) data
            stream are:
                1. The main data stream is filtered for messages on the node's upstream topic;
                2. Then, messages are decoded to a dictionary format from, e.g, JSON;
                3. Messages are filtered so only those addressed to the node itself are passed through.
            At its end, the upstream flow presents an Observable for use by clients. This flow contains just messages
            from this node.

            :param plant:     The device plant the assembly is being added to.
            :param blueprint: A blueprint in the form of the dictionary above.
            :param owner:     Owning device, usually None for a NodeDevice
            :returns:         The device assembly for a node.

            TODO: Deal with module configuration messages
            TODO: Common first part of upstream & last of downstream - worth making generic? E.g. topic name can be
                  derived, it need not be specified per device.
            TODO: DRY failure? Stream ops for all devices of the same type should be the same?
        """
        device = NodeDevice(blueprint['device_id'])
        upstream_ops = (
                plant.upstream_source
                | Op.filter(lambda msg, dev=device: msg['topic'].startswith(dev.topic_prefix()))
                | Op.map(lambda msg, dev=device: dev.coder.decode(msg['message']))
                | Op.filter(lambda msg_dict, dev=device: dev.device_id in msg_dict)
        )
        downstream_source = AsyncStreamFromQueue(asyncio.Queue())
        downstream_ops = (
            downstream_source
            | Op.map(lambda msg, dev=device: {dev.device_id: msg})
            | Op.map(lambda msg, dev=device: dev.get_node().coder.encode(msg))
            | Op.map(lambda msg, dev=device: {
                'topic': f'{dev.get_node().topic_prefix()}{TOPIC_DOWNSTREAM_APPENDIX}',
                'message': msg
              })
        )
        return self.assemble(plant, device, upstream_ops, downstream_source, downstream_ops)


class PlantBuilder:
    """ Builds all the devices in the device tree, with a RootDevice at the root of the tree. """

    def build(
        self,
        blueprint: Dict[str, Any],
        loop: asyncio.BaseEventLoop,
        mqtt_server: str = conf.MQTT_BROKER,
        mqtt_port: int = conf.MQTT_PORT,
    ) -> DevicePlant:
        """ Steps through the blueprint components and build a device, and an upstream and downstream stream
            for each. The result is a full instantiation of every device in the plant (from config), wired up in the
            correct owner / sub-device relationships (through CompoundDevice's sub_devices and SubDevice's
            owner_device), organized into a dictionary of device bundles that are accessible by the fully qualified
            device name. Note that the 'root' bundle contains the RootDevice that forms the root of the whole device
            tree.

            :param blueprint:   Blueprint, of the whole plant, as a dictionary.
            :param loop:        The event loop to run the plant on.
            :param mqtt_server: The name of the MQTT server.
            :param mqtt_port:   The MQTT port number.
            :return:            A DevicePlant, filled with device assemblies.

            TODO: Consider whether to combine all the device bundles' upstreams into the root observable
        """
        plant = DevicePlant(loop, mqtt_server, mqtt_port)
        root = DeviceAssembly(RootDevice())
        assemblies = self.build_devices(plant, blueprint, root.device)
        plant.device_assemblies = assemblies
        plant.device_assemblies['root'] = root
        return plant

    def build_devices(
            self,
            plant: DevicePlant,
            blueprints: Dict[str, Dict[str, Any]],
            owner_device: CompoundDevice,
            owner_fullname: str = ''
    ) -> Dict[str, DeviceAssembly]:
        """ Given a dictionary of blueprints, construct all the devices. If devices contain sub-devices, construct
            those too, through recursion.

            :param plant:          The plant that is being built.
            :param blueprints:     The collection of blueprints for devices to build.
            :param owner_device:   If the blueprints are for sub-devices, the parent they belong to.
            :param owner_fullname: The fully qualified name of the owner, used as the base of the bundle names.
            :return                The device bundles for the sub-tree
        """
        assemblies = {}
        for name, blueprint in blueprints.items():
            builder = builders[blueprint['type']]
            fullname = f'{owner_fullname}.{name}' if owner_fullname else name
            assemblies[fullname] = builder.from_blueprint(plant, blueprint, owner_device)
            device = assemblies[fullname].device
            if isinstance(device, SubDevice):
                owner_device.add_sub_device(name, device)
            if isinstance(device, CompoundDevice):
                assemblies = {**assemblies, **self.build_devices(plant, blueprint['devices'], device, fullname)}
        return assemblies
