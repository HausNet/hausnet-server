from abc import ABC, abstractmethod
from typing import cast, Union

import janus

from hausnet.devices import NodeDevice, BasicSwitch, CompoundDevice, Device, SubDevice, RootDevice, Sensor
from hausnet.operators.operators import HausNetOperators as Op
from hausnet.flow import *
from hausnet.states import FloatState, State

log = logging.getLogger(__name__)


class BuilderError(Exception):
    """Wraps errors encountered during building for convenience"""
    pass


class DeviceInterface:
    """ A class binding together everything needed to work with a device: The device itself; Its upstream and
    downstream data streams.
    """
    # Convenience access to the (singleton) Janus queues that sits at the MQTT side. Mostly needed for testing.
    upstream_src_queue: janus.Queue
    downstream_dest_queue: janus.Queue

    def __init__(
            self,
            device: (Device, CompoundDevice),
            up_stream: MessageStream = None,
            down_stream: MessageStream = None
    ) -> None:
        """Set up the components

        :param device:      The device object, capturing the static structure of the device and its owner / sub-devices
        :param up_stream:   A MessageStream managing upstream data flow
        :param down_stream: A MessageStream managing downstream data flow
        """
        self.device: (Device, CompoundDevice) = device
        self.up_stream: MessageStream = up_stream
        self.down_stream: MessageStream = down_stream
        # Convenience accessors to in- and out-queues, from a client perspective
        self.in_queue: asyncio.Queue = down_stream.source.queue if down_stream else None
        self.out_queue: asyncio.Queue = up_stream.sink.queue if up_stream else None


class DeviceBuilder(ABC):
    """Builds a specific device from configuration. Each concrete device type should have a corresponding builder."""
    upstream_source: AsyncStreamFromQueue
    downstream_sink: AsyncStreamToQueue

    def __init__(self, loop):
        """Constructor capturing the source of upstream data flows

        :param loop: The event loop to run async operations on
        """
        self.loop = loop

    @abstractmethod
    def from_blueprint(self, blueprint: Dict[str, Any], owner: CompoundDevice = None) -> DeviceInterface:
        """Given a structured build blueprint, build a device.

        :param blueprint: A dictionary containing the config values in the format above.
        :param owner:     The owner of this device, if any
        :returns: A completed device bundle, with a device of the type the builder builds.
        """
        pass


class BasicSwitchBuilder(DeviceBuilder):
    """Builds a basic switch from a blueprint dictionary. Configuration structure:
            {
              'type':      'basic_switch',
              'device_id': 'switch',
            }
        The device_id of the basic switch is the device_id of the firmware device in the node that contains it.
    """
    def from_blueprint(self, blueprint: Dict[str, Any], owner: CompoundDevice = None) -> DeviceInterface:
        """Given a plan dictionary as above, construct the device.

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

        :param blueprint: A blueprint in the form of the dictionary above.
        :param owner:     The owner (usually the node) for this device
        :returns: A device bundle with the BasicSwitch device object and the up/downstream data sources/sinks.

        TODO: Currently just handles state. Add configuration too.
        """
        device = BasicSwitch(blueprint['device_id'])
        # noinspection DuplicatedCode
        device.owner_device = owner
        upstream_ops = (
            self.upstream_source
            | Op.filter(lambda msg, dev=device: msg['topic'].startswith(dev.get_node().topic_prefix()))
            | Op.map(lambda msg, dev=device: dev.get_node().coder.decode(msg['message']))
            | Op.filter(lambda msg_dict, dev=device: dev.device_id in msg_dict)
            | Op.map(lambda msg_dict, dev=device: msg_dict[dev.device_id])
            | Op.tap(lambda dev_msg, dev=device: dev.state.set_value(dev_msg['state']))
        )
        up_stream = MessageStream(
            self.loop,
            self.upstream_source,
            upstream_ops,
            AsyncStreamToQueue(asyncio.Queue(loop=self.loop))
        )
        downstream_source = AsyncStreamFromQueue(self.loop, asyncio.Queue(loop=self.loop))
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
        down_stream = MessageStream(
            self.loop,
            downstream_source,
            downstream_ops,
            self.downstream_sink
        )
        return DeviceInterface(device, up_stream, down_stream)


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
    def from_blueprint(self, blueprint: Dict[str, Any], owner: CompoundDevice = None) -> DeviceInterface:
        # noinspection DuplicatedCode
        """Given a plan dictionary as above, construct the device.

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

                :param blueprint: A blueprint in the form of the dictionary above.
                :param owner:     The owner (usually the node) for this device
                :returns: A device bundle with the BasicSwitch device object and the up/downstream data sources/sinks.

                TODO: Currently just handles state. Add configuration too.
                """
        state = self.create_state(blueprint['config'])
        device = Sensor(blueprint['device_id'], state)
        # noinspection DuplicatedCode
        device.owner_device = owner
        upstream_ops = (
            self.upstream_source
            | Op.filter(lambda msg, dev=device: msg['topic'].startswith(dev.get_node().topic_prefix()))
            | Op.map(lambda msg, dev=device: dev.get_node().coder.decode(msg['message']))
            | Op.filter(lambda msg_dict, dev=device: dev.device_id in msg_dict)
            | Op.map(lambda msg_dict, dev=device: msg_dict[dev.device_id])
            | Op.tap(lambda dev_msg, dev=device: dev.state.set_value(dev_msg['state']))
        )
        up_stream = MessageStream(
            self.loop,
            self.upstream_source,
            upstream_ops,
            AsyncStreamToQueue(asyncio.Queue(loop=self.loop))
        )
        downstream_source = AsyncStreamFromQueue(self.loop, asyncio.Queue(loop=self.loop))
        downstream_ops = (
            downstream_source
            | Op.map(lambda msg, dev=device: {dev.device_id: msg})
            | Op.map(lambda msg, dev=device: dev.get_node().coder.encode(msg))
            | Op.map(lambda msg, dev=device: {
                'topic': topic_name(f'{dev.get_node().topic_prefix()}', TOPIC_DIRECTION_DOWNSTREAM),
                'message': msg
            })
        )
        down_stream = MessageStream(
            self.loop,
            downstream_source,
            downstream_ops,
            self.downstream_sink
        )
        return DeviceInterface(device, up_stream, down_stream)

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


class NodeDeviceBuilder(DeviceBuilder):
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
    def from_blueprint(self, blueprint: Dict[str, Any], owner: CompoundDevice = None) -> DeviceInterface:
        """Given a plan dictionary as above, construct the device. The operations on the input (MQTT) data
            stream are:
            1. The main data stream is filtered for messages on the node's upstream topic;
            2. Then, messages are decoded to a dictionary format from, e.g, JSON;
            3. Messages are filtered so only those addressed to the node itself are passed through.
        At its end, the upstream flow presents an Observable for use by clients. This flow contains just messages
        from this node.

        :param blueprint: A blueprint in the form of the dictionary above.
        :param owner:     Owning device, usually None for a NodeDevice
        :returns: The device bundle for a node.

        TODO: Deal with module configuration messages
        TODO: Common first part of upstream & last of downstream - worth making generic? E.g. topic name can be
              derived, it need not be specified per device.
        TODO: DRY failure? Stream ops for all devices of the same type should be the same?
        """
        device = NodeDevice(blueprint['device_id'])
        upstream_ops = (
                self.upstream_source
                | Op.filter(lambda msg, dev=device: msg['topic'].startswith(dev.topic_prefix()))
                | Op.map(lambda msg, dev=device: dev.coder.decode(msg['message']))
                | Op.filter(lambda msg_dict, dev=device: dev.device_id in msg_dict)
        )
        up_stream = MessageStream(
            self.loop,
            self.upstream_source,
            upstream_ops,
            AsyncStreamToQueue(asyncio.Queue())
        )
        downstream_source = AsyncStreamFromQueue(self.loop, asyncio.Queue(loop=self.loop))
        downstream_ops = (
            downstream_source
            | Op.map(lambda msg, dev=device: {dev.device_id: msg})
            | Op.map(lambda msg, dev=device: dev.get_node().coder.encode(msg))
            | Op.map(lambda msg, dev=device: {
                'topic': f'{dev.get_node().topic_prefix()}{TOPIC_DOWNSTREAM_APPENDIX}',
                'message': msg
              })
        )
        down_stream = MessageStream(
            self.loop,
            downstream_source,
            downstream_ops,
            self.downstream_sink
        )
        return DeviceInterface(device, up_stream, down_stream)


class DevicePlantBuilder:
    """Builds all the devices in the device tree, with a RootDevice at the root of the tree."""

    def __init__(self, loop, mqtt_server: str = conf.MQTT_BROKER, mqtt_port: int = conf.MQTT_PORT):
        """Build the whole plant recursively, employing type-specific builders as needed.

        :param loop: The async event loop to run the plant on.
        """
        self.loop = loop
        DeviceInterface.upstream_src_queue = janus.Queue(loop=loop)
        upstream_source = AsyncStreamFromQueue(loop, cast(asyncio.Queue, DeviceInterface.upstream_src_queue.async_q))
        DeviceInterface.downstream_dest_queue = janus.Queue(loop=loop)
        downstream_sink = AsyncStreamToQueue(cast(asyncio.Queue, DeviceInterface.downstream_dest_queue.async_q))
        self.builders = DeviceBuilderRegistry(loop, upstream_source, downstream_sink)
        self.mqtt_client: MqttClient = MqttClient(
            cast(queue.Queue, DeviceInterface.downstream_dest_queue.sync_q),
            cast(queue.Queue, DeviceInterface.upstream_src_queue.sync_q),
            mqtt_server,
            mqtt_port
        )

    def build(self, blueprint: Dict[str, Any]) -> Dict[str, DeviceInterface]:
        """Steps through the blueprint components and build a device, and an upstream and downstream stream
        for each. The result is a full instantiation of every device in the plant (from config), wired up in the
        correct owner / sub-device relationships (through CompoundDevice's sub_devices and SubDevice's owner_device),
        organized into a dictionary of device bundles that are accessible by the fully qualified device name. Note
        that the 'root' bundle contains the RootDevice that forms the root of the whole device tree.

        TODO: Consider whether to combine all the device bundles' upstreams into the root observable

        :param blueprint: Blueprint, of the whole plant, as a dictionary
        :return: A dictionary of device bundles.
        """
        root = DeviceInterface(RootDevice())
        bundles = self._from_blueprints(blueprint, root.device)
        bundles['root'] = root
        return bundles

    def _from_blueprints(
            self,
            blueprints: Dict[str, Dict[str, Any]],
            owner_device: CompoundDevice,
            owner_fullname: str = ''
    ) -> Dict[str, DeviceInterface]:
        """Given a dictionary of blueprints, construct all the devices. If devices contain sub-devices, construct
        those too, through recursion.

        :param blueprints:     The collection of blueprints for devices to build.
        :param owner_device:   If the blueprints are for sub-devices, the parent they belong to.
        :param owner_fullname: The fully qualified name of the owner, used as the base of the bundle names.
        :return The device bundles for the sub-tree
        """
        bundles = {}
        for name, blueprint in blueprints.items():
            builder = self.builders.builder_for(blueprint['type'])
            fullname = f'{owner_fullname}.{name}' if owner_fullname else name
            bundles[fullname] = builder.from_blueprint(blueprint, owner_device)
            device = bundles[fullname].device
            if isinstance(device, SubDevice):
                owner_device.add_sub_device(name, device)
            if isinstance(device, CompoundDevice):
                bundles = {**bundles, **self._from_blueprints(blueprint['devices'], device, fullname)}
        return bundles


class DeviceBuilderRegistry:
    """Maps device type handles to their builders"""

    def __init__(self, loop, upstream_source: AsyncStreamFromQueue, downstream_sink: AsyncStreamToQueue):
        """Initialize the registry with the source needed by all builders to construct data streams. The self.registry
        variable holds all the device handle -> class mappings, and should be amended as new devices are defined.

        :param upstream_source: The one shared source of all incoming MQTT messages.
        :param downstream_sink: The one shared sink for all outgoing MQTT messages.
        """
        DeviceBuilder.downstream_sink = downstream_sink
        DeviceBuilder.upstream_source = upstream_source
        self.registry: Dict[str, DeviceBuilder] = {
            'node':         NodeDeviceBuilder(loop),
            'basic_switch': BasicSwitchBuilder(loop),
            'sensor':       SensorBuilder(loop),
        }

    def builder_for(self, type_handle: str) -> DeviceBuilder:
        """Get the builder for a specific device type handle.

            :param type_handle: Handle to the class of the appropriate builder object.
            :raises BuilderError: When the builder for the specified type cannot be found.
        """
        if type_handle not in self.registry:
            raise BuilderError(f"Device type handle not found: {type_handle}")
        return self.registry[type_handle]
