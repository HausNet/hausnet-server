##
# Classes to build devices of different types, wire them up, and, provide convenient lookups to devices without
# needing to traverse device tree.
#
# TODO: Figure out if blueprint errors should be logged / should break process (it's breaking now)
#
import asyncio
from abc import ABC, abstractmethod
from typing import Dict, Any
import logging

from aioreactive.core import AsyncObservable, AsyncStream, AsyncObserver, subscribe, AsyncAnonymousObserver

from hausnet.devices import NodeDevice, BasicSwitch, CompoundDevice, Device, SubDevice, RootDevice
from hausnet.operators.operators import HausNetOperators as Op
from hausnet.flow import TOPIC_DOWNSTREAM_APPENDIX, FromBufferAsyncStream, MqttClient, DeviceInterfaceStreams

log = logging.getLogger(__name__)


class BuilderError(Exception):
    """Wraps errors encountered during building for convenience"""
    pass


class DeviceInterface:
    """ A class binding together everything needed to work with a device: The device itself; Its upstream stream and
    upstream terminal queue; its downstream data stream and terminal queue. The up/down-streams are asynchronous
    aioreactive streams, composed of sources, and intervening operations.
    """

    # The event loop, stays the same for everything
    loop = None

    def __init__(
            self,
            device: (Device, CompoundDevice),
            up_stream: AsyncObservable,
            down_stream: AsyncStream
    ):
        """Set up the components

        :param device:      The device object, capturing the static structure of the device and its owner / sub-devices
        :param loop:        The async loop to run async operations on
        :param up_stream:   An aioreactive Observable composed of a data source and chained operations
        :param down_stream: An aioreactive Observable composed of chained operations, with an AsyncStream as its source
        """
        self.device: (Device, CompoundDevice) = device
        stream: DeviceInterfaceStreams = DeviceInterfaceStreams(self.loop, up_stream, down_stream)


class DeviceBuilder(ABC):
    """Builds a specific device from configuration. Each concrete device type should have a corresponding builder.
    TODO: Consider moving the sources and sinks out of the low-level building, since they are the same for all devices.
    """

    def __init__(self, upstream_source: AsyncObservable, downstream_sink: AsyncStream):
        """Constructor capturing the source of upstream data flows"""
        self.upstream_source = upstream_source
        self.downstream_sink = downstream_sink

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
        device.owner_device = owner
        up_stream = (
            self.upstream_source
            | Op.filter(lambda msg: msg['topic'].startswith(device.get_node().topic_prefix()))
            | Op.map(lambda msg: device.get_node().coder.decode(msg['message']))
            | Op.filter(lambda msg_dict, device_id=device.device_id: device_id in msg_dict)
            | Op.map(lambda msg_dict, device_id=device.device_id: msg_dict[device_id])
            | Op.tap(lambda dev_msg, dev=device: dev.state.set_value(dev_msg['state']))
        )
        downstream_topic = f'{device.get_node().topic_prefix()}{TOPIC_DOWNSTREAM_APPENDIX}'
        down_stream = (
            AsyncStream()
            | Op.map(lambda msg: {device.device_id: msg})
            | Op.map(lambda msg: device.get_node().coder.encode(msg))
            | Op.map(lambda msg: {'topic': downstream_topic, 'message': msg})
        )
        return DeviceInterface(device, up_stream, down_stream)


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
        """Given a plan dictionary as above, construct the device. The operations on the input (MQTT) data stream are:
            1. The main data stream is filtered for messages on the node's upstream topic;
            2. Then, messages are decoded to a dictionary format from, e.g, JSON;
        At its end, the upstream flow presents an Observable for use by clients. This flow contains just messages
        from this node.

        :param blueprint: A blueprint in the form of the dictionary above.
        :param owner:     Owning device, usually None for a NodeDevice
        :returns: The device bundle for a node.

        TODO: Deal with module configuration messages
        """
        device = NodeDevice(blueprint['device_id'])
        up_stream = (
                self.upstream_source
                | Op.filter(lambda msg: msg['topic'].startswith(device.topic_prefix()))
                | Op.map(lambda msg: device.coder.decode(msg['message']))
        )
        downstream_topic = f'{device.get_node().topic_prefix()}{TOPIC_DOWNSTREAM_APPENDIX}'
        down_stream = (
            AsyncStream()
            | Op.map(lambda msg: {device.device_id: msg})
            | Op.map(lambda msg: device.get_node().coder.encode(msg))
            | Op.map(lambda msg: {'topic': downstream_topic, 'message': msg})
        )
        return DeviceInterface(device, up_stream, down_stream)


class DevicePlantBuilder:
    """Builds all the devices in the device tree, with a RootDevice at the root of the tree."""

    def __init__(self, loop, upstream_source: AsyncObservable, downstream_sink: AsyncStream):
        """Prep the builders.

        :param upstream_source: Source for data flowing upstream, to serve as source for the device's upstream data
                                streams. Outside of testing, this will be the MQTT async subscriber to all HausNet
                                topics.
        """
        self.loop = loop
        self.upstream_source = upstream_source
        self.downstream_sink = downstream_sink
        self.builders = DeviceBuilderRegistry(upstream_source, downstream_sink)
        self.mqtt_client: MqttClient = MqttClient(self.loop)
        self.upstream_source = self.mqtt_client.streams.upstream_src
        self.downstream_sink = self.mqtt_client.streams.downstream_sink

    def build(self, blueprint: Dict[str, Any]) -> Dict[str, DeviceInterface]:
        """Steps through the blueprint components and build a device, and an upstream and downstream stream for each.
        The result is a full instantiation of every device in the plant (from config), wired up in the correct
        owner / sub-device relationships (through CompoundDevice's sub_devices and SubDevice's owner_device), organized
        into a dictionary of device bundles that are accessible by the fully qualified device name. Note that the
        'root' bundle contains the RootDevice that forms the root of the whole device tree.

        TODO: Consider whether to combine all the device bundles' upstreams into the root observable

        :param blueprint:       Blueprint, of the whole plant, as a dictionary
        :return: A dictionary of device bundles.
        """
        root = DeviceInterface(RootDevice(), self.upstream_source, self.downstream_sink)
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

    def __init__(self, upstream_source: AsyncObservable, downstream_sink: AsyncObservable):
        """Initialize the registry with the source needed by all builders to construct data streams. The self.registry
        variable holds all the device handle -> class mappings, and should be amended as new devices are defined.

        TODO: Mapping should be automatable (?)

        :param source: In non-test environments, the MQTT subscriber to all HausNet messages.
        """
        self.registry: Dict[str, DeviceBuilder] = {
            'node':         NodeDeviceBuilder(upstream_source, downstream_sink),
            'basic_switch': BasicSwitchBuilder(upstream_source, downstream_sink)
        }

    def builder_for(self, type_handle: str) -> DeviceBuilder:
        """Get the builder for a specific device type handle.

            :param type_handle: Handle to the class of the appropriate builder object.
            :raises BuilderError: When the builder for the specified type cannot be found.
        """
        if type_handle not in self.registry:
            raise BuilderError(f"Device type handle not found: {type_handle}")
        return self.registry[type_handle]
