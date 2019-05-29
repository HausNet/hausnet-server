##
# Classes to build devices of different types, wire them up, and, provide convenient lookups to devices without
# needing to traverse device tree.
#
# TODO: Figure out if blueprint errors should be logged / should break process (it's breaking now)
#
from abc import ABC, abstractmethod
from typing import Dict, Any
import logging

from aioreactive.core import AsyncObservable

from hausnet.devices import NodeDevice, BasicSwitch, CompoundDevice, Device, SubDevice, RootDevice
from hausnet.operators.operators import HausNetOperators as Op

log = logging.getLogger(__name__)


class BuilderError(Exception):
    """Wraps errors encountered during building for convenience"""
    pass


class DeviceBundle:
    """ A class binding together everything needed to work with a device: The device itself; Its upstream data;
    Its downstream data. The up/downstream are asynchronous aioreactive streams, composed of sources, destinations,
    and intervening operations.
    """
    def __init__(self, device: (Device, CompoundDevice), up_stream: AsyncObservable):
        self.device: (Device, CompoundDevice) = device
        self.up_stream: AsyncObservable = up_stream


class DeviceBuilder(ABC):
    """Builds a specific device from configuration. Each concrete device type should have a corresponding builder."""

    def __init__(self, source: AsyncObservable):
        """Constructor capturing the source of upstream data flows"""
        self.source = source

    @abstractmethod
    def from_blueprint(self, blueprint: Dict[str, Any]) -> DeviceBundle:
        """Given a structured build blueprint, build a device.

        :param blueprint: A dictionary containing the config values in the format above.
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
    def from_blueprint(self, blueprint: Dict[str, Any]) -> DeviceBundle:
        """Given a plan dictionary as above, construct the device. The upstream is constructed with the following
        operations:
            1. The main data stream is filtered for messages on the switch's parent node's upstream topic;
            2. Then, messages are decoded to a dictionary format from, e.g, JSON;
            3. The resultant dictionary is further filtered by this device's ID (to separate out from possible
               multiple devices in the message);
            4. Then, the message payload is extracted;
            5. Finally, the message state is set via a tap.
        At its end, the upstream flow presents an Observable for use by clients. This flow contains just messages
        from the specific device.

        :param blueprint: A blueprint in the form of the dictionary above.
        :returns: A device bundle with the BasicSwitch device object and the up/downstream data sources/sinks.

        TODO: Currently just handles state. Add configuration too.
        """
        device = BasicSwitch(blueprint['device_id'])
        up_stream = (
                self.source
                | Op.filter(lambda msg: msg['topic'].startswith(device.get_node().topic_prefix()))
                | Op.map(lambda msg: device.get_node().coder.decode(msg['message']))
                | Op.filter(lambda msg_dict, device_id=device.device_id: device_id in msg_dict)
                | Op.map(lambda msg_dict, device_id=device.device_id: msg_dict[device_id])
                | Op.tap(lambda dev_msg, dev=device: dev.state.set_value(dev_msg['state']))
        )
        return DeviceBundle(device, up_stream)


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
    def from_blueprint(self, blueprint: Dict[str, Any]) -> DeviceBundle:
        """Given a plan dictionary as above, construct the device. The operations on the input (MQTT) data stream are:
            1. The main data stream is filtered for messages on the node's upstream topic;
            2. Then, messages are decoded to a dictionary format from, e.g, JSON;
        At its end, the upstream flow presents an Observable for use by clients. This flow contains just messages
        from this node.

        :param blueprint: A blueprint in the form of the dictionary above.
        :returns: The device bundle for a node.

        TODO: Deal with module configuration messages
        """
        device = NodeDevice(blueprint['device_id'])
        up_stream = (
                self.source
                | Op.filter(lambda msg: msg['topic'].startswith(device.topic_prefix()))
                | Op.map(lambda msg: device.coder.decode(msg['message']))
        )
        return DeviceBundle(device, up_stream)


class DevicePlantBuilder:
    """Builds all the devices in the device tree, with a RootDevice at the root of the tree."""

    def __init__(self, upstream_source: AsyncObservable):
        """Prep the builders.

        :param upstream_source: Source for data flowing upstream, to serve as source for the device's upstream data
                                streams. Outside of testing, this will be the MQTT async subscriber to all HausNet
                                topics.
        """
        self.upstream_source = upstream_source
        self.builders = DeviceBuilderRegistry(upstream_source)

    def build(self, blueprint: Dict[str, Any]) -> Dict[str, DeviceBundle]:
        """Steps through the blueprint components and build a device, and an upstream and downstream stream for each.
        The result is a full instantiation of every device in the plant (from config), wired up in the correct
        owner / sub-device relationships (through CompoundDevice's sub_devices and SubDevice's owner_device), organized
        into a dictionary of device bundles that are accessible by the fully qualified device name. Note that the
        'root' bundle contains the RootDevice that forms the root of the whole device tree.

        TODO: Consider whether to combine all the device bundles' upstreams into the root observable

        :param blueprint:       Blueprint, of the whole plant, as a dictionary
        :return: A dictionary of device bundles.
        """
        root = DeviceBundle(RootDevice(), self.upstream_source)
        bundles = self._from_blueprints(blueprint, root.device)
        bundles['root'] = root
        return bundles

    def _from_blueprints(
            self,
            blueprints: Dict[str, Dict[str, Any]],
            owner_device: CompoundDevice,
            owner_fullname: str = ''
    ) -> Dict[str, DeviceBundle]:
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
            bundles[fullname] = builder.from_blueprint(blueprint)
            device = bundles[fullname].device
            if isinstance(device, SubDevice):
                device.owner_device = owner_device
                owner_device.add_sub_device(name, device)
            if isinstance(device, CompoundDevice):
                bundles = {**bundles, **self._from_blueprints(blueprint['devices'], device, fullname)}
        return bundles


class DeviceBuilderRegistry:
    """Maps device type handles to their builders"""

    def __init__(self, source: AsyncObservable):
        """Initialize the registry with the source needed by all builders to construct data streams. The self.registry
        variable holds all the device handle -> class mappings, and should be amended as new devices are defined.

        TODO: Mapping should be automatable (?)

        :param source: In non-test environments, the MQTT subscriber to all HausNet messages.
        """
        self.registry: Dict[str, DeviceBuilder] = {
            'node':         NodeDeviceBuilder(source),
            'basic_switch': BasicSwitchBuilder(source)
        }

    def builder_for(self, type_handle: str) -> DeviceBuilder:
        """Get the builder for a specific device type handle.

            :param type_handle: Handle to the class of the appropriate builder object.
            :raises BuilderError: When the builder for the specified type cannot be found.
        """
        if type_handle not in self.registry:
            raise BuilderError(f"Device type handle not found: {type_handle}")
        return self.registry[type_handle]
