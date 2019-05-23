##
# Classes to build devices of different types, wire them up, and, provide convenient lookups to devices without
# needing to traverse device tree.
#
# TODO: Figure out if blueprint errors should be logged / should break process (it's breaking now)
#
from abc import ABC, abstractmethod
from typing import Dict, Any, Tuple
import logging

from aioreactive.core import AsyncObservable

from hausnet.devices import NodeDevice, BasicSwitch, CompoundDevice, Device
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
    def __init__(self, device: Device, up_stream: AsyncObservable):
        self.device: Device = device
        self.up_stream: AsyncObservable = up_stream


class DeviceBuilder(ABC):
    """Builds a specific device from configuration. Each concrete device type should have a corresponding builder."""

    @abstractmethod
    def from_blueprint(self, blueprint: Dict[str, Any], source: AsyncObservable, parent: CompoundDevice = None)\
            -> DeviceBundle:
        """Given a structured build blueprint, build a device.

        :param blueprint: A dictionary containing the config values in the format above.
        :param source:    An observable for the data streams coming from real devices.
        :param parent:    The parent device, if any
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
    def from_blueprint(self, blueprint: Dict[str, Any], source: AsyncObservable, parent: CompoundDevice = None) \
            -> DeviceBundle:
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
        :param source:    An observable for the data streams coming from real devices.
        :param parent:    The parent device for the switch.
        :returns: A device bundle with the BasicSwitch device object and the up/downstream data sources/sinks.

        TODO: Currently just handles state. Add configuration too.
        """
        device = BasicSwitch(blueprint['device_id'], parent)
        up_stream = (
                source
                | Op.filter(lambda msg: msg['topic'].startswith(parent.topic_prefix()))
                | Op.map(lambda msg: parent.coder.decode(msg['message']))
                | Op.filter(lambda msg_dict, id=device.device_id: id in msg_dict)
                | Op.map(lambda msg_dict, id=device.device_id: msg_dict[id])
                | Op.tap(lambda dev_msg, dev=device: dev.state.set_value(dev_msg['state']))
        )
        return DeviceBundle(device, up_stream)


class NodeDeviceBuilder(DeviceBuilder, CompoundDeviceBuilder):
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
    def from_blueprint(self, blueprint: Dict[str, Any], source: AsyncObservable, parent: CompoundDevice = None) \
            -> DeviceBundle:
        """Given a plan dictionary as above, construct the device. The operations on the input (MQTT) data stream are:
            1. The main data stream is filtered for messages on the node's upstream topic;
            2. Then, messages are decoded to a dictionary format from, e.g, JSON;
        At its end, the upstream flow presents an Observable for use by clients. This flow contains just messages
        from this node.

        :param blueprint: A blueprint in the form of the dictionary above.
        :param source:    An observable for the data streams coming from real devices.
        :param parent:    Unused for nodes.
        :returns: The device bundle for a node.

        TODO: Deal with module configuration messages
        """
        device = NodeDevice(blueprint['device_id'])
        up_stream = (
                source
                | Op.filter(lambda msg: msg['topic'].startswith(device.topic_prefix()))
                | Op.map(lambda msg: device.coder.decode(msg['message']))
        )
        return DeviceBundle(device, up_stream)


class RootBuilder:
    """Builds the devices at the root of the device tree. Compound device builders will take care of building
    their own sub-devices.
    """
    class RootDevice(CompoundDevice):
        """A convenience device to act as parent for the top-level devices"""
        p
    @classmethod
    def build(cls, blueprint: Dict[str, Any], upstream_source: AsyncObservable) \
            -> Tuple[Dict[str, NodeDevice], Dict[str, DeviceBundle]]:
        """Steps through the blueprint components and build a device, and an upstream and downstream stream for each.
        :param blueprint:       Blueprint as a dictionary
        :param upstream_source: Source for data flowing upstream
        :return: A tuple of: A tree structure of all devices, mirroring the blueprint structure; A flat representation
                 of device bundles, one bundle per device. Both are dictionaries indexed by the extended device name
                 - the parent device name combined with the child device name separated by dots. E.g. "bathroom.lights",
                 "bathroom.fan", "bathroom" (for the node itself), etc.
        """
        device_tree: Dict[str, NodeDevice] = {}
        device_bundles: Dict[str, DeviceBundle] = {}
        for key, device_blueprint in blueprint.items():
            builder = DeviceBuilderRegistry.builder_for(device_blueprint['type'])
            device, new_bundles = builder.from_blueprint(device_blueprint)
            device_tree[key] = device
            device_bundles = {**device_bundles, **new_bundles}
        return device_tree, device_bundles

    @classmethod
    def from_blueprints(cls, blueprints: Dict[str, Dict[str, Any]], parent_device: CompoundDevice = None)\
            -> Tuple[Dict[str, Device], Dict[str, DeviceBundle]]:
        """Given a dictionary of blueprints, construct all the devices. If devices contain sub-devices, construct
        those too, through recursion.

        :param blueprints:    The collection of blueprints for devices to build.
        :param parent_device: If the blueprints are for sub-devices, the parent they belong to.
        :return The device tree constructed
        """
        for name, blueprint in blueprints.items():
            builder = DeviceBuilderRegistry.builder_for(blueprint['type'])
            bundle = builder.from_blueprint(blueprint)
            bundle.device.
            if (isinstance(device))
            compound_device.add_sub_device(name, builder.from_blueprint(blueprint))


class DeviceBuilderRegistry:
    """Maps device type handles to their builders"""

    # The device type handle -> builder mapping
    _registry: Dict[str, DeviceBuilder] = {
        'node':         NodeDeviceBuilder(),
        'basic_switch': BasicSwitchBuilder()
    }

    @classmethod
    def builder_for(cls, type_handle: str) -> DeviceBuilder:
        """Get the builder for a specific device type handle.

            :param type_handle: Handle to the class of the appropriate builder object.
            :raises BuilderError: When the builder for the specified type cannot be found.
        """
        if type_handle not in cls._registry:
            raise BuilderError(f"Device type handle not found: {type_handle}")
        return cls._registry[type_handle]
