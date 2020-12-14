# hausnet-server
Server-side HausNet protocol implementation. 

# Architecture
A HausNet system consists of an integration with a management system 
(e.g. [Home Assistant](https://github.com/HausNet/hausnet-hass)), this server software, and a network of devices 
running the protocol.

# Protocol

## Module / Device Configuration
Each module, or device, can have a set of one or more configuration items. Each configuration value consists of a name, 
and an implementation-dependent value. The value may have an internal structure, which is module / device dependent. 

Node devices typically contain modules, each of which has its own configuration with multiple items. This can be 
modeled as nested dictionaries. Each device implementation takes care of managing the configuration structure.

Devices embedded in nodes typically don't have complex configuration values, just one or more key/value pairs. It is
possible to have devices with a more complex config, though. 

The point is that the config item value is up to the module or device, while modules and devices themselves fit into
a formal higher-level structure not up to them. 

An example of a structure containing configuration (in YAML) :

```
    hausnode/48A8F0:
      config:
        network:                        # Multi-valued item
          wifi_ap: "My access point"
          wifi_pw: "mypassword"
        flash_files:                   # Module that has no config items
        heartbeat:                     # Module that has one item
          period: 60
      devices:
        switch:                        # No configuration
        thermo:                        # Multiple config items 
          units: "F"
          period: 600
        
```

# Development notes
## Building & uploading PyPi package
1. First, edit ```setup.py``` and update the version number.
2. Remove the old distribution, build the distribution, and push it to PyPi:
    ```
    rm dist/*
    python setup.py sdist bdist_wheel
    twine upload dist/*
    ```
3. Check in the final changes, and tag the release with "v1.2.3"
