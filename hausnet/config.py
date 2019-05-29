import logging
import os


class DefaultConfig:
    """ Default configuration applies when derived classes do not override a value. Used for development."""
    MQTT_BROKER = 'localhost'
    MQTT_PORT = 1883

    def __init__(self):
        logging.basicConfig(format='%(asctime)s %(levelname)s: %(message)s [%(funcName)s]', level=logging.DEBUG)


class DevConfig(DefaultConfig):
    """ Basically an interface to DefaultConfig, named to reflect the environment."""

    # The configuration for the HausNet plant
    HAUSNET_CONFIG = {
        'sonoff_switch_node': {
            'type':      'node',
            'device_id': 'hausnode/17A317',
            'devices':   {
                'sonoff_switch': {
                    'type':      'basic_switch',
                    'device_id': 'sonoff_switch'
                }
            }
        }
    }



class TestConfig(DefaultConfig):
    """ Config for test environment.
    """
    pass


class ProdConfig(DefaultConfig):
    """ Config for production environment
    """
    pass


env = os.environ.get("LOGLEVEL", "DEBUG")
conf = TestConfig() if env == 'test' else ProdConfig() if env == 'production' else DevConfig()

