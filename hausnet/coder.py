import json
import logging
from abc import ABC, abstractmethod
from typing import Dict, Any

logger = logging.getLogger(__name__)


class Decoder(ABC):
    """ Base class for all decoders
    """
    @classmethod
    @abstractmethod
    def decode(cls, encoded_value: str) -> Dict[str, Any]:
        pass


class JsonDecoder(Decoder):
    """ JSON decoder

        :except: Passes on JSON decoding exceptions - have to be handled at a higher level.
    """
    @classmethod
    def decode(cls, encoded_value: str) -> Dict[str, Any]:
        """ Turns a JSON string into a dict
        """
        try:
            decoded_value = json.loads(encoded_value)
            return decoded_value
        except (ValueError, KeyError, TypeError) as e:
            logger.error("JSON decoding failed")
            raise e

