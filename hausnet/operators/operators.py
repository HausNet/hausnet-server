from typing import Callable
from functools import partial

from aioreactive.core.typing import AsyncObservable
from aioreactive.core.operators import Operators


class HausNetOperators(Operators):
    """ Expands core operators to include HausNet custom ones.
    """
    @staticmethod
    def tap(fn: Callable) -> Callable[[AsyncObservable], AsyncObservable]:
        from hausnet.operators.tap import tap as _tap
        return partial(_tap, fn)

    @staticmethod
    def concat(other: AsyncObservable) -> Callable[[AsyncObservable], AsyncObservable]:
        """ TODO: This should go back into the aioreactive library - seems to be an oversight
        """
        from aioreactive.operators.concat import concat
        return partial(concat, other)
