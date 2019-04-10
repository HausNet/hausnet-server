from asyncio import iscoroutinefunction
from typing import Callable, TypeVar

from aioreactive.abc import AsyncDisposable
from aioreactive.core import AsyncObserver, AsyncObservable
from aioreactive.core import AsyncSingleStream, chain, AsyncCompositeDisposable

T = TypeVar('T')


class Tap(AsyncObservable[T]):
    """ An operator that taps values for out-of-stream purposes, but does not disturb the main stream itself.
        Intended for uses like devices recording their values as the updates flow by.
    """
    def __init__(self, tapper: Callable[[T], None], source: AsyncObservable[T]) -> None:
        self._source = source
        self._tapper = tapper

    async def __asubscribe__(self, observer: AsyncObserver) -> AsyncDisposable:
        sink = Tap.Sink(self)
        down = await chain(sink, observer)  # type: AsyncDisposable
        up = await chain(self._source, sink)   # type: AsyncDisposable

        return AsyncCompositeDisposable(up, down)

    class Sink(AsyncSingleStream):

        def __init__(self, source: "Tap") -> None:
            super().__init__()
            self._tapper = source._tapper

        async def asend_core(self, value: T) -> None:
            try:
                self._tapper(value)
            except Exception as err:
                await self._observer.athrow(err)
            else:
                await self._observer.asend(value)


def tap(tapper: Callable[[T], None], source: AsyncObservable[T]) -> AsyncObservable[T]:
    """ Provides out-of-stream visibility to each item of the source observable.

        xs = tap(lambda value: my_func(value), source)
        xs = tap(my_func, source)

        :param tapper: Function taking any value from the source
        :param source: The source observable producing values

        :return An observable sequence which is a copy of the input source
    """

    assert not iscoroutinefunction(tapper)

    return Tap(tapper, source)
