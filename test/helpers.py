import unittest
import asyncio


class AsyncTest(unittest.TestCase):
    """Base class to handle async setup and teardown"""

    def setUp(self) -> None:
        """Creates an event loop to use in the tests, and re-initializes the UpStream class"""
        super().setUp()
        self.loop = asyncio.get_event_loop()

    def tearDown(self) -> None:
        """Close the event loop after cancelling all the tasks"""
        tasks = asyncio.all_tasks(self.loop)
        for task in tasks:
            task.cancel()
        self.loop.run_until_complete(asyncio.sleep(0))
        super().tearDown()
