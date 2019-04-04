from hausnet.flow import BufferedAsyncStream


class TestableBufferedAsyncStream(BufferedAsyncStream):
    """ A buffered stream for testing that will only transmit a certain number of messages before stopping
    """
    def __init__(self, max_messages: int) -> None:
        super().__init__()
        self.max_messages = max_messages

    async def send_from_queue(self):
        message_count = 0
        while True:
            message = await self.queue.get()
            await super().asend(message)
            message_count += 1
            if message_count >= self.max_messages:
                break
