import asyncio
from collections import deque
from contextlib import closing
from enum import Enum
import logging


class Status(Enum):

    PENDING = 'pending'

    RUNNING = 'running'

    FAILED = 'failed'

    SUCCEEDED = 'succeeded'


COMPLETED_STATUS = frozenset([Status.SUCCEEDED, Status.FAILED])


class Event:
    """Represent an event."""

    def __init__(self, event_id):
        self.event_id = event_id

    @property
    def item(self):
        return (self.event_id, None)

    def __repr__(self):
        return '{}({})'.format(self.__class__.__name__, repr(self.event_id))


class ItemEvent(Event):
    """Represent an event."""

    def __init__(self, event_id, value):
        super().__init__(event_id)
        self.value = value
        self.event_id = event_id

    @property
    def item(self):
        return (self.event_id, self.value)

    def __repr__(self):
        return '{}({}={})'.format(
            self.__class__.__name__, repr(self.event_id), repr(self.value))


class EventQueue:
    """Support event-driven programming by iterating through events.

    An event may be any arbitrary type.  They are iterated through in FIFO
    order.  Each event is iterated through only once, so one EventQueue per
    client is recommended.
    """
    def __init__(self, loop):
        self.events = deque()
        self.ready = asyncio.Event(loop=loop)
        self.done = False

    def send_events(self, event):
        """Put multiple events into the queue.

        It is valid to send events into this queue while iterating through
        events in this queue.
        """
        if self.done:
            raise Exception('Queue is closed!')
        self.events.extend(event)
        if len(self.events) != 0:
            self.ready.set()

    async def iter_events(self):
        """Asynchronosly iterate through events.

        Each event is popped from the queue on iteration.
        """
        while not self.done:
            await self.ready.wait()
            while len(self.events) != 0:
                x = self.events.popleft()
                yield x
            self.ready.clear()

    def close(self):
        self.done = True
        self.ready.set()


class EventRouter:

    def __init__(self, event_queue):
        self.event_queue = event_queue
        self._subscription = {}

    def add_subscription(self, event_id, queue):
        self._subscription.setdefault(event_id, []).append(queue)

    async def route_events(self, target):
        with closing(self):
            async for event in self.event_queue.iter_events():
                logging.debug(f'Event: {event!r}')
                if event.event_id == target.status_id:
                    if event.value in COMPLETED_STATUS:
                        break
                for event_queue in self._subscription.get(event.event_id, []):
                    event_queue.send_events([event])

    def close(self):
        for event_queues in self._subscription.values():
            for event_queue in event_queues:
                event_queue.close()
        self.event_queue.close()

    async def run_pipeline(self, pipeline, target=None):
        handlers = pipeline.subscribe(self)
        if target is None:
            target = pipeline.default_target
        target.trigger(self.event_queue)
        await asyncio.gather(self.route_events(target), *handlers)
