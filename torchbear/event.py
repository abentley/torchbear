import logging


class Event:
    """Represent an event."""

    def __init__(self, event_id):
        self.event_id = event_id

    def __repr__(self):
        return '{}({})'.format(self.__class__.__name__, repr(self.event_id))


class Queue:
    """Represent an event queue."""

    def __init__(self):
        self._stack = []

    def send_event(self, event):
        logging.debug('Sending event {}'.format(repr(event)))
        self._stack.append(event)


class Listener:

    def __init__(self, queue):
        self.queue = queue
        self._callbacks = {}

    def add_callback(self, event_id, func):
        self._callbacks.setdefault(event_id, []).append(func)

    def run(self):
        """Handle events until there are none left.

        This function assumes that callbacks will keep yielding events until
        there is nothing left to process.
        """
        while(len(self.queue._stack) != 0):
            event = self.queue._stack.pop()
            for func in self._callbacks.get(event.event_id, []):
                for func_event in func(event=event):
                    self.queue.send_event(func_event)

    def run_pipeline(self, pipeline, target=None):
        pipeline.subscribe(self)
        if target is None:
            target = pipeline.default_target
        target.trigger(self.queue)
        self.run()
