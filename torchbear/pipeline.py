from contextlib import contextmanager

from .event import (
    Event,
    EventQueue,
    ItemEvent,
    Status,
    )


class Pipeline:

    pending_pipeline = []

    @classmethod
    @contextmanager
    def build(cls, *args, **kwargs):
        """While this context is active, any new Step is auto-appended.

        This applies only to Step subclasses, of course.  Other callables must
        be done manually.
        """
        pipeline = cls([], None, *args, **kwargs)
        Pipeline.pending_pipeline.append(pipeline)
        try:
            yield pipeline
        finally:
            if pipeline.default_target is None and len(pipeline.targets) > 0:
                pipeline.default_target = pipeline.targets[-1]

    def __init__(self, targets, default_target):
        seen_ids = set()
        for target in targets:
            if target.target_id in seen_ids:
                raise ValueError(
                    'Duplicate target id "{}".'.format(target.target_id))
            seen_ids.add(target.target_id)
        self.targets = targets
        self.default_target = default_target

    @classmethod
    def for_one_target(cls, target):
        return cls([target], target)

    def subscribe(self, queue):
        handlers = []
        for target in self.targets:
            handlers.append(target.subscribe(queue))
        return handlers


class Target:

    pending_target = []

    @classmethod
    @contextmanager
    def build(cls, loop, target_id, *args, **kwargs):
        """While this context is active, any new Step is auto-appended."""
        Target.pending_target.append(cls(loop, target_id, [], *args, **kwargs))
        try:
            yield Target.pending_target[-1]
        finally:
            Target.pending_target.pop()

    def __init__(self, loop, target_id, steps):
        self._target_id = target_id
        self._start_id = (target_id, 'start')
        self._status_id = (target_id, 'status')
        self.steps = steps
        if len(Pipeline.pending_pipeline) > 0:
            Pipeline.pending_pipeline[-1].targets.append(self)
        self.event_queue = EventQueue(loop)

    @property
    def target_id(self):
        return self._target_id

    @property
    def status_id(self):
        return self._status_id

    @property
    def start_id(self):
        return self._start_id

    @property
    def start_item(self):
        return (self._start_id, None)

    @property
    def succeeded_item(self):
        return (self._status_id, Status.SUCCEEDED)

    def make_succeeded_event(self):
        return ItemEvent(*self.succeeded_item)

    @property
    def failed_item(self):
        return (self._status_id, Status.FAILED)

    def make_failed_event(self):
        return ItemEvent(*self.failed_item)

    def subscribe(self, event_router):
        event_router.add_subscription(self._start_id, self.event_queue)
        return self.handle_events(event_router.event_queue)

    def trigger(self, event_queue):
        event_queue.send_events([Event(self._start_id)])

    async def start(self, event):
        try:
            for event in self.run_steps():
                yield event
        except Exception:
            yield self.make_failed_event()
        else:
            yield self.make_succeeded_event()

    async def handle_events(self, queue):
        async for in_event in self.event_queue.iter_events():
            async for out_event in self.start(in_event):
                if not queue.done:
                    queue.send_events([out_event])

    def run_steps(self):
        for index, step in enumerate(self.steps):
            yield Event((self._target_id, 'step', index, 'running'))
            func = getattr(step, 'call', step)
            func()


class DependentTarget(Target):
    """A target that depends on one or more other targets."""

    def __init__(self, loop, target_id, steps, dependencies=None):
        super().__init__(loop, target_id, steps)
        self.dependencies = dependencies if dependencies is not None else []
        self.seen = {}
        self.seen_items = self.seen.items()

    def __repr__(self):
        return '{}({})'.format(type(self).__name__, repr(self.target_id))

    def subscribe(self, event_router):
        handler = super().subscribe(event_router)
        for dependency in self.dependencies:
            event_router.add_subscription(dependency.status_id,
                                          self.event_queue)
        event_router.add_subscription(self.status_id, self.event_queue)
        return handler

    def trigger(self, event_queue):
        super().trigger(event_queue)
        for dependency in self.dependencies:
            dependency.trigger(event_queue)

    async def start(self, event):
        """Start once all dependencies are satisfied.

        Also, don't start at all if self.start_id hasn't been seen.
        """
        self.seen.update([event.item])
        if self.failed_item in self.seen_items:
            return
        if self.succeeded_item in self.seen_items:
            return
        if self.start_item not in self.seen_items:
            return
        for dependency in self.dependencies:
            if dependency.failed_item in self.seen_items:
                new_event = self.make_failed_event()
                self.seen.update([new_event.item])
                yield new_event
        for dependency in self.dependencies:
            if dependency.succeeded_item not in self.seen_items:
                return
        async for event in super().start(event):
            yield event
