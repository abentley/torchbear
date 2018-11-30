from contextlib import contextmanager
from enum import Enum

from .event import (
    Event,
    ItemEvent,
    )


class Status(Enum):

    PENDING = 'pending'

    RUNNING = 'running'

    FAILED = 'failed'

    SUCCEEDED = 'succeeded'


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
        for target in self.targets:
            target.subscribe(queue)


class Target:

    pending_target = []

    @classmethod
    @contextmanager
    def build(cls, target_id, *args, **kwargs):
        """While this context is active, any new Step is auto-appended."""
        Target.pending_target.append(cls(target_id, [], *args, **kwargs))
        try:
            yield Target.pending_target[-1]
        finally:
            Target.pending_target.pop()

    def __init__(self, target_id, steps):
        self._target_id = target_id
        self._start_id = (target_id, 'start')
        self._status_id = (target_id, 'status')
        self.steps = steps
        if len(Pipeline.pending_pipeline) > 0:
            Pipeline.pending_pipeline[-1].targets.append(self)

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

    def subscribe(self, queue):
        queue.add_callback(self._start_id, self.start)

    def trigger(self, queue):
        queue.send_event(Event(self._start_id))

    def start(self, event):
        try:
            for event in self.run_steps():
                yield event
        except Exception:
            yield self.make_failed_event()
        else:
            yield self.make_succeeded_event()

    def run_steps(self):
        for index, step in enumerate(self.steps):
            yield Event((self._target_id, 'step', index, 'running'))
            func = getattr(step, 'call', step)
            func()


class DependentTarget(Target):
    """A target that depends on one or more other targets."""

    def __init__(self, target_id, steps, dependencies=None):
        super().__init__(target_id, steps)
        self.dependencies = dependencies if dependencies is not None else []
        self.seen = {}
        self.seen_items = self.seen.items()

    def __repr__(self):
        return '{}({})'.format(type(self).__name__, repr(self.target_id))

    def subscribe(self, queue):
        super().subscribe(queue)
        for dependency in self.dependencies:
            queue.add_callback(dependency.status_id, self.start)
        queue.add_callback(self.status_id, self.start)

    def trigger(self, queue):
        super().trigger(queue)
        for dependency in self.dependencies:
            dependency.trigger(queue)

    def start(self, event):
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
        for event in super().start(event):
            yield event
