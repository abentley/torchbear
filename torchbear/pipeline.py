from contextlib import contextmanager

from .event import Event


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
            if pipeline.default_target is None:
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
        self._running_id = (target_id, 'running')
        self._failure_id = (target_id, 'failure')
        self._success_id = (target_id, 'success')
        self.steps = steps
        if len(Pipeline.pending_pipeline) > 0:
            Pipeline.pending_pipeline[-1].targets.append(self)

    @property
    def target_id(self):
        return self._target_id

    @property
    def start_id(self):
        return self._start_id

    @property
    def success_id(self):
        return self._success_id

    @property
    def failure_id(self):
        return self._failure_id

    def subscribe(self, queue):
        queue.add_callback(self._start_id, self.start)

    def trigger(self, queue):
        queue.send_event(Event(self._start_id))

    def start(self, event):
        try:
            for event in self.run_steps():
                yield event
        except Exception:
            yield Event(self._failure_id)
            raise
        else:
            yield Event(self._success_id)

    def run_steps(self):
        for index, step in enumerate(self.steps):
            yield Event((self._target_id, 'step', index, 'running'))
            func = getattr(step, 'call', step)
            func()


class DependentTarget(Target):
    """A target that depends on one or more other targets."""

    def __init__(self, target_id, steps, dependencies):
        super().__init__(target_id, steps)
        self.dependencies = dependencies
        self.seen_ids = set()

    def subscribe(self, queue):
        super().subscribe(queue)
        queue.add_callback(self.failure_id, self.start)
        for dependency in self.dependencies:
            queue.add_callback(dependency.success_id, self.start)
            queue.add_callback(dependency.failure_id, self.start)

    def trigger(self, queue):
        super().trigger(queue)
        for dependency in self.dependencies:
            dependency.trigger(queue)

    def start(self, event):
        """Start once all dependencies are satisfied.

        Also, don't start at all if self.start_id hasn't been seen.
        """
        if self.failure_id in self.seen_ids:
            return
        self.seen_ids.add(event.event_id)
        if self.start_id not in self.seen_ids:
            return
        for dependency in self.dependencies:
            if dependency.failure_id in self.seen_ids:
                new_event = Event(self.failure_id)
                self.seen_ids.add(new_event.event_id)
                yield new_event
        for dependency in self.dependencies:
            if dependency.success_id not in self.seen_ids:
                return
        yield from super().start(event)
