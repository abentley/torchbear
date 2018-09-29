from .event import Event


class Pipeline:

    def __init__(self, targets, default_target):
        self.targets = targets
        self.default_target = default_target

    @classmethod
    def for_one_target(cls, target):
        return cls([target], target)

    def subscribe(self, queue):
        for target in self.targets:
            target.subscribe(queue)


class Target:

    def __init__(self, target_id, steps):
        self._target_id = target_id
        self._start_id = (target_id, 'start')
        self._running_id = (target_id, 'running')
        self._failure_id = (target_id, 'failure')
        self._success_id = (target_id, 'success')
        self.steps = steps

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
