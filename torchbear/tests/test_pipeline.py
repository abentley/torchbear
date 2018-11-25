from contextlib import contextmanager
from io import StringIO
from unittest import TestCase

from torchbear.event import (
    Event,
    Listener,
    Queue,
    )
from torchbear.pipeline import (
    CoRoutineTarget,
    DependentTarget,
    Pipeline,
    Target,
    )


class TestPipeline(TestCase):

    def test_duplicate_id(self):
        with self.assertRaisesRegex(ValueError,
                                    'Duplicate target id "hello".'):
            Pipeline([Target('hello', []), Target('hello', [])], None)


class TestDependentTarget(TestCase):

    def test_subscribe_none(self):
        target = DependentTarget('hello', [], [])
        listener = Listener(Queue())
        target.subscribe(listener)
        self.assertEqual({target.start_id, target.failure_id,
                          target.success_id}, set(listener._callbacks))

    def test_subscribe_two(self):
        dependencies = [Target('first', []), Target('second', [])]
        target = DependentTarget('hello', [], dependencies)
        queue = Queue()
        listener = Listener(queue)
        target.subscribe(listener)
        self.assertEqual({
            dependencies[0].success_id,
            dependencies[0].failure_id,
            dependencies[1].success_id,
            dependencies[1].failure_id,
            target.failure_id,
            target.success_id,
            target.start_id,
        }, set(listener._callbacks))

    def test_start(self):
        output = StringIO()
        dependencies = [Target('first', []), Target('second', [])]

        def write_foo():
            output.write('foo')

        target = DependentTarget('hello', [write_foo], dependencies)
        events = list(target.start(Event(target.start_id)))
        self.assertEqual(events, [])
        events = list(target.start(Event(dependencies[0].success_id)))
        self.assertEqual(events, [])
        events = list(target.start(Event(dependencies[1].success_id)))
        self.assertNotEqual(events, [])

    def test_start_failure(self):
        dependencies = [Target('first', []), Target('second', [])]
        target = DependentTarget('hello', [], dependencies)
        events = list(target.start(Event(target.start_id)))
        self.assertEqual(events, [])
        (event,) = list(target.start(Event(dependencies[0].failure_id)))
        self.assertEqual(('hello', 'failure'), event.event_id)
        events = list(target.start(Event(dependencies[1].success_id)))
        self.assertEqual(events, [])

    def test_start_after_failure(self):
        # Don't even report as failed unless we've tried to start it.
        dependencies = [Target('first', []), Target('second', [])]
        target = DependentTarget('hello', [], dependencies)
        events = list(target.start(Event(dependencies[0].failure_id)))
        self.assertEqual(events, [])
        events = list(target.start(Event(dependencies[1].success_id)))
        self.assertEqual(events, [])
        (event,) = list(target.start(Event(target.start_id)))
        self.assertEqual(('hello', 'failure'), event.event_id)

    @contextmanager
    def no_run_test(self):
        output = []

        def add_output():
            output.append('hello')

        target = DependentTarget('foo', [add_output], [])
        list(target.start(Event(target.start_id)))
        self.assertEqual(['hello'], output)
        yield target
        list(target.start(Event(target.start_id)))
        self.assertEqual(['hello'], output)

    def test_no_run_after_failure(self):
        with self.no_run_test() as target:
            list(target.start(Event(target.failure_id)))

    def test_no_run_after_success(self):
        with self.no_run_test() as target:
            list(target.start(Event(target.success_id)))

    def test_run(self):
        output = StringIO()

        def write_foo():
            output.write('foo')

        def write_bar():
            output.write('bar')

        foo_target = DependentTarget('foo', [write_foo], [])
        bar_target = DependentTarget('bar', [write_bar], [foo_target])
        pipeline = Pipeline([bar_target, foo_target], bar_target)
        Listener(Queue()).run_pipeline(pipeline)
        self.assertEqual('foobar', output.getvalue())


class TestCoRoutineTarget(TestCase):

    def test_subscribe_none(self):
        target = CoRoutineTarget('hello', [], [])
        listener = Listener(Queue())
        target.subscribe(listener)
        self.assertEqual({target.start_id}, set(listener._callbacks))

    def test_subscribe_two(self):
        dependencies = [Target('first', []), Target('second', [])]
        target = CoRoutineTarget('hello', [], dependencies)
        queue = Queue()
        listener = Listener(queue)
        target.subscribe(listener)
        self.assertEqual({
            dependencies[0].success_id,
            dependencies[0].failure_id,
            dependencies[1].success_id,
            dependencies[1].failure_id,
            target.start_id,
        }, set(listener._callbacks))

    def test_start(self):
        output = StringIO()
        dependencies = [Target('first', []), Target('second', [])]

        def write_foo():
            output.write('foo')

        target = CoRoutineTarget('hello', [write_foo], dependencies)
        events = list(target.start(Event(target.start_id)))
        self.assertEqual(events, [])
        events = list(target.start(Event(dependencies[0].success_id)))
        self.assertEqual(events, [])
        events = list(target.start(Event(dependencies[1].success_id)))
        self.assertNotEqual(events, [])

    def test_start_failure(self):
        dependencies = [Target('first', []), Target('second', [])]
        target = CoRoutineTarget('hello', [], dependencies)
        events = list(target.start(Event(target.start_id)))
        self.assertEqual(events, [])
        (event,) = list(target.start(Event(dependencies[0].failure_id)))
        self.assertEqual(('hello', 'failure'), event.event_id)
        events = list(target.start(Event(dependencies[1].success_id)))
        self.assertEqual(events, [])

    def test_start_after_failure(self):
        # Don't even report as failed unless we've tried to start it.
        dependencies = [Target('first', []), Target('second', [])]
        target = CoRoutineTarget('hello', [], dependencies)
        events = list(target.start(Event(dependencies[0].failure_id)))
        self.assertEqual([e.event_id for e in events], [target.failure_id])
        events = list(target.start(Event(dependencies[1].success_id)))
        self.assertEqual(events, [])
        events = list(target.start(Event(target.start_id)))
        self.assertEqual([], events)

    def test_no_run_after_failure(self):

        def fail():
            raise Exception('I am exceptional!')

        target = CoRoutineTarget('foo', [fail], [])
        events = list(target.start(Event(target.start_id)))
        self.assertNotEqual([], events)
        events = list(target.start(Event(target.start_id)))
        self.assertEqual([], events)

    def test_no_run_after_success(self):
        output = []

        def add_output():
            output.append('hello')

        target = CoRoutineTarget('foo', [add_output], [])
        events = list(target.start(Event(target.start_id)))
        self.assertNotEqual([], events)
        events = list(target.start(Event(target.start_id)))
        self.assertEqual([], events)

    def test_run(self):
        output = StringIO()

        def write_foo():
            output.write('foo')

        def write_bar():
            output.write('bar')

        foo_target = CoRoutineTarget('foo', [write_foo], [])
        bar_target = CoRoutineTarget('bar', [write_bar], [foo_target])
        pipeline = Pipeline([bar_target, foo_target], bar_target)
        Listener(Queue()).run_pipeline(pipeline)
        self.assertEqual('foobar', output.getvalue())
