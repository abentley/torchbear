from io import StringIO
from unittest import TestCase

from torchbear.event import (
    Event,
    Listener,
    Queue,
    )
from torchbear.pipeline import (
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
        self.assertEqual({target.start_id, target.failure_id},
                         set(listener._callbacks))

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
