from contextlib import contextmanager
from io import StringIO
from unittest import TestCase

from torchbear.event import (
    Event,
    ItemEvent,
    Listener,
    Queue,
    )
from torchbear.pipeline import (
    DependentTarget,
    Pipeline,
    Target,
    Status,
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
        self.assertEqual({
            target.start_id, target.status_id,
            }, set(listener._callbacks))

    def test_subscribe_two(self):
        dependencies = [Target('first', []), Target('second', [])]
        target = DependentTarget('hello', [], dependencies)
        queue = Queue()
        listener = Listener(queue)
        target.subscribe(listener)
        expected = {
            dependencies[0].status_id,
            dependencies[1].status_id,
            target.start_id,
            target.status_id,
        }
        actual = set(listener._callbacks)
        self.assertEqual(expected, actual)

    def test_start(self):
        output = StringIO()
        dependencies = [Target('first', []), Target('second', [])]

        def write_foo():
            output.write('foo')

        target = DependentTarget('hello', [write_foo], dependencies)
        events = list(target.start(Event(target.start_id)))
        self.assertEqual(events, [])
        events = list(target.start(ItemEvent(*dependencies[0].success_item)))
        self.assertEqual(events, [])
        events = list(target.start(ItemEvent(*dependencies[1].success_item)))
        self.assertNotEqual(events, [])

    def test_start_failure(self):
        dependencies = [Target('first', []), Target('second', [])]
        target = DependentTarget('hello', [], dependencies)
        events = list(target.start(Event(target.start_id)))
        self.assertEqual(events, [])
        (event,) = list(target.start(ItemEvent(*dependencies[0].failure_item)))
        self.assertEqual((('hello', 'status'), Status.FAILED), event.item)
        events = list(target.start(ItemEvent(*dependencies[1].success_item)))
        self.assertEqual(events, [])

    def test_start_after_failure(self):
        # Don't even report as failed unless we've tried to start it.
        dependencies = [Target('first', []), Target('second', [])]
        target = DependentTarget('hello', [], dependencies)
        events = list(target.start(ItemEvent(*dependencies[0].failure_item)))
        self.assertEqual(events, [])
        events = list(target.start(ItemEvent(*dependencies[1].success_item)))
        self.assertEqual(events, [])
        (event,) = list(target.start(Event(target.start_id)))
        self.assertEqual((('hello', 'status'), Status.FAILED), event.item)

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
            list(target.start(ItemEvent(*target.failure_item)))

    def test_no_run_after_success(self):
        with self.no_run_test() as target:
            list(target.start(ItemEvent(*target.success_item)))

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
