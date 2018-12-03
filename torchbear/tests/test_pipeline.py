import asyncio
from contextlib import contextmanager
from io import StringIO
from unittest import TestCase

from torchbear.event import (
    Event,
    EventQueue,
    EventRouter,
    )
from torchbear.pipeline import (
    DependentTarget,
    Pipeline,
    Target,
    Status,
    )
from torchbear.pipeline_runner import run_pipeline


class TestPipeline(TestCase):

    def test_duplicate_id(self):
        loop = asyncio.get_event_loop()
        with self.assertRaisesRegex(ValueError,
                                    'Duplicate target id "hello".'):
            Pipeline([Target(loop, 'hello', []), Target(loop, 'hello', [])], None)


def drain_events(loop, iterable):
    async def listify(iterable):
        values = []
        async for value in iterable:
            values.append(value)
        return values
    return loop.run_until_complete(listify(iterable))


def cancel_coro(loop, coro):
    task = loop.create_task(coro)
    for task in asyncio.Task.all_tasks():
        task.cancel()
    loop.stop()
    loop.run_forever()


class TestDependentTarget(TestCase):

    def test_subscribe_none(self):
        loop = asyncio.new_event_loop()
        target = DependentTarget(loop, 'hello', [], [])
        router = EventRouter(EventQueue(loop))
        coro = target.subscribe(router)
        self.assertEqual({
            target.start_id, target.status_id,
            }, set(router._subscription))
        cancel_coro(loop, coro)

    def test_subscribe_two(self):
        loop = asyncio.new_event_loop()
        dependencies = [Target(loop, 'first', []), Target(loop, 'second', [])]
        target = DependentTarget(loop, 'hello', [], dependencies)
        queue = EventQueue(loop)
        listener = EventRouter(queue)
        coro = target.subscribe(listener)
        expected = {
            dependencies[0].status_id,
            dependencies[1].status_id,
            target.start_id,
            target.status_id,
        }
        actual = set(listener._subscription)
        self.assertEqual(expected, actual)
        cancel_coro(loop, coro)

    def test_start(self):
        output = StringIO()
        loop = asyncio.new_event_loop()
        dependencies = [Target(loop, 'first', []), Target(loop, 'second', [])]

        def write_foo():
            output.write('foo')

        target = DependentTarget(loop, 'hello', [write_foo], dependencies)
        events = drain_events(loop, target.start(Event(target.start_id)))
        self.assertEqual(events, [])
        events = drain_events(loop, target.start(dependencies[0].make_succeeded_event()))
        self.assertEqual(events, [])
        events = drain_events(loop, target.start(dependencies[1].make_succeeded_event()))
        self.assertNotEqual(events, [])

    def test_start_failure(self):
        loop = asyncio.new_event_loop()
        dependencies = [Target(loop, 'first', []), Target(loop, 'second', [])]
        target = DependentTarget(loop, 'hello', [], dependencies)

        async def listify(iterable):
            values = []
            async for value in iterable:
                values.append(value)
            return values

        events = drain_events(loop, target.start(Event(target.start_id)))
        self.assertEqual(events, [])
        (event,) = drain_events(loop, target.start(dependencies[0].make_failed_event()))
        self.assertEqual((('hello', 'status'), Status.FAILED), event.item)
        events = drain_events(loop, target.start(dependencies[1].make_succeeded_event()))
        self.assertEqual(events, [])

    def test_start_after_failure(self):
        # Don't even report as failed unless we've tried to start it.
        loop = asyncio.new_event_loop()
        dependencies = [Target(loop, 'first', []), Target(loop, 'second', [])]
        target = DependentTarget(loop, 'hello', [], dependencies)
        events = drain_events(loop, target.start(dependencies[0].make_failed_event()))
        self.assertEqual(events, [])
        events = drain_events(loop, target.start(dependencies[1].make_succeeded_event()))
        self.assertEqual(events, [])
        (event,) = drain_events(loop, target.start(Event(target.start_id)))
        self.assertEqual((('hello', 'status'), Status.FAILED), event.item)

    @contextmanager
    def no_run_test(self):
        output = []

        def add_output():
            output.append('hello')

        loop = asyncio.new_event_loop()
        target = DependentTarget(loop, 'foo', [add_output], [])
        drain_events(loop, target.start(Event(target.start_id)))
        self.assertEqual(['hello'], output)
        yield target, loop
        drain_events(loop, target.start(Event(target.start_id)))
        self.assertEqual(['hello'], output)

    def test_no_run_after_failure(self):
        with self.no_run_test() as (target, loop):
            drain_events(loop, target.start(target.make_failed_event()))

    def test_no_run_after_success(self):
        with self.no_run_test() as (target, loop):
            drain_events(loop, target.start(target.make_succeeded_event()))

    def test_run(self):
        output = StringIO()

        def write_foo():
            output.write('foo')

        def write_bar():
            output.write('bar')

        loop = asyncio.new_event_loop()
        foo_target = DependentTarget(loop, 'foo', [write_foo], [])
        bar_target = DependentTarget(loop, 'bar', [write_bar], [foo_target])
        pipeline = Pipeline([bar_target, foo_target], bar_target)
        run_pipeline(loop, pipeline)
        self.assertEqual('foobar', output.getvalue())
