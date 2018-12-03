import asyncio
from io import StringIO
from unittest import TestCase

from torchbear.event import (
    EventRouter,
    EventQueue,
    )
from torchbear.pipeline import (
    Pipeline,
    Target,
    )
from torchbear.pipeline_runner import run_pipeline


class TestQueue(TestCase):

    def test_run_pipeline(self):
        output = StringIO()

        def write_foo():
            output.write('foo')
        loop = asyncio.get_event_loop()
        target = Target(loop, 'hello', [write_foo])
        run_pipeline(loop, Pipeline.for_one_target(target))
        self.assertEqual('foo', output.getvalue())
