from io import StringIO
from unittest import TestCase

from torchbear.event import (
    Listener,
    Queue,
    )
from torchbear.pipeline import (
    Pipeline,
    Target,
    )


class TestQueue(TestCase):

    def test_run_pipeline(self):
        output = StringIO()

        def write_foo():
            output.write('foo')

        target = Target('hello', [write_foo])
        Listener(Queue()).run_pipeline(Pipeline.for_one_target(target), target)
        self.assertEqual('foo', output.getvalue())
