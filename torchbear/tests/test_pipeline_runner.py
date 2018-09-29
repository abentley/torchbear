from io import StringIO
from unittest import TestCase

from torchbear.pipeline import (
    Pipeline,
    Target,
    )
from torchbear.pipeline_runner import run_pipeline


class TestRunPipeline(TestCase):

    def test_run_pipeline(self):
        output = StringIO()

        def write_foo():
            output.write('foo')

        target = Target('hello', [write_foo])

        run_pipeline(Pipeline.for_one_target(target))
        self.assertEqual('foo', output.getvalue())
