import asyncio
from contextlib import closing
import logging
from runpy import run_path

from .event import (
    EventRouter,
    EventQueue,
    )
from .pipeline import (
    DependentTarget,
    Pipeline,
    Target,
    )
from .step import ShellStep


def run_pipeline(loop, pipeline, target=None):
    with closing(loop):
        loop.run_until_complete(
            EventRouter(EventQueue(loop)).run_pipeline(pipeline, target))


def load_pipeline(loop):
    with Pipeline.build() as pipeline:
        run_path('./torchbear.tb', init_globals={
            'Target': Target,
            'ShellStep': ShellStep,
            'loop': loop,
            })
    return pipeline


def pipeline2(loop):
    with Pipeline.build() as pipeline:
        with Target.build(loop, 'Shelly') as s:
            ShellStep('echo foo')
            ShellStep('ls -l')
        with DependentTarget.build(loop, 'Nopy', {s}):
            ShellStep('echo steve')
    return pipeline


def pipeline():
    target = Target('target-id', [lambda: print('Hello world')])
    return Pipeline.for_one_target(target)


def main():
    logging.basicConfig(level=logging.WARNING)
    logging.getLogger().setLevel(logging.INFO)
    loop = asyncio.get_event_loop()
    run_pipeline(loop, pipeline2(loop))


if __name__ == '__main__':
    main()
