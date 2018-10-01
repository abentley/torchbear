import logging

from .event import (
    Listener,
    Queue,
    )
from .pipeline import (
    DependentTarget,
    Pipeline,
    Target,
    )
from .step import ShellStep


def run_pipeline(pipeline, target=None):
    Listener(Queue()).run_pipeline(pipeline, target)


def pipeline2():
    with Pipeline.build() as pipeline:
        with Target.build('Shelly') as s:
            ShellStep('echo foo')
            ShellStep('ls -l')
        with DependentTarget.build('Nopy', {s}):
            ShellStep('echo steve')
    return pipeline


def pipeline():
    target = Target('target-id', [lambda: print('Hello world')])
    return Pipeline.for_one_target(target)


def main():
    logging.basicConfig(level=logging.WARNING)
    logging.getLogger().setLevel(logging.INFO)
    run_pipeline(pipeline2())


if __name__ == '__main__':
    main()
