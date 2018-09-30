import logging

from .event import (
    Queue,
    )
from .pipeline import (
    DependentTarget,
    Pipeline,
    Target,
    )
from .step import ShellStep


def run_pipeline(pipeline, target=None):
    Queue().run_pipeline(pipeline, target)


def pipeline2():
    with Pipeline.build() as pipeline:
        with Target.build('Shelly') as s:
            ShellStep('echo foo')
            ShellStep('ls -l')
        with DependentTarget.build('Nopy', [s]):
            ShellStep('echo steve')
    return pipeline


def main():
    logging.basicConfig(level=logging.WARNING)
    logging.getLogger().setLevel(logging.INFO)
    target = Target('target-id', [lambda: print('Hello world')])
    pipeline = Pipeline.for_one_target(target)
    run_pipeline(pipeline2())

if __name__ == '__main__':
    main()
