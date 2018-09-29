import logging

from .pipeline import (
    Pipeline,
    Target,
    )
from .event import (
    Queue,
    )


def run_pipeline(pipeline, target_name=None):
    if target_name is None:
        target = pipeline.default_target
    queue = Queue()
    pipeline.subscribe(queue)
    target.trigger(queue)
    queue.run()


def main():
    logging.basicConfig(level=logging.DEBUG)
    target = Target('target-id', [lambda: print('Hello world')])
    run_pipeline(Pipeline.for_one_target(target))


if __name__ == '__main__':
    main()
