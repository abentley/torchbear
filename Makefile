run:
	python3 -m torchbear.pipeline_runner

test:
	python3 -m unittest discover torchbear

lint:
	flake8 torchbear

.PHONY: run test lint
