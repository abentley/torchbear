import subprocess

from .pipeline import Target


class BaseStep:

    def __init__(self):
        if len(Target.pending_target) > 0:
            Target.pending_target[-1].steps.append(self)


class CallableStep:

    def __init__(self, func):
        self.func = func

    def call(self):
        self.func()


class ShellStep(BaseStep):

    def __init__(self, command):
        super().__init__()
        self.command = command

    def call(self):
        subprocess.run(self.command, shell=True, check=True)
