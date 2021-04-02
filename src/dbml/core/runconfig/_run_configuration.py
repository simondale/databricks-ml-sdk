from dbml.core import Environment


class RunConfiguration:
    @property
    def environment(self) -> Environment:
        return self._environment

    @environment.setter
    def environment(self, environment: Environment):
        self._environment = environment