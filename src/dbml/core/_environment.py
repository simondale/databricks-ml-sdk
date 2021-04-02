class Environment:
    def __init__(self, name, requirements):
        self._name = name
        self._requirements = requirements

    @staticmethod
    def from_requirements(name, requirements):
        return Environment(name, requirements)

    def register(self, workspace):
        self._workspace = workspace

    def get_libraries(self):
        with open(self._requirements, "r") as f:
            return f.readlines()
