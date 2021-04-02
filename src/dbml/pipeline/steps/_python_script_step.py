class PythonScriptStep:
    def __init__(self, name, script_name, source_directory, compute_target, runconfig, **kwargs):
        self.name = name
        self.script_name = script_name
        self.source_directory = source_directory
        self.compute_target = compute_target
        self.runconfig = runconfig
