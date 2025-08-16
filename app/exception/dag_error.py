from app.exception.exception import RJMException


class DagGraphError(ValueError):
    pass


class DagError(DagGraphError):
    def __init__(self, *args, **kwargs):
        super().__init__(*args)
        self.port = kwargs.get("port", None)
        self.input = kwargs.get("input", None)
        self.edge = kwargs.get("edge", None)
        self.traceback = kwargs.get("traceback", None)


class DagExecError(ValueError):
    def __init__(self, *args, **kwargs):
        super().__init__(*args)
        self.port = kwargs.get("port", None)
        self.input = kwargs.get("input", None)
        self.edge = kwargs.get("edge", None)
        self.traceback = kwargs.get("traceback", None)
