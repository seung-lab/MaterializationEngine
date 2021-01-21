class MaterializationEngineException(Exception):
    """ generic error in materialization engine """
    pass


class AlignedVolumeNotFoundException(MaterializationEngineException):
    """ error raised when a aligned_volume is not found """
    pass

class DataStackNotFoundException(MaterializationEngineException):
    """ error raised when a datastack is not found """
    pass

class MaterializeAnnotationException(Exception):
    pass


class RootIDNotFoundException(MaterializeAnnotationException):
    pass


class AnnotationParseFailure(MaterializeAnnotationException):
    pass


class TaskFailure(Exception):
   pass


class WrongModelType(KeyError):
    pass

class IndexMatchError(KeyError):
    pass