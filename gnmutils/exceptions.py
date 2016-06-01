from utility.exceptions import BasicException


class NonUniqueRootException(BasicException):
    """
    Thrown when Job gets several root nodes
    """
    def __init__(self):
        BasicException.__init__(self, "Received second root node!")


class ParserNotInitializedException(BasicException):
    """
    Thrown when data has not been initialized in Parser
    """
    def __init__(self):
        BasicException.__init__(self, "Data in Parser has not been initialized!")


class ProcessMismatchException(BasicException):
    """
    Thrown when trying to combine two process events that actually do not match
    """
    def __init__(self):
        BasicException.__init__(self, "Processes that should be combined do not match")


class NoGNMDirectoryStructure(BasicException):
    """
    Thrown when trying to identify folder structure from gnm workflows
    """
    def __init__(self):
        BasicException.__init__(self, "No GNM directory strucutre could be identified")


class DataNotInCacheException(BasicException):
    """
    Thrown when the data that is looked for in ObjectCache cannot be identified
    """
    def __init__(self, key=None, value=None):
        BasicException.__init__(self, "Element for key %s at %d cannot be identified" %
                                (key, value))


class ArgumentNotDefinedException(BasicException):
    """
    Thrown when a default mapping of an argument has not been defined in GNMObjects
    """
    def __init__(self, argument=None, value=None):
        BasicException.__init__(self, "The argument %s (value: %s) cannot be mapped because it is "
                                      "not specified" % (argument, value))
