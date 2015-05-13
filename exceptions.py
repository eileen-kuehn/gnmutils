from utility.exceptions import *

# GNM related exceptions/errors
class NonUniqueRootException(BasicException):
    """Thrown when JobParser gets several root nodes"""
    def __init__(self):
        BasicException.__init__(self, "Received second root node!")