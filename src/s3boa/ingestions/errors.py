"""
Errors definition for the ingestions module

Written by DEIMOS Space S.L. (dibb)

module s1boa
"""
class Error(Exception):
    """Base class for exceptions in this module."""
    pass

class WrongDate(Error):
    """Exception raised when the dates are not correct.

    Attributes:
        message -- explanation of the error
    """

    def __init__(self, message):
        self.message = message

class WrongSatellite(Error):
    """Exception raised when the satellite is not recognized.

    Attributes:
        message -- explanation of the error
    """

    def __init__(self, message):
        self.message = message
