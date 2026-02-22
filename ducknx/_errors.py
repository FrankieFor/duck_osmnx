"""Define custom errors and exceptions."""


class GraphSimplificationError(ValueError):
    """Exception for a problem with graph simplification."""


class InsufficientResponseError(ValueError):
    """Exception for empty or too few results in server response."""


class ResponseStatusCodeError(ValueError):
    """Exception for an unhandled server response status code."""
