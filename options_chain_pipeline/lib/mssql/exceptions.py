class InvalidColumnError(Exception):
    """
    Raised when column information is invalid.
    e.g., when precision and scale parameters
    are missing for a column of type DECIMAL
    """
