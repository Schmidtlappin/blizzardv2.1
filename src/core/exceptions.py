"""
Custom exceptions for the Blizzard system.
"""

class BlizzardError(Exception):
    """Base class for all Blizzard exceptions."""
    pass

class ConfigurationError(BlizzardError):
    """Raised when there's a configuration issue."""
    pass

class DatabaseError(BlizzardError):
    """Raised when there's a database-related error."""
    pass

class XMLProcessingError(BlizzardError):
    """Raised when there's an error processing XML."""
    pass

class RepeatingGroupError(BlizzardError):
    """Raised when there's an error with repeating groups."""
    pass

class ExtractionError(BlizzardError):
    """Raised when there's an error extracting data."""
    pass

class TransformationError(BlizzardError):
    """Raised when there's an error transforming data."""
    pass

class LoadingError(BlizzardError):
    """Raised when there's an error loading data."""
    pass

class ConcordanceError(BlizzardError):
    """Raised when there's an error with the concordance file."""
    pass

class ValidationError(BlizzardError):
    """Raised when data validation fails."""
    pass
