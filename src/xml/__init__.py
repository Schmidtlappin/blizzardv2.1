"""XML processing module for Blizzard 2.1.

This module provides tools for parsing, validating, and processing IRS 990 XML files.
"""

from .parser import XMLParser
from .validator import XMLValidator
from .streaming import XMLStreamReader

__all__ = ['XMLParser', 'XMLValidator', 'XMLStreamReader']