"""Repeating Groups module for Blizzard 2.1.

This module provides detection and processing of repeating groups in IRS 990 XML files.
"""

from .detector import RepeatGroupDetector
from .nested_detector import NestedRepeatGroupDetector
from .processor import RepeatGroupProcessor
from .utils import extract_groups, normalize_group_data

__all__ = ['RepeatGroupDetector', 'NestedRepeatGroupDetector', 
           'RepeatGroupProcessor', 'extract_groups', 'normalize_group_data']