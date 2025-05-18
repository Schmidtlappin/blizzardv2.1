"""
Logging setup and configuration module for the Blizzard system.

This module provides a centralized way to set up logging across the application.
"""

import os
import logging
import logging.handlers
from datetime import datetime
from pathlib import Path

from src.core.constants import LOG_DIR

def setup_logging(name, level=logging.INFO):
    """
    Set up a logger with console and file handlers.
    
    Args:
        name: The name of the logger (usually __name__)
        level: The logging level (default: INFO)
        
    Returns:
        A configured logger instance
    """
    # Create the logger
    logger = logging.getLogger(name)
    logger.setLevel(level)
    
    # Remove existing handlers to avoid duplicates
    if logger.handlers:
        for handler in logger.handlers:
            logger.removeHandler(handler)
    
    # Create and configure console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(level)
    console_format = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    console_handler.setFormatter(console_format)
    logger.addHandler(console_handler)
    
    # Create logs directory if it doesn't exist
    log_dir = Path(LOG_DIR)
    log_dir.mkdir(exist_ok=True)
    
    # Generate timestamped log filename
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_filename = f"{name.replace('.','-')}_{timestamp}.log"
    log_path = log_dir / log_filename
    
    # Create and configure file handler
    file_handler = logging.FileHandler(log_path)
    file_handler.setLevel(level)
    file_format = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(file_format)
    logger.addHandler(file_handler)
    
    # Log setup completion
    logger.debug(f"Logger {name} initialized with log file: {log_path}")
    
    return logger