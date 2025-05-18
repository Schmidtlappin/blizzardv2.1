"""
Test configuration for the Blizzard 2.1 test suite.
"""

import os
import sys
import pytest
from pathlib import Path

# Add the parent directory to the Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Constants for testing
TEST_DIR = Path(__file__).resolve().parent
FIXTURES_DIR = TEST_DIR / "fixtures"
TEST_XML_DIR = FIXTURES_DIR / "xml"
TEST_CONFIG_FILE = FIXTURES_DIR / "test_config.json"

# Set up test environment
def pytest_configure(config):
    """Set up test environment."""
    # Make sure fixture directories exist
    os.makedirs(FIXTURES_DIR, exist_ok=True)
    os.makedirs(TEST_XML_DIR, exist_ok=True)
    
    # Create test configuration if it doesn't exist
    if not TEST_CONFIG_FILE.exists():
        import json
        with open(TEST_CONFIG_FILE, 'w') as f:
            json.dump({
                "database": {
                    "host": "localhost",
                    "port": 5432,
                    "name": "irs990_test",
                    "user": "postgres",
                    "password": "postgres"
                },
                "processing": {
                    "batch_size": 10,
                    "workers": 2
                }
            }, f, indent=2)
