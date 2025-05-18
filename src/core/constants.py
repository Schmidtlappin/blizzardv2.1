"""
Core constants used throughout the Blizzard system.
"""

# File system paths
XML_DIR = "xml_files"
XSD_DIR = "xsd_files"
LOG_DIR = "logs"

# Database constants
DEFAULT_BATCH_SIZE = 100
MAX_CONNECTION_RETRIES = 3
CONNECTION_RETRY_DELAY = 1.0  # seconds

# XML processing
NAMESPACES = {
    'irs': 'http://www.irs.gov/efile',
    'xsi': 'http://www.w3.org/2001/XMLSchema-instance'
}
