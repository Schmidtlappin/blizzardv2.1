# Configuration Options

This document details the configuration options available in Blizzard 2.1.

## Configuration File Formats

Blizzard 2.1 supports the following configuration file formats:
- JSON (recommended)
- YAML
- Key-value pairs (.env format)

The recommended format is JSON for its simplicity and consistency.

## Main Configuration Sections

### Database Configuration

```json
"database": {
  "host": "localhost",           // Database server hostname
  "port": 5432,                  // Database server port
  "name": "irs990",              // Database name
  "user": "postgres",            // Database username
  "password": "password",        // Database password
  "ssl_mode": "prefer",          // SSL mode (disable, prefer, require, verify-ca, verify-full)
  "connection_timeout": 30,      // Connection timeout in seconds
  "pool_size": 5,                // Connection pool size
  "pool_timeout": 30,            // Connection pool timeout in seconds
  "connection_retries": 3        // Number of connection retries
}
```

### Processing Configuration

```json
"processing": {
  "batch_size": 100,             // Number of items to process in a batch
  "workers": 4,                  // Number of worker processes/threads
  "log_level": "INFO",           // Log level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
  "max_files_per_run": 1000,     // Maximum number of files to process in a single run
  "continue_on_error": true      // Whether to continue processing when an error occurs
}
```

### Storage Configuration

```json
"storage": {
  "xml_archive_path": "./xml_archive", // Path to store archived XML files
  "reports_path": "./reports",         // Path to store generated reports
  "cache_enabled": true,               // Whether to enable caching
  "cache_max_size": 500                // Maximum number of items in cache
}
```

## Environment Variables

You can also use environment variables to override configuration settings. The naming convention is:

```
SECTION_OPTION=value
```

For example:
- `DATABASE_HOST=db.example.com`
- `PROCESSING_BATCH_SIZE=200`
- `STORAGE_CACHE_ENABLED=false`

## Configuration Precedence

Configuration values are loaded in the following order (later sources override earlier ones):

1. Default values
2. Configuration file
3. Environment variables

## Loading Configuration

```python
from src.config.settings import Settings

# Load from a specific file
settings = Settings.from_file('config/settings.json')

# Get database parameters
db_params = settings.get_db_params()

# Get specific values
host = settings.get('database', 'host', 'localhost')
batch_size = settings.get('processing', 'batch_size', 100)
```

## Templates

A template configuration file is provided at:
`/workspaces/blizzard/2.1/config/templates/settings_template.json`

Copy this template to create your own configuration:

```bash
cp config/templates/settings_template.json config/settings.json
```

Then edit the `config/settings.json` file with your specific values.
