# Blizzard 2.1

## Production-Ready IRS 990 Data Extraction and Processing System

Blizzard 2.1 is a standalone, production-ready version of the Blizzard system for processing IRS 990 tax form data. This version consolidates the learnings and improvements from Blizzard 2.0 into a more streamlined, maintainable, and robust codebase.

## Features

- **Complete ETL Pipeline**: Extract data from IRS 990 XML files, transform it into a structured format, and load it into a PostgreSQL database
- **Repeating Groups Support**: Intelligent detection and processing of repeating data elements in IRS 990 forms
- **Production-Ready Database Loader**: Consolidated PostgreSQL loader with robust error handling and transaction management
- **Performance Optimized**: Efficient batch processing and connection pooling
- **Command-line Interface**: Easy-to-use CLI for common operations
- **Comprehensive Documentation**: Clear guides for installation, setup, and usage

## Installation

### Requirements

- Python 3.8+
- PostgreSQL 12+
- Packages listed in requirements.txt

### Quick Install

```bash
# Clone the repository
git clone https://github.com/your-org/blizzard.git

# Navigate to the 2.1 directory
cd blizzard/2.1

# Install dependencies
pip install -r requirements.txt

# Install development dependencies (optional)
pip install -r requirements-dev.txt

# Install the package
pip install -e .
```

## Configuration

Create a configuration file at `config/settings.json` with your database connection settings:

```json
{
  "database": {
    "host": "localhost",
    "port": 5432,
    "name": "irs990",
    "user": "postgres",
    "password": "password"
  },
  "processing": {
    "batch_size": 100,
    "workers": 4
  }
}
```

## Getting Started

### Setup the Database

```bash
python -m scripts.setup_database --config config/settings.json
```

### Adding XML Files to Process

Place your IRS 990 XML files in the `xml_files` directory at the root of the project. You can also use the provided script to copy files from another location:

```bash
# Copy 100 XML files from the main workspace
python -m scripts.copy_xml_files

# Copy XML files from a specific year
python -m scripts.copy_xml_files --year 2023 --limit 50

# Copy a random sample of files
python -m scripts.copy_xml_files --random --limit 200
```

### Verify XML Files

Check that your XML files are properly staged for processing:

```bash
# Check XML files in the default directory
python -m scripts.check_xml_files

# Perform basic XML validation
python -m scripts.check_xml_files --validate
```

### Process XML Files

```bash
# Process all files in the xml_files directory
python -m scripts.run_etl

# Process files with custom options
python -m scripts.run_etl --batch-size 100 --workers 4

# Process files in batches with reporting
python -m scripts.batch_etl --report
```

### Using the CLI

```bash
# Setup the database
blizzard setup --config config/settings.json

# Process XML files
blizzard process --batch-size 100

# Check the database status
blizzard check
```

## Project Structure

```
2.1/
├── cli/               # Command-line interface
├── config/            # Configuration files
├── docs/              # Documentation
├── logs/              # Log files
├── reports/           # Generated reports
├── schema/            # Database schema
├── scripts/           # Utility scripts
├── src/               # Source code
│   ├── core/          # Core functionality
│   ├── db/            # Database connectivity
│   ├── etl/           # ETL pipeline
│   ├── logging/       # Logging setup
│   ├── repeating_groups/ # Repeating groups handling
│   └── xml/           # XML processing
├── test/              # Test suite
├── utils/             # Utility functions
└── xml_files/         # Directory for XML files to be processed
```

## Development

### Running Tests

```bash
# Run all tests
make test

# Run specific tests
python -m pytest test/test_etl.py
```

### Linting

```bash
make lint
```

## Documentation

Detailed documentation is available in the `docs` directory:
- [Database Schema](docs/DATABASE_SCHEMA.md)
- [Production Setup Guide](docs/PRODUCTION_SETUP.md)
- [Configuration Options](docs/CONFIGURATION.md)
- [Troubleshooting](docs/TROUBLESHOOTING.md)

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Acknowledgments

This project builds upon the work done in Blizzard 2.0 and consolidates the learnings into a production-ready system.
