# Troubleshooting Guide

This guide addresses common issues encountered when using Blizzard 2.1.

## Database Connection Issues

### Connection Refused

**Symptoms:** Error messages like "Connection refused" or "Could not connect to server"

**Solutions:**

1. Check if PostgreSQL is running:
   ```bash
   sudo systemctl status postgresql
   ```

2. Verify connection settings in your configuration file:
   ```bash
   cat config/settings.json
   ```

3. Test PostgreSQL connection manually:
   ```bash
   psql -h localhost -U yourusername -d irs990
   ```

4. Check PostgreSQL logs:
   ```bash
   sudo tail -f /var/log/postgresql/postgresql-12-main.log
   ```

### Authentication Failure

**Symptoms:** "Password authentication failed" or "role does not exist"

**Solutions:**

1. Verify your username and password in configuration

2. Check PostgreSQL authentication settings:
   ```bash
   sudo nano /etc/postgresql/12/main/pg_hba.conf
   ```

3. Reset PostgreSQL user password:
   ```bash
   sudo -u postgres psql
   postgres=# ALTER USER yourusername WITH PASSWORD 'newpassword';
   ```

## XML Processing Errors

### XML Parsing Errors

**Symptoms:** "XML parsing error" or "Invalid XML format"

**Solutions:**

1. Validate XML format integrity:
   ```bash
   python -m scripts.validate_xml --file path/to/file.xml
   ```

2. Check if the XML file is complete:
   ```bash
   xmllint --noout path/to/file.xml
   ```

3. Increase logging level for more detailed error information:
   ```json
   "processing": {
     "log_level": "DEBUG"
   }
   ```

### Missing Schema Files

**Symptoms:** "Schema file not found" or "Cannot validate against schema"

**Solutions:**

1. Make sure XSD files are in the expected location

2. Check the XML namespace matches the XSD version

3. Update schema paths in configuration

## ETL Pipeline Issues

### Process Timeouts

**Symptoms:** Processing stops or times out with large files

**Solutions:**

1. Adjust batch size to a smaller value:
   ```json
   "processing": {
     "batch_size": 50
   }
   ```

2. Increase database connection timeout:
   ```json
   "database": {
     "connection_timeout": 60
   }
   ```

3. Use batch processing instead of trying to process all files at once:
   ```bash
   python -m scripts.batch_etl.py --max-files 100
   ```

### Memory Issues

**Symptoms:** "Out of memory" errors or process crashes

**Solutions:**

1. Reduce batch size:
   ```json
   "processing": {
     "batch_size": 20
   }
   ```

2. Use streaming XML parser for large files (enabled by default)

3. Disable caching if necessary:
   ```json
   "storage": {
     "cache_enabled": false
   }
   ```

## Repeating Groups Issues

### Missing Data

**Symptoms:** Repeating group data is missing or incomplete

**Solutions:**

1. Check logging for specific errors related to repeating groups

2. Enable debug logging to trace the repeating group detection:
   ```json
   "processing": {
     "log_level": "DEBUG"
   }
   ```

3. Run a specific test on the problematic file:
   ```bash
   python -m scripts.test_repeating_groups --file path/to/file.xml
   ```

## CLI Tool Issues

### Command Not Found

**Symptoms:** "Command not found" when running the `blizzard` command

**Solutions:**

1. Make sure the package is installed in development mode:
   ```bash
   pip install -e .
   ```

2. Add the installation directory to your PATH

3. Check that entry points are correctly defined in `setup.py`

## Logging and Reporting

### Missing Logs

**Symptoms:** Logs are not being generated

**Solutions:**

1. Check permissions on the logs directory:
   ```bash
   chmod 755 logs/
   ```

2. Verify logging configuration in settings

3. Check if the application has write access to the logs directory

## Getting Help

If you're still experiencing issues:

1. Check full logs in the `logs` directory
2. Run in debug mode with increased verbosity
3. Submit an issue to the project repository with:
   - Detailed error messages
   - Configuration (with sensitive information removed)
   - Steps to reproduce the problem
