#!/usr/bin/env python3
"""
Database setup script for Blizzard 2.0.

This script initializes the PostgreSQL database for the IRS 990 XML Processing System.
It creates the necessary schema and tables for storing IRS 990 data.

Usage:
    python setup_database.py [--config CONFIG_FILE]
"""

import os
import sys
import argparse
import logging
from pathlib import Path
from datetime import datetime

# Add the parent directory to the Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.config.settings import Settings
from src.db.connection import initialize_database
from src.db.schema import get_schema_path, create_default_schema

def setup_logging(verbose=False):
    """Set up logging configuration."""
    log_level = logging.DEBUG if verbose else logging.INFO
    
    # Create logs directory if it doesn't exist
    log_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'logs'))
    os.makedirs(log_dir, exist_ok=True)
    
    # Create a unique log file name with timestamp
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    log_file = os.path.join(log_dir, f'db_setup_{timestamp}.log')
    
    # Configure logging
    logging.basicConfig(
        level=log_level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler()
        ]
    )
    
    logger = logging.getLogger('setup_database')
    logger.info(f"Logging to file: {log_file}")
    
    return logger

def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description='Initialize the IRS 990 database')
    parser.add_argument('--config', help='Path to the configuration file')
    parser.add_argument('--force', action='store_true', help='Force recreate tables even if they exist')
    parser.add_argument('--no-prompt', action='store_true', help='Do not prompt for confirmation')
    parser.add_argument('--template', action='store_true', help='Create a template configuration file')
    parser.add_argument('--verbose', '-v', action='store_true', help='Enable verbose logging')
    return parser.parse_args()

def create_template_config():
    """Create a template configuration file if it doesn't exist."""
    template_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'config', 'template.yaml'))
    
    if os.path.exists(template_path):
        print(f"Template configuration file already exists: {template_path}")
        return template_path
    
    # Create or overwrite the template configuration
    config_dir = os.path.dirname(template_path)
    os.makedirs(config_dir, exist_ok=True)
    
    # Check if we need to create the template (it might already exist from our previous step)
    if not os.path.exists(template_path):
        with open(template_path, 'w') as f:
            f.write("""# Blizzard 2.0 Configuration Template
# Copy this file and modify it for your environment

# Database configuration 
database:
  # PostgreSQL server hostname or IP address
  host: localhost
  
  # PostgreSQL server port
  port: 5432
  
  # Database name for IRS 990 data
  dbname: irs990
  
  # Database username
  user: postgres
  
  # Database password
  password: changeme
  
  # Connection pool settings
  min_conn: 1
  max_conn: 10
""")
    
    print(f"Created template configuration file: {template_path}")
    print("Edit this file with your database settings and use it with --config option.")
    
    return template_path

def main():
    """Run the database setup process."""
    args = parse_arguments()
    
    # Handle template creation request
    if args.template:
        create_template_config()
        return 0
    
    logger = setup_logging(args.verbose)
    
    try:
        # Check for existing credentials file
        default_creds_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'config', 'db_credentials.txt')
        
        if os.path.exists(default_creds_path):
            logger.info(f"Found existing credentials file: {default_creds_path}")
            print(f"Using existing credentials file: {default_creds_path}")
            print("To use a different configuration, specify with --config")
            
            # Load credentials from the file to display database info
            creds = {}
            with open(default_creds_path, 'r') as f:
                section = None
                for line in f:
                    line = line.strip()
                    if not line or line.startswith('//') or line.startswith('#'):
                        continue
                    if line.startswith('[') and line.endswith(']'):
                        section = line[1:-1]
                        continue
                    if '=' in line:
                        key, value = line.split('=', 1)
                        key = key.strip()
                        value = value.strip()
                        creds[key] = value
            
            # Display database info
            print(f"Database: {creds.get('dbname', 'unknown')} on {creds.get('host', 'unknown')}")
            
            # Ask for confirmation
            if not args.no_prompt:
                prompt = f"Continue with database initialization? [y/N]: "
                response = input(prompt)
                if response.lower() not in ['y', 'yes']:
                    logger.info("Operation cancelled by user")
                    return 0
            
            # Initialize the database
            logger.info(f"Using credentials from {default_creds_path}")
            create_default_schema()
            schema_path = get_schema_path()
            logger.info(f"Using schema file: {schema_path}")
            initialize_database(default_creds_path, schema_path)
            logger.info("Database initialization completed successfully")
            
            print("✅ Database initialization completed successfully!")
            print(f"   Database: {creds.get('dbname', 'unknown')} on {creds.get('host', 'unknown')}")
            
            return 0
        
        # Load settings from file or use defaults
        if args.config:
            settings = Settings.from_file(args.config)
            logger.info(f"Loaded configuration from {args.config}")
        else:
            settings = Settings.get_instance()
            logger.info("Using default configuration")
        
        # Get database parameters
        db_params = settings.get_db_params()
        logger.info(f"Using database: {db_params['dbname']} on {db_params['host']}")
        
        # Confirm the operation if not using --no-prompt
        if not args.no_prompt:
            prompt = f"This will initialize the database '{db_params['dbname']}' on '{db_params['host']}'. Continue? [y/N]: "
            response = input(prompt)
            if response.lower() not in ['y', 'yes']:
                logger.info("Operation cancelled by user")
                return 0
        
        # Create a credentials file for the database connection
        temp_creds_path = os.path.join(os.getcwd(), '.db_credentials_temp')
        with open(temp_creds_path, 'w') as f:
            for key, value in db_params.items():
                f.write(f"{key}={value}\n")
        logger.info("Created temporary credentials file")
        
        try:
            # Ensure schema file exists
            create_default_schema()
            schema_path = get_schema_path()
            logger.info(f"Using schema file: {schema_path}")
            
            # Initialize the database
            initialize_database(temp_creds_path, schema_path)
            logger.info("Database initialization completed successfully")
            
            print("✅ Database initialization completed successfully!")
            print(f"   Database: {db_params['dbname']} on {db_params['host']}")
            
        finally:
            # Clean up temporary credentials file
            if os.path.exists(temp_creds_path):
                os.unlink(temp_creds_path)
                logger.debug("Removed temporary credentials file")
        
    except Exception as e:
        logger.error(f"Database initialization failed: {e}")
        if args.verbose:
            import traceback
            logger.debug(traceback.format_exc())
        return 1
    
    return 0

if __name__ == "__main__":
    sys.exit(main())
