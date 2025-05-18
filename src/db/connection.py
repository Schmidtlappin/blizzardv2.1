"""
Database connection management for the Blizzard system.

This module provides functions to connect to a PostgreSQL database and execute queries.
"""

import os
import psycopg2
import psycopg2.extras
from contextlib import contextmanager
from typing import Dict, Any, Optional, List, Tuple, Iterator, Generator

from src.core.exceptions import DatabaseError
from src.db.connection_pool import ConnectionPool

def load_credentials(credentials_file: str) -> Dict[str, str]:
    """
    Load database credentials from a file.
    
    Args:
        credentials_file: Path to the credentials file
        
    Returns:
        Dictionary with database connection parameters
    """
    if not os.path.exists(credentials_file):
        raise DatabaseError(f"Credentials file not found: {credentials_file}")
    
    creds = {}
    with open(credentials_file, 'r') as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith('#'):
                key, value = line.split('=', 1)
                creds[key.strip()] = value.strip()
    
    required_keys = ['host', 'dbname', 'user', 'password', 'port']
    missing_keys = [key for key in required_keys if key not in creds]
    if missing_keys:
        raise DatabaseError(f"Missing required credentials: {', '.join(missing_keys)}")
    
    return creds

def get_connection(credentials_file: str) -> psycopg2.extensions.connection:
    """
    Get a database connection using connection pooling.
    
    Args:
        credentials_file: Path to the credentials file
        
    Returns:
        Database connection
    """
    creds = load_credentials(credentials_file)
    pool = ConnectionPool.get_instance(**creds)
    return pool.get_connection()

def return_connection(conn: psycopg2.extensions.connection) -> None:
    """
    Return a connection to the pool.
    
    Args:
        conn: Database connection to return
    """
    pool = ConnectionPool.get_instance()
    pool.return_connection(conn)

def execute_query(conn: psycopg2.extensions.connection, query: str, 
                  params: Optional[Tuple] = None) -> List[Dict[str, Any]]:
    """
    Execute a query and return results as a list of dictionaries.
    
    Args:
        conn: Database connection
        query: SQL query to execute
        params: Query parameters
        
    Returns:
        List of dictionaries with query results
    """
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            cur.execute(query, params)
            
            if cur.description:
                columns = [col[0] for col in cur.description]
                return [dict(zip(columns, row)) for row in cur.fetchall()]
            
            return []
    except psycopg2.Error as e:
        conn.rollback()
        raise DatabaseError(f"Query execution error: {e}")

def commit(conn: psycopg2.extensions.connection) -> None:
    """
    Commit changes to the database.
    
    Args:
        conn: Database connection
    """
    conn.commit()

def execute_batch(conn: psycopg2.extensions.connection, query: str, 
                 argslist: List[Tuple], page_size: int = 100) -> None:
    """
    Execute a batch query with multiple parameter sets.
    
    Args:
        conn: Database connection
        query: SQL query to execute
        argslist: List of parameter tuples
        page_size: Number of rows per batch
    """
    try:
        with conn.cursor() as cur:
            psycopg2.extras.execute_batch(cur, query, argslist, page_size=page_size)
    except psycopg2.Error as e:
        conn.rollback()
        raise DatabaseError(f"Batch execution error: {e}")

def execute_values(conn: psycopg2.extensions.connection, query: str, 
                  argslist: List[Tuple], template: Optional[str] = None, 
                  page_size: int = 100) -> List[Dict[str, Any]]:
    """
    Execute a query with multiple rows using the more efficient execute_values method.
    
    Args:
        conn: Database connection
        query: SQL query to execute
        argslist: List of parameter tuples
        template: Optional template string
        page_size: Number of rows per batch
        
    Returns:
        List of dictionaries with query results
    """
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            psycopg2.extras.execute_values(cur, query, argslist, template, page_size)
            
            if cur.description:
                columns = [col[0] for col in cur.description]
                return [dict(zip(columns, row)) for row in cur.fetchall()]
            
            return []
    except psycopg2.Error as e:
        conn.rollback()
        raise DatabaseError(f"Execute values error: {e}")

@contextmanager
def transaction(credentials_file: str) -> Generator[psycopg2.extensions.connection, None, None]:
    """
    Context manager for database transactions.
    
    Args:
        credentials_file: Path to the credentials file
        
    Yields:
        Database connection
        
    Example:
        with transaction(credentials_file) as conn:
            execute_query(conn, "INSERT INTO table VALUES (%s)", ("value",))
    """
    conn = get_connection(credentials_file)
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        return_connection(conn)

def create_tables(conn: psycopg2.extensions.connection, schema_file: str) -> None:
    """
    Create database tables from a schema file.
    
    Args:
        conn: Database connection
        schema_file: Path to the SQL schema file
    """
    if not os.path.exists(schema_file):
        raise DatabaseError(f"Schema file not found: {schema_file}")
    
    try:
        with open(schema_file, 'r') as f:
            sql = f.read()
        
        with conn.cursor() as cur:
            cur.execute(sql)
        conn.commit()
    except (IOError, psycopg2.Error) as e:
        conn.rollback()
        raise DatabaseError(f"Failed to create tables: {e}")

def initialize_database(credentials_file: str, schema_file: str) -> None:
    """
    Initialize the database with the required schema.
    
    Args:
        credentials_file: Path to the credentials file
        schema_file: Path to the SQL schema file
    """
    with transaction(credentials_file) as conn:
        create_tables(conn, schema_file)
