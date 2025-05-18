"""
Connection pooling for database access.
"""

import psycopg2
from psycopg2 import pool
from typing import Optional, Dict, Any

from src.core.exceptions import DatabaseError

class ConnectionPool:
    """A singleton connection pool for database connections."""
    
    _instance = None
    
    @classmethod
    def get_instance(cls, **kwargs):
        """Get or create the singleton instance."""
        if cls._instance is None:
            cls._instance = cls(**kwargs)
        return cls._instance
    
    def __init__(self, min_conn=1, max_conn=10, **db_params):
        """Initialize the connection pool."""
        try:
            self.pool = psycopg2.pool.ThreadedConnectionPool(
                min_conn, max_conn, **db_params
            )
        except psycopg2.Error as e:
            raise DatabaseError(f"Failed to create connection pool: {e}")
    
    def get_connection(self):
        """Get a connection from the pool."""
        try:
            return self.pool.getconn()
        except psycopg2.Error as e:
            raise DatabaseError(f"Failed to get connection: {e}")
    
    def return_connection(self, conn):
        """Return a connection to the pool."""
        self.pool.putconn(conn)
    
    def close_all(self):
        """Close all connections in the pool."""
        self.pool.closeall()
