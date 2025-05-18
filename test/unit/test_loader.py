"""
Unit tests for the PostgreSQLLoader class.
"""

import os
import sys
import pytest
from unittest.mock import MagicMock, patch
from typing import Dict, Any, List

# Add the parent directory to the Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

from src.etl.loader import PostgreSQLLoader
from src.core.exceptions import DatabaseError


class TestPostgreSQLLoader:
    """Test suite for the PostgreSQLLoader class."""
    
    @pytest.fixture
    def db_params(self):
        """Get database parameters for testing."""
        return {
            "host": "localhost",
            "port": 5432,
            "dbname": "irs990_test",
            "user": "postgres",
            "password": "postgres"
        }
    
    @pytest.fixture
    def sample_filing_data(self):
        """Sample filing data for testing."""
        return {
            "metadata": {
                "filing_id": "test-filing-123",
                "ein": "12-3456789",
                "object_id": "202001000000000",
                "form_type": "990",
                "form_version": "2020v1.0",
                "tax_year": 2020,
                "tax_period": "202012",
                "submission_date": "2021-01-15"
            },
            "organization": {
                "ein": "12-3456789",
                "name": "Test Charity Organization",
                "address_line1": "123 Main St",
                "city": "Anytown",
                "state": "CA",
                "zip": "90210"
            },
            "filing_values": [
                {
                    "filing_id": "test-filing-123",
                    "field_id": "TotalRevenue",
                    "numeric_value": 1000000.00
                },
                {
                    "filing_id": "test-filing-123",
                    "field_id": "MissionStatement",
                    "text_value": "To provide services to the community"
                }
            ],
            "repeating_groups": [
                {
                    "group_id": "group-1",
                    "filing_id": "test-filing-123",
                    "name": "Officers",
                    "xpath": "/Return/ReturnData/IRS990/Officers"
                }
            ],
            "group_values": [
                {
                    "group_id": "group-1",
                    "field_id": "Name",
                    "text_value": "John Smith"
                },
                {
                    "group_id": "group-1",
                    "field_id": "Title",
                    "text_value": "President"
                }
            ],
            "xml_hash": "abc123hash456"
        }
    
    @patch('src.etl.loader.get_db_connection')
    def test_load_success(self, mock_get_conn, db_params, sample_filing_data):
        """Test successful loading of filing data."""
        # Set up mock connection and cursor
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.__enter__.return_value = mock_conn
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        mock_get_conn.return_value = mock_conn
        
        # Set up cursor fetchone results for organization and filing checks
        mock_cursor.fetchone.side_effect = [None, None]
        
        # Create loader and load data
        loader = PostgreSQLLoader(db_params, batch_size=10)
        results = loader.load([sample_filing_data])
        
        # Verify result
        assert len(results) == 1
        assert results[0]['status'] == 'success'
        assert results[0]['filing_id'] == 'test-filing-123'
        assert results[0]['filing_values_count'] == 2
        assert results[0]['repeating_groups_count'] == 1
        
        # Verify database commits were called
        assert mock_conn.commit.called
    
    @patch('src.etl.loader.get_db_connection')
    def test_load_error(self, mock_get_conn, db_params, sample_filing_data):
        """Test error handling during loading."""
        # Set up mock connection to raise an exception
        mock_conn = MagicMock()
        mock_conn.__enter__.return_value = mock_conn
        mock_conn.cursor.side_effect = Exception("Database error")
        mock_get_conn.return_value = mock_conn
        
        # Create loader and load data
        loader = PostgreSQLLoader(db_params, batch_size=10)
        results = loader.load([sample_filing_data])
        
        # Verify result contains error
        assert len(results) == 1
        assert results[0]['status'] == 'error'
        assert 'error' in results[0]
        assert results[0]['filing_id'] == 'test-filing-123'
        
        # Verify rollback was called
        assert mock_conn.rollback.called
