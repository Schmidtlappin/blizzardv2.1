"""
Database schema models for the Blizzard system.

This module defines the database schema models used by the system.
"""

from typing import Dict, Any, List, Optional, Union
import logging

from src.core.exceptions import DatabaseError

logger = logging.getLogger(__name__)

class FieldDefinition:
    """
    Definition of a field in the database schema.
    
    This class represents metadata about a field, including its name,
    type, description, and other attributes.
    """
    
    def __init__(self, name: str, field_type: str, description: Optional[str] = None,
                 xpath: Optional[str] = None, form_type: Optional[str] = None,
                 repeating_group: bool = False, field_id: Optional[int] = None):
        """
        Initialize a field definition.
        
        Args:
            name: Field name
            field_type: Field data type
            description: Field description
            xpath: XPath to the field in the XML
            form_type: Form type the field appears in
            repeating_group: Whether the field is part of a repeating group
            field_id: Field ID in the database
        """
        self.field_id = field_id
        self.name = name
        self.field_type = field_type
        self.description = description or ""
        self.xpath = xpath or ""
        self.form_type = form_type or ""
        self.repeating_group = repeating_group
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary representation."""
        return {
            "field_id": self.field_id,
            "name": self.name,
            "field_type": self.field_type,
            "description": self.description,
            "xpath": self.xpath,
            "form_type": self.form_type,
            "repeating_group": self.repeating_group
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'FieldDefinition':
        """Create from dictionary representation."""
        return cls(
            name=data.get("name", ""),
            field_type=data.get("field_type", ""),
            description=data.get("description"),
            xpath=data.get("xpath"),
            form_type=data.get("form_type"),
            repeating_group=data.get("repeating_group", False),
            field_id=data.get("field_id")
        )

class FilingModel:
    """
    Model for a filing in the database.
    
    This class represents a filing and its associated values.
    """
    
    def __init__(self, ein: str, tax_year: str, form_type: str, 
                 filing_id: Optional[int] = None):
        """
        Initialize a filing model.
        
        Args:
            ein: Employer Identification Number
            tax_year: Tax year
            form_type: Form type (e.g., 990, 990EZ, 990PF)
            filing_id: Filing ID in the database
        """
        self.filing_id = filing_id
        self.ein = ein
        self.tax_year = tax_year
        self.form_type = form_type
        self.values = {}  # Field values
        self.repeating_groups = []  # Repeating groups
    
    def add_value(self, field_name: str, value: Any) -> None:
        """
        Add a field value to the filing.
        
        Args:
            field_name: Name of the field
            value: Field value
        """
        self.values[field_name] = value
    
    def add_repeating_group(self, group_name: str, values: List[Dict[str, Any]]) -> None:
        """
        Add a repeating group to the filing.
        
        Args:
            group_name: Name of the repeating group
            values: List of dictionaries with values for each instance
        """
        self.repeating_groups.append({
            "group_name": group_name,
            "values": values
        })
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary representation."""
        return {
            "filing_id": self.filing_id,
            "ein": self.ein,
            "tax_year": self.tax_year,
            "form_type": self.form_type,
            "values": self.values,
            "repeating_groups": self.repeating_groups
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'FilingModel':
        """Create from dictionary representation."""
        filing = cls(
            ein=data.get("ein", ""),
            tax_year=data.get("tax_year", ""),
            form_type=data.get("form_type", ""),
            filing_id=data.get("filing_id")
        )
        
        filing.values = data.get("values", {})
        filing.repeating_groups = data.get("repeating_groups", [])
        
        return filing
