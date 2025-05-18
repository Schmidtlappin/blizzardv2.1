"""
Detector module for identifying repeating groups in XML files.

This module provides functionality to detect and analyze repeating element patterns
in IRS 990 XML filings. It uses pattern matching and structural analysis to identify
groups of elements that repeat within a filing, including nested repeating groups.
"""

import re
import logging
from typing import List, Dict, Any, Set, Tuple, Optional
from lxml import etree

from src.core.exceptions import RepeatingGroupError
from src.xml.streaming import StreamingParser
from src.repeating_groups.utils import clean_element_name, guess_table_name

logger = logging.getLogger(__name__)

class RepeatingGroupDetector:
    """
    Detector for repeating element groups in XML files.
    
    This class analyzes XML structure to identify repeating element patterns,
    which are common in IRS 990 filings (e.g., lists of officers, expenses, grants).
    It now supports detection of nested repeating groups.
    """
    
    def __init__(self, namespaces=None, max_nesting_level=3):
        """
        Initialize the detector with optional namespaces.
        
        Args:
            namespaces: XML namespaces to use for XPath queries
            max_nesting_level: Maximum level of nesting to detect (default: 3)
        """
        self.namespaces = namespaces or {}
        self.parser = StreamingParser(namespaces=self.namespaces)
        self.max_nesting_level = max_nesting_level
        # Store parent-child relationships
        self.group_hierarchy = {}
        
    def find_repeating_groups(self, xml_file_path: str) -> List[Dict[str, Any]]:
        """
        Find all repeating element groups in an XML file.
        
        Args:
            xml_file_path: Path to the XML file
            
        Returns:
            List of dictionaries describing the detected repeating groups
        """
        # Analyze document structure
        root = self._load_xml(xml_file_path)
        potential_groups = self._identify_potential_groups(root)
        
        # Filter and refine potential repeating groups
        validated_groups = self._validate_groups(potential_groups)
        
        # Build hierarchy of groups
        from src.repeating_groups.nested_detector import build_group_hierarchy
        self.group_hierarchy = build_group_hierarchy(validated_groups, root)
        
        # Extract metadata for each group
        results = []
        for group_element, group_name in validated_groups:
            group_info = self._extract_group_metadata((group_element, group_name), root)
            if group_info:
                # Add parent group information if available
                element_path = root.getroottree().getpath(group_element)
                if element_path in self.group_hierarchy:
                    parent_path = self.group_hierarchy[element_path]
                    group_info['parent_path'] = parent_path
                
                results.append(group_info)
            
        return results
        
    def find_nested_groups(self, xml_file_path: str) -> List[Dict[str, Any]]:
        """
        Find all repeating element groups in an XML file, including nested groups.
        
        Args:
            xml_file_path: Path to the XML file
            
        Returns:
            List of dictionaries describing the detected repeating groups, with nested groups included
        """
        from src.repeating_groups.nested_detector import find_nested_repeating_groups
        
        # First find all top-level repeating groups
        top_level_groups = self.find_repeating_groups(xml_file_path)
        
        # For each top-level group, find nested groups
        for group in top_level_groups:
            nested_groups = find_nested_repeating_groups(
                xml_file_path,
                group.get('path'),
                self.max_nesting_level, 
                1,  # Start at level 1 for nested groups
                self.namespaces
            )
            
            if nested_groups:
                group['nested_groups'] = nested_groups
                
        return top_level_groups
    
    def _load_xml(self, file_path: str) -> etree._Element:
        """Load XML file and return the root element."""
        try:
            tree = etree.parse(file_path)
            return tree.getroot()
        except Exception as e:
            raise RepeatingGroupError(f"Failed to parse XML file {file_path}: {e}")
    
    def _identify_potential_groups(self, root: etree._Element) -> List[Tuple[etree._Element, str]]:
        """
        Identify potential repeating element groups based on element patterns.
        
        This is a heuristic approach that looks for similar element paths
        that appear multiple times.
        
        Args:
            root: Root element of the XML document
            
        Returns:
            List of tuples (parent_element, group_name) that might represent repeating groups
        """
        potential_groups = []
        
        # First try to find ReturnData element which often contains all data
        return_data_elem = None
        for pattern in ["//*[local-name()='ReturnData']", "//irs:ReturnData", "//default:ReturnData"]:
            try:
                elements = root.xpath(pattern, namespaces=self.namespaces)
                if elements:
                    return_data_elem = elements[0]
                    break
            except Exception:
                pass
                
        # Use ReturnData as start point if found, otherwise use original root
        search_root = return_data_elem if return_data_elem is not None else root
        
        # 1. Look for elements with "Grp" in the name
        grp_patterns = [
            ".//*[contains(local-name(), 'Grp')]",
            ".//*[contains(local-name(), 'Group')]",
            ".//irs:*[contains(local-name(), 'Grp')]",
            ".//default:*[contains(local-name(), 'Grp')]"
        ]
        
        for pattern in grp_patterns:
            try:
                elements = search_root.xpath(pattern, namespaces=self.namespaces)
                for element in elements:
                    # Check if this element has multiple similar children
                    if self._has_repeating_children(element):
                        element_name = etree.QName(element).localname
                        potential_groups.append((element, element_name))
            except Exception:
                continue
        
        # 2. Look for common IRS 990 repeating patterns by name
        common_group_patterns = [
            ".//*[contains(local-name(), 'Compensation')]",
            ".//*[contains(local-name(), 'Officer')]",
            ".//*[contains(local-name(), 'Director')]",
            ".//*[contains(local-name(), 'Highest')]",
            ".//*[contains(local-name(), 'Grant')]",
            ".//*[contains(local-name(), 'Expense')]",
            ".//*[contains(local-name(), 'Program')]",
            ".//*[contains(local-name(), 'Table')]",
            ".//*[contains(local-name(), 'List')]",
            ".//*[contains(local-name(), 'Schedule')]"
        ]
        
        for pattern in common_group_patterns:
            try:
                elements = search_root.xpath(pattern, namespaces=self.namespaces)
                for element in elements:
                    # Avoid duplicates
                    element_name = etree.QName(element).localname
                    parent = element.getparent()
                    if parent is not None and self._has_repeating_children(parent):
                        potential_groups.append((parent, element_name))
            except Exception:
                continue
                
        # Remove duplicates while preserving order
        seen = set()
        result = []
        for element, name in potential_groups:
            element_path = root.getroottree().getpath(element)
            if element_path not in seen:
                seen.add(element_path)
                result.append((element, name))
                
        return result
    
    def _has_repeating_children(self, element: etree._Element) -> bool:
        """Check if an element has multiple children with the same name."""
        child_counts = {}
        for child in element:
            local_name = etree.QName(child).localname
            if local_name in child_counts:
                child_counts[local_name] += 1
            else:
                child_counts[local_name] = 1
                
        # If any child type appears more than once, it's a repeating pattern
        return any(count > 1 for count in child_counts.values())
    
    def _validate_groups(self, potential_groups: List[Tuple[etree._Element, str]]) -> List[Tuple[etree._Element, str]]:
        """
        Validate that potential groups are actually repeating groups.
        
        Args:
            potential_groups: List of potential repeating group tuples
            
        Returns:
            Filtered list of validated repeating group tuples
        """
        validated_groups = []
        
        for element, name in potential_groups:
            # Count all children
            children = list(element)
            if not children:
                continue
                
            # Check child structure consistency
            child_structure = {}
            for child in children:
                child_name = etree.QName(child).localname
                
                # Count grandchildren by name
                grandchildren_names = [etree.QName(gc).localname for gc in child]
                structure_key = tuple(sorted(grandchildren_names))
                
                if structure_key in child_structure:
                    child_structure[structure_key] += 1
                else:
                    child_structure[structure_key] = 1
            
            # Check if any structure appears multiple times
            if any(count > 1 for count in child_structure.values()):
                validated_groups.append((element, name))
                
        return validated_groups
    
    def _extract_group_metadata(self, group_info: Tuple[etree._Element, str], root: etree._Element) -> Dict[str, Any]:
        """
        Extract metadata about a repeating group.
        
        Args:
            group_info: Tuple of (element, name) for the repeating group
            root: Root element of the XML document
            
        Returns:
            Dictionary with metadata about the repeating group
        """
        element, name = group_info
        element_path = root.getroottree().getpath(element)
        children = list(element)
        
        # Extract field names from first child as a representative
        fields = []
        if children:
            first_child = children[0]
            for grandchild in first_child:
                field_name = etree.QName(grandchild).localname
                fields.append({
                    "name": field_name,
                    "path": f".//{field_name}",
                    "sample": grandchild.text if hasattr(grandchild, "text") else None
                })
        
        # Get sample values from first few children
        sample_values = []
        for i, child in enumerate(children[:3]):  # Sample from first 3 children
            sample = {}
            for grandchild in child:
                field_name = etree.QName(grandchild).localname
                sample[field_name] = grandchild.text if hasattr(grandchild, "text") else None
            sample_values.append(sample)
            
        return {
            "name": name,
            "path": element_path,
            "count": len(children),
            "fields": fields,
            "sample_values": sample_values,
            "element": element  # Keep reference to original element
        }
