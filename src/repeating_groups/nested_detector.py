"""
Helper module for detecting nested repeating groups in XML files.

This module provides functionality to detect and process nested repeating element patterns
in IRS 990 XML filings.
"""

import logging
from typing import List, Dict, Any, Set, Tuple
from lxml import etree

logger = logging.getLogger(__name__)

def build_group_hierarchy(groups: List[Tuple[etree._Element, str]], 
                         root: etree._Element) -> Dict[str, str]:
    """
    Build a hierarchy of parent-child relationships between repeating groups.
    
    Args:
        groups: List of (element, name) tuples representing repeating groups
        root: Root element of the XML document
        
    Returns:
        Dictionary mapping child path to parent path
    """
    group_hierarchy = {}
    
    # Get paths for all groups
    group_paths = {}
    for element, name in groups:
        path = root.getroottree().getpath(element)
        group_paths[path] = element
    
    # For each group, find its closest parent group if any
    for child_path, child_element in group_paths.items():
        parent_path = None
        parent_path_len = 0
        
        # Check all other groups to see if they are ancestors of this one
        for path, element in group_paths.items():
            # Skip if it's the same element or path is longer than current candidate
            if path == child_path or len(path) <= parent_path_len:
                continue
            
            # Check if this path is a prefix of child_path (indicating parent-child)
            if child_path.startswith(path + "/"):
                # This is a better parent candidate
                parent_path = path
                parent_path_len = len(path)
        
        # Store parent path if found
        if parent_path:
            group_hierarchy[child_path] = parent_path
            
    return group_hierarchy

def is_nested_in_another_group(element: etree._Element, 
                              groups: List[Tuple[etree._Element, str]]) -> bool:
    """
    Check if an element is nested inside another repeating group.
    
    Args:
        element: The element to check
        groups: List of (element, name) tuples representing repeating groups
        
    Returns:
        True if the element is nested inside another repeating group
    """
    element_path = element.getroottree().getpath(element)
    
    for parent_elem, _ in groups:
        if parent_elem == element:
            continue
            
        parent_path = parent_elem.getroottree().getpath(parent_elem)
        if element_path.startswith(parent_path + "/"):
            return True
            
    return False
    
def find_nested_repeating_groups(xml_file_path: str, parent_group_xpath: str = None, 
                                max_nesting_level: int = 3, current_level: int = 0,
                                namespaces: Dict[str, str] = None) -> List[Dict[str, Any]]:
    """
    Find repeating groups that are nested within a parent group.
    
    Args:
        xml_file_path: Path to the XML file
        parent_group_xpath: XPath to the parent repeating group
        max_nesting_level: Maximum nesting level to detect
        current_level: Current nesting level (for recursion control)
        namespaces: XML namespaces to use
        
    Returns:
        List of dictionaries describing the nested repeating groups
    """
    from src.repeating_groups.detector import RepeatingGroupDetector
    
    if current_level >= max_nesting_level:
        return []
        
    # Create a detector
    detector = RepeatingGroupDetector(namespaces=namespaces)
    
    # Load the XML
    try:
        tree = etree.parse(xml_file_path)
        root = tree.getroot()
    except Exception as e:
        logger.warning(f"Error parsing XML file: {e}")
        return []
    
    # If parent_group_xpath is provided, start from that element
    if parent_group_xpath:
        try:
            parent_elements = root.xpath(parent_group_xpath, namespaces=namespaces or {})
            if not parent_elements:
                return []
            search_root = parent_elements[0]
        except Exception as e:
            logger.warning(f"Error finding parent group: {e}")
            return []
    else:
        search_root = root
        
    # Look for potential groups within this element
    potential_groups = detector._identify_potential_groups(search_root)
    validated_groups = detector._validate_groups(potential_groups)
    
    # Extract metadata for each group
    results = []
    for group_element, group_name in validated_groups:
        group_info = detector._extract_group_metadata((group_element, group_name), root)
        if group_info:
            group_info['nesting_level'] = current_level + 1
            group_info['parent_xpath'] = parent_group_xpath
            results.append(group_info)
            
            # Recursively find nested groups within this group
            if current_level + 1 < max_nesting_level:
                nested_groups = find_nested_repeating_groups(
                    xml_file_path, 
                    group_info['path'], 
                    max_nesting_level,
                    current_level + 1,
                    namespaces
                )
                if nested_groups:
                    group_info['nested_groups'] = nested_groups
    
    return results
