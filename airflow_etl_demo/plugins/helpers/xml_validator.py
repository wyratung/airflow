"""
XML Validation Helper Module
"""
import xml.etree.ElementTree as ET
from typing import List, Dict, Any
import logging

class XMLValidator:
    def __init__(self, validation_rules: Dict[str, Any]):
        self.rules = validation_rules
        self.logger = logging.getLogger(__name__)
    
    def validate(self, xml_content: str) -> tuple[bool, List[str]]:
        """
        Validate XML content against predefined rules
        Returns: (is_valid, list_of_errors)
        """
        errors = []
        
        try:
            root = ET.fromstring(xml_content)
        except ET.ParseError as e:
            return False, [f"XML Parse Error: {str(e)}"]
        
        # Validate required fields
        for field, xpath in self.rules.get('required_fields', {}).items():
            if not root.find(xpath):
                errors.append(f"Missing required field: {field}")
        
        # Validate data types
        for field, config in self.rules.get('data_types', {}).items():
            element = root.find(config['xpath'])
            if element is not None:
                if not self._validate_type(element.text, config['type']):
                    errors.append(f"Invalid type for {field}")
        
        return len(errors) == 0, errors
    
    def _validate_type(self, value: str, expected_type: str) -> bool:
        """Validate data type"""
        if expected_type == 'numeric':
            return value.isdigit()
        elif expected_type == 'alphanumeric':
            return value.isalnum()
        return True
