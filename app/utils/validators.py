"""
Data validation utilities for the Retail Risk Intelligence Platform.

This module provides validation functions for CSV and JSON data uploads,
ensuring data quality before processing.

Requirements: 13.1, 13.2, 13.3, 13.4, 13.6, 13.7
"""

from typing import Dict, List, Any, Union, Optional
from datetime import datetime
import pandas as pd
import json
from io import StringIO


# Expected schema for retail data
REQUIRED_FIELDS = [
    "product_id",
    "product_name",
    "date",
    "quantity",
    "price"
]

FIELD_TYPES = {
    "product_id": str,
    "product_name": str,
    "date": str,  # Will be validated as ISO 8601
    "quantity": (int, float),
    "price": (int, float)
}

NUMERIC_FIELDS = ["quantity", "price"]
DATE_FIELDS = ["date"]


def validate_csv_schema(file_content: Union[str, bytes]) -> Dict[str, Any]:
    """
    Validate CSV file schema.
    
    Requirements: 13.6 - Validate CSV files contain header rows
    
    Args:
        file_content: CSV file content as string or bytes
        
    Returns:
        Dict with validation results:
        {
            "is_valid": bool,
            "errors": List[str],
            "warnings": List[str],
            "data": pd.DataFrame or None
        }
    """
    errors = []
    warnings = []
    data = None
    
    try:
        # Convert bytes to string if needed
        if isinstance(file_content, bytes):
            file_content = file_content.decode('utf-8')
        
        # Try to read CSV
        data = pd.read_csv(StringIO(file_content))
        
        # Check if CSV has header rows (at least one row)
        if data.empty:
            errors.append("CSV file is empty")
            return {
                "is_valid": False,
                "errors": errors,
                "warnings": warnings,
                "data": None
            }
        
        # Check if header exists by verifying required fields are present
        # If none of the required fields are in columns, likely no header
        has_required_fields = any(field in data.columns for field in REQUIRED_FIELDS)
        if not has_required_fields:
            errors.append("CSV file does not contain header rows with required field names")
        
        # Validate required fields
        field_validation = validate_required_fields(data)
        if not field_validation["is_valid"]:
            errors.extend(field_validation["errors"])
        
        # Validate data types
        type_validation = validate_data_types(data)
        if not type_validation["is_valid"]:
            errors.extend(type_validation["errors"])
        
        # Validate date format
        date_validation = validate_date_format(data)
        if not date_validation["is_valid"]:
            errors.extend(date_validation["errors"])
        
        # Validate numeric fields
        numeric_validation = validate_numeric_fields(data)
        if not numeric_validation["is_valid"]:
            errors.extend(numeric_validation["errors"])
        
        # Check for duplicate records
        if data.duplicated().any():
            duplicate_count = data.duplicated().sum()
            errors.append(f"CSV contains {duplicate_count} duplicate records")
        
    except pd.errors.EmptyDataError:
        errors.append("CSV file is empty or has no data")
    except pd.errors.ParserError as e:
        errors.append(f"CSV parsing error: {str(e)}")
    except Exception as e:
        errors.append(f"Unexpected error validating CSV: {str(e)}")
    
    return {
        "is_valid": len(errors) == 0,
        "errors": errors,
        "warnings": warnings,
        "data": data if len(errors) == 0 else None
    }


def validate_json_schema(file_content: Union[str, bytes, dict]) -> Dict[str, Any]:
    """
    Validate JSON file schema.
    
    Requirements: 13.7 - Validate JSON files conform to defined schema structure
    
    Args:
        file_content: JSON file content as string, bytes, or dict
        
    Returns:
        Dict with validation results:
        {
            "is_valid": bool,
            "errors": List[str],
            "warnings": List[str],
            "data": pd.DataFrame or None
        }
    """
    errors = []
    warnings = []
    data = None
    
    try:
        # Parse JSON if string or bytes
        if isinstance(file_content, bytes):
            file_content = file_content.decode('utf-8')
        
        if isinstance(file_content, str):
            json_data = json.loads(file_content)
        else:
            json_data = file_content
        
        # JSON should be either a list of records or a dict with a records key
        if isinstance(json_data, list):
            records = json_data
        elif isinstance(json_data, dict):
            if "records" in json_data:
                records = json_data["records"]
            elif "data" in json_data:
                records = json_data["data"]
            else:
                errors.append("JSON must contain 'records' or 'data' key, or be a list of records")
                return {
                    "is_valid": False,
                    "errors": errors,
                    "warnings": warnings,
                    "data": None
                }
        else:
            errors.append("JSON must be a list or object with records")
            return {
                "is_valid": False,
                "errors": errors,
                "warnings": warnings,
                "data": None
            }
        
        # Check if records is empty
        if not records:
            errors.append("JSON file contains no records")
            return {
                "is_valid": False,
                "errors": errors,
                "warnings": warnings,
                "data": None
            }
        
        # Convert to DataFrame for validation
        data = pd.DataFrame(records)
        
        # Validate required fields
        field_validation = validate_required_fields(data)
        if not field_validation["is_valid"]:
            errors.extend(field_validation["errors"])
        
        # Validate data types
        type_validation = validate_data_types(data)
        if not type_validation["is_valid"]:
            errors.extend(type_validation["errors"])
        
        # Validate date format
        date_validation = validate_date_format(data)
        if not date_validation["is_valid"]:
            errors.extend(date_validation["errors"])
        
        # Validate numeric fields
        numeric_validation = validate_numeric_fields(data)
        if not numeric_validation["is_valid"]:
            errors.extend(numeric_validation["errors"])
        
        # Check for duplicate records
        if data.duplicated().any():
            duplicate_count = data.duplicated().sum()
            errors.append(f"JSON contains {duplicate_count} duplicate records")
        
    except json.JSONDecodeError as e:
        errors.append(f"JSON parsing error: {str(e)}")
    except Exception as e:
        errors.append(f"Unexpected error validating JSON: {str(e)}")
    
    return {
        "is_valid": len(errors) == 0,
        "errors": errors,
        "warnings": warnings,
        "data": data if len(errors) == 0 else None
    }


def validate_required_fields(data: pd.DataFrame) -> Dict[str, Any]:
    """
    Validate presence of required fields.
    
    Requirements: 13.1 - Validate presence of required fields
    
    Args:
        data: DataFrame to validate
        
    Returns:
        Dict with validation results:
        {
            "is_valid": bool,
            "errors": List[str],
            "warnings": List[str]
        }
    """
    errors = []
    warnings = []
    
    # Check for required fields
    missing_fields = [field for field in REQUIRED_FIELDS if field not in data.columns]
    
    if missing_fields:
        errors.append(f"Missing required fields: {', '.join(missing_fields)}")
    
    # Check for null values in required fields
    for field in REQUIRED_FIELDS:
        if field in data.columns:
            null_count = data[field].isnull().sum()
            if null_count > 0:
                errors.append(f"Field '{field}' contains {null_count} null values")
    
    return {
        "is_valid": len(errors) == 0,
        "errors": errors,
        "warnings": warnings
    }


def validate_data_types(data: pd.DataFrame) -> Dict[str, Any]:
    """
    Validate data types for all fields.
    
    Requirements: 13.2 - Validate data types for all fields
    
    Args:
        data: DataFrame to validate
        
    Returns:
        Dict with validation results:
        {
            "is_valid": bool,
            "errors": List[str],
            "warnings": List[str]
        }
    """
    errors = []
    warnings = []
    
    for field, expected_type in FIELD_TYPES.items():
        if field not in data.columns:
            continue
        
        # Check each value in the column
        for idx, value in data[field].items():
            if pd.isnull(value):
                continue
            
            # Handle tuple of types (e.g., (int, float))
            if isinstance(expected_type, tuple):
                if not isinstance(value, expected_type):
                    # Try to convert
                    try:
                        if int in expected_type or float in expected_type:
                            float(value)
                    except (ValueError, TypeError):
                        errors.append(
                            f"Field '{field}' at row {idx}: expected {expected_type}, got {type(value).__name__}"
                        )
                        break  # Only report first error per field
            else:
                if not isinstance(value, expected_type):
                    # Try to convert for string types
                    if expected_type == str:
                        try:
                            str(value)
                        except:
                            errors.append(
                                f"Field '{field}' at row {idx}: expected {expected_type.__name__}, got {type(value).__name__}"
                            )
                            break
                    else:
                        errors.append(
                            f"Field '{field}' at row {idx}: expected {expected_type.__name__}, got {type(value).__name__}"
                        )
                        break
    
    return {
        "is_valid": len(errors) == 0,
        "errors": errors,
        "warnings": warnings
    }


def validate_date_format(data: pd.DataFrame) -> Dict[str, Any]:
    """
    Validate ISO 8601 date format.
    
    Requirements: 13.3 - Validate ISO 8601 format for date fields
    
    Args:
        data: DataFrame to validate
        
    Returns:
        Dict with validation results:
        {
            "is_valid": bool,
            "errors": List[str],
            "warnings": List[str]
        }
    """
    errors = []
    warnings = []
    
    for field in DATE_FIELDS:
        if field not in data.columns:
            continue
        
        # Check each date value
        for idx, value in data[field].items():
            if pd.isnull(value):
                continue
            
            # Try to parse as ISO 8601
            try:
                # Convert to string if not already
                date_str = str(value)
                
                # Try multiple ISO 8601 formats
                parsed = False
                for fmt in [
                    "%Y-%m-%d",
                    "%Y-%m-%dT%H:%M:%S",
                    "%Y-%m-%dT%H:%M:%SZ",
                    "%Y-%m-%dT%H:%M:%S.%f",
                    "%Y-%m-%dT%H:%M:%S.%fZ",
                    "%Y-%m-%d %H:%M:%S"
                ]:
                    try:
                        datetime.strptime(date_str, fmt)
                        parsed = True
                        break
                    except ValueError:
                        continue
                
                if not parsed:
                    # Try pandas to_datetime as fallback
                    try:
                        pd.to_datetime(date_str, format='ISO8601')
                    except:
                        errors.append(
                            f"Field '{field}' at row {idx}: invalid ISO 8601 date format: {date_str}"
                        )
                        break  # Only report first error per field
                        
            except Exception as e:
                errors.append(
                    f"Field '{field}' at row {idx}: error parsing date: {str(e)}"
                )
                break
    
    return {
        "is_valid": len(errors) == 0,
        "errors": errors,
        "warnings": warnings
    }


def validate_numeric_fields(data: pd.DataFrame) -> Dict[str, Any]:
    """
    Validate non-negative values for numeric fields.
    
    Requirements: 13.4 - Validate non-negative values for quantities and prices
    
    Args:
        data: DataFrame to validate
        
    Returns:
        Dict with validation results:
        {
            "is_valid": bool,
            "errors": List[str],
            "warnings": List[str]
        }
    """
    errors = []
    warnings = []
    
    for field in NUMERIC_FIELDS:
        if field not in data.columns:
            continue
        
        # Check each numeric value
        for idx, value in data[field].items():
            if pd.isnull(value):
                continue
            
            try:
                # Convert to float for comparison
                numeric_value = float(value)
                
                if numeric_value < 0:
                    errors.append(
                        f"Field '{field}' at row {idx}: negative value not allowed: {numeric_value}"
                    )
                    break  # Only report first error per field
                    
            except (ValueError, TypeError):
                errors.append(
                    f"Field '{field}' at row {idx}: cannot convert to numeric: {value}"
                )
                break
    
    return {
        "is_valid": len(errors) == 0,
        "errors": errors,
        "warnings": warnings
    }
