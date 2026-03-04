"""
Unit tests for data validation utilities.

Tests validation functions for CSV and JSON data uploads.
"""

import pytest
import pandas as pd
from app.utils.validators import (
    validate_csv_schema,
    validate_json_schema,
    validate_required_fields,
    validate_data_types,
    validate_date_format,
    validate_numeric_fields
)


class TestValidateCSVSchema:
    """Test CSV schema validation."""
    
    def test_valid_csv(self):
        """Test validation of a valid CSV file."""
        csv_content = """product_id,product_name,date,quantity,price
P001,Widget A,2024-01-15,100,29.99
P002,Widget B,2024-01-16,50,49.99"""
        
        result = validate_csv_schema(csv_content)
        
        assert result["is_valid"] is True
        assert len(result["errors"]) == 0
        assert result["data"] is not None
        assert len(result["data"]) == 2
    
    def test_csv_with_bytes(self):
        """Test validation of CSV provided as bytes."""
        csv_content = b"""product_id,product_name,date,quantity,price
P001,Widget A,2024-01-15,100,29.99"""
        
        result = validate_csv_schema(csv_content)
        
        assert result["is_valid"] is True
        assert result["data"] is not None
    
    def test_empty_csv(self):
        """Test validation of empty CSV file."""
        csv_content = ""
        
        result = validate_csv_schema(csv_content)
        
        assert result["is_valid"] is False
        assert any("empty" in error.lower() for error in result["errors"])
    
    def test_csv_without_header(self):
        """Test validation of CSV without header rows."""
        csv_content = """P001,Widget A,2024-01-15,100,29.99
P002,Widget B,2024-01-16,50,49.99"""
        
        result = validate_csv_schema(csv_content)
        
        assert result["is_valid"] is False
        assert any("header" in error.lower() for error in result["errors"])
    
    def test_csv_missing_required_fields(self):
        """Test validation of CSV with missing required fields."""
        csv_content = """product_id,product_name,date
P001,Widget A,2024-01-15
P002,Widget B,2024-01-16"""
        
        result = validate_csv_schema(csv_content)
        
        assert result["is_valid"] is False
        assert any("missing required fields" in error.lower() for error in result["errors"])
    
    def test_csv_with_duplicates(self):
        """Test validation of CSV with duplicate records."""
        csv_content = """product_id,product_name,date,quantity,price
P001,Widget A,2024-01-15,100,29.99
P001,Widget A,2024-01-15,100,29.99"""
        
        result = validate_csv_schema(csv_content)
        
        assert result["is_valid"] is False
        assert any("duplicate" in error.lower() for error in result["errors"])
    
    def test_csv_with_negative_values(self):
        """Test validation of CSV with negative numeric values."""
        csv_content = """product_id,product_name,date,quantity,price
P001,Widget A,2024-01-15,-10,29.99"""
        
        result = validate_csv_schema(csv_content)
        
        assert result["is_valid"] is False
        assert any("negative" in error.lower() for error in result["errors"])
    
    def test_csv_with_invalid_date(self):
        """Test validation of CSV with invalid date format."""
        csv_content = """product_id,product_name,date,quantity,price
P001,Widget A,15/01/2024,100,29.99"""
        
        result = validate_csv_schema(csv_content)
        
        assert result["is_valid"] is False
        assert any("date" in error.lower() for error in result["errors"])


class TestValidateJSONSchema:
    """Test JSON schema validation."""
    
    def test_valid_json_list(self):
        """Test validation of valid JSON as list."""
        json_content = """[
            {"product_id": "P001", "product_name": "Widget A", "date": "2024-01-15", "quantity": 100, "price": 29.99},
            {"product_id": "P002", "product_name": "Widget B", "date": "2024-01-16", "quantity": 50, "price": 49.99}
        ]"""
        
        result = validate_json_schema(json_content)
        
        assert result["is_valid"] is True
        assert len(result["errors"]) == 0
        assert result["data"] is not None
        assert len(result["data"]) == 2
    
    def test_valid_json_with_records_key(self):
        """Test validation of valid JSON with 'records' key."""
        json_content = """{
            "records": [
                {"product_id": "P001", "product_name": "Widget A", "date": "2024-01-15", "quantity": 100, "price": 29.99}
            ]
        }"""
        
        result = validate_json_schema(json_content)
        
        assert result["is_valid"] is True
        assert result["data"] is not None
    
    def test_valid_json_with_data_key(self):
        """Test validation of valid JSON with 'data' key."""
        json_content = """{
            "data": [
                {"product_id": "P001", "product_name": "Widget A", "date": "2024-01-15", "quantity": 100, "price": 29.99}
            ]
        }"""
        
        result = validate_json_schema(json_content)
        
        assert result["is_valid"] is True
        assert result["data"] is not None
    
    def test_json_with_bytes(self):
        """Test validation of JSON provided as bytes."""
        json_content = b"""[{"product_id": "P001", "product_name": "Widget A", "date": "2024-01-15", "quantity": 100, "price": 29.99}]"""
        
        result = validate_json_schema(json_content)
        
        assert result["is_valid"] is True
        assert result["data"] is not None
    
    def test_json_with_dict(self):
        """Test validation of JSON provided as dict."""
        json_content = {
            "records": [
                {"product_id": "P001", "product_name": "Widget A", "date": "2024-01-15", "quantity": 100, "price": 29.99}
            ]
        }
        
        result = validate_json_schema(json_content)
        
        assert result["is_valid"] is True
        assert result["data"] is not None
    
    def test_empty_json(self):
        """Test validation of empty JSON."""
        json_content = "[]"
        
        result = validate_json_schema(json_content)
        
        assert result["is_valid"] is False
        assert any("no records" in error.lower() for error in result["errors"])
    
    def test_json_invalid_structure(self):
        """Test validation of JSON with invalid structure."""
        json_content = """{"invalid": "structure"}"""
        
        result = validate_json_schema(json_content)
        
        assert result["is_valid"] is False
        assert any("records" in error.lower() or "data" in error.lower() for error in result["errors"])
    
    def test_json_with_duplicates(self):
        """Test validation of JSON with duplicate records."""
        json_content = """[
            {"product_id": "P001", "product_name": "Widget A", "date": "2024-01-15", "quantity": 100, "price": 29.99},
            {"product_id": "P001", "product_name": "Widget A", "date": "2024-01-15", "quantity": 100, "price": 29.99}
        ]"""
        
        result = validate_json_schema(json_content)
        
        assert result["is_valid"] is False
        assert any("duplicate" in error.lower() for error in result["errors"])
    
    def test_json_parse_error(self):
        """Test validation of malformed JSON."""
        json_content = """{"invalid": json}"""
        
        result = validate_json_schema(json_content)
        
        assert result["is_valid"] is False
        assert any("parsing" in error.lower() for error in result["errors"])


class TestValidateRequiredFields:
    """Test required fields validation."""
    
    def test_all_required_fields_present(self):
        """Test validation when all required fields are present."""
        data = pd.DataFrame({
            "product_id": ["P001"],
            "product_name": ["Widget A"],
            "date": ["2024-01-15"],
            "quantity": [100],
            "price": [29.99]
        })
        
        result = validate_required_fields(data)
        
        assert result["is_valid"] is True
        assert len(result["errors"]) == 0
    
    def test_missing_required_field(self):
        """Test validation when required field is missing."""
        data = pd.DataFrame({
            "product_id": ["P001"],
            "product_name": ["Widget A"],
            "date": ["2024-01-15"],
            "quantity": [100]
            # Missing 'price'
        })
        
        result = validate_required_fields(data)
        
        assert result["is_valid"] is False
        assert any("missing required fields" in error.lower() for error in result["errors"])
        assert any("price" in error.lower() for error in result["errors"])
    
    def test_null_values_in_required_field(self):
        """Test validation when required field has null values."""
        data = pd.DataFrame({
            "product_id": ["P001", "P002"],
            "product_name": ["Widget A", None],
            "date": ["2024-01-15", "2024-01-16"],
            "quantity": [100, 50],
            "price": [29.99, 49.99]
        })
        
        result = validate_required_fields(data)
        
        assert result["is_valid"] is False
        assert any("null values" in error.lower() for error in result["errors"])


class TestValidateDataTypes:
    """Test data type validation."""
    
    def test_valid_data_types(self):
        """Test validation with correct data types."""
        data = pd.DataFrame({
            "product_id": ["P001"],
            "product_name": ["Widget A"],
            "date": ["2024-01-15"],
            "quantity": [100],
            "price": [29.99]
        })
        
        result = validate_data_types(data)
        
        assert result["is_valid"] is True
        assert len(result["errors"]) == 0
    
    def test_numeric_fields_accept_int_and_float(self):
        """Test that numeric fields accept both int and float."""
        data = pd.DataFrame({
            "product_id": ["P001", "P002"],
            "product_name": ["Widget A", "Widget B"],
            "date": ["2024-01-15", "2024-01-16"],
            "quantity": [100, 50.5],  # int and float
            "price": [29, 49.99]  # int and float
        })
        
        result = validate_data_types(data)
        
        assert result["is_valid"] is True


class TestValidateDateFormat:
    """Test date format validation."""
    
    def test_valid_iso8601_date(self):
        """Test validation with valid ISO 8601 date."""
        data = pd.DataFrame({
            "date": ["2024-01-15"]
        })
        
        result = validate_date_format(data)
        
        assert result["is_valid"] is True
        assert len(result["errors"]) == 0
    
    def test_valid_iso8601_datetime(self):
        """Test validation with valid ISO 8601 datetime."""
        data = pd.DataFrame({
            "date": ["2024-01-15T10:30:00"]
        })
        
        result = validate_date_format(data)
        
        assert result["is_valid"] is True
    
    def test_valid_iso8601_datetime_with_z(self):
        """Test validation with valid ISO 8601 datetime with Z."""
        data = pd.DataFrame({
            "date": ["2024-01-15T10:30:00Z"]
        })
        
        result = validate_date_format(data)
        
        assert result["is_valid"] is True
    
    def test_invalid_date_format(self):
        """Test validation with invalid date format."""
        data = pd.DataFrame({
            "date": ["15/01/2024"]  # DD/MM/YYYY not ISO 8601
        })
        
        result = validate_date_format(data)
        
        assert result["is_valid"] is False
        assert any("invalid iso 8601" in error.lower() for error in result["errors"])


class TestValidateNumericFields:
    """Test numeric field validation."""
    
    def test_valid_positive_values(self):
        """Test validation with positive numeric values."""
        data = pd.DataFrame({
            "quantity": [100, 50],
            "price": [29.99, 49.99]
        })
        
        result = validate_numeric_fields(data)
        
        assert result["is_valid"] is True
        assert len(result["errors"]) == 0
    
    def test_zero_values_allowed(self):
        """Test that zero values are allowed."""
        data = pd.DataFrame({
            "quantity": [0],
            "price": [0.0]
        })
        
        result = validate_numeric_fields(data)
        
        assert result["is_valid"] is True
    
    def test_negative_values_rejected(self):
        """Test that negative values are rejected."""
        data = pd.DataFrame({
            "quantity": [-10],
            "price": [29.99]
        })
        
        result = validate_numeric_fields(data)
        
        assert result["is_valid"] is False
        assert any("negative" in error.lower() for error in result["errors"])
    
    def test_non_numeric_values_rejected(self):
        """Test that non-numeric values are rejected."""
        data = pd.DataFrame({
            "quantity": ["not a number"],
            "price": [29.99]
        })
        
        result = validate_numeric_fields(data)
        
        assert result["is_valid"] is False
        assert any("cannot convert to numeric" in error.lower() for error in result["errors"])
