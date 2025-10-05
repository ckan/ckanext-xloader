# -*- coding: utf-8 -*-
import os
import pytest
import tempfile
import logging
from typing import Callable, List, Tuple, Any
from unittest.mock import patch, MagicMock
import csv
import sqlalchemy.orm as orm

from ckan.tests import factories
from ckanext.xloader import loader
from ckanext.xloader.loader import get_write_engine
from ckanext.xloader.tests.test_loader import TestLoadBase, get_sample_filepath

logger = logging.getLogger(__name__)


@pytest.fixture()
def Session():
    engine = get_write_engine()
    Session = orm.scoped_session(orm.sessionmaker(bind=engine))
    yield Session
    Session.close()


@pytest.mark.usefixtures("full_reset", "with_plugins")
@pytest.mark.ckan_config("ckan.plugins", "datastore xloader")
class TestChunkedLoading(TestLoadBase):
    
    def _create_mock_split_copy(self, chunk_size: int) -> Callable:
        """Create a mock function for split_copy_by_size with specified chunk size"""
        original_split_copy = loader.split_copy_by_size
        
        def mock_split_copy(input_file: Any, engine: Any, logger: Any, resource_id: str, headers: List[str], delimiter: str = ',', max_size: int = 1024**3) -> Any:
            return original_split_copy(input_file, engine, logger, resource_id, headers, delimiter, chunk_size)
        
        return mock_split_copy
    
    def _create_mock_copy_file(self, copy_calls_list: List[Tuple]) -> Callable:
        """Create a mock function for copy_file that tracks calls"""
        original_copy_file = loader.copy_file
        
        def mock_copy_file(*args: Any, **kwargs: Any) -> Any:
            copy_calls_list.append(args)
            return original_copy_file(*args, **kwargs)
        
        return mock_copy_file
    
    def _generate_large_csv(self, filepath: str, num_rows: int = 100000, row_size_kb: int = 1) -> Tuple[str, List[str], int]:
        """Generate a large CSV file for testing chunked processing"""
        headers = ['id', 'name', 'description', 'data']
        
        # Create data that will make each row approximately row_size_kb KB
        padding_size = (row_size_kb * 1024) - 50  # Account for other columns
        padding_data = 'x' * max(1, padding_size)
        
        with open(filepath, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(headers)
            
            for i in range(num_rows):
                writer.writerow([
                    i + 1,
                    f'Name_{i + 1}',
                    f'Description for row {i + 1}',
                    padding_data
                ])
        
        return filepath, headers, num_rows

    def test_chunked_processing_large_file(self, Session: Any) -> None:
        """Test that large files are processed in chunks and data integrity is maintained"""
        
        # Create a temporary large CSV file (~15MB to trigger chunking)
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as temp_file:
            temp_filepath = temp_file.name
        
        try:
            # Generate file with ~15MB (15000 rows * ~1KB each)
            csv_filepath, expected_headers, expected_rows = self._generate_large_csv(
                temp_filepath, num_rows=15000, row_size_kb=1
            )
            
            # Verify file size is large enough to trigger chunking
            file_size = os.path.getsize(csv_filepath)
            assert file_size > 10 * 1024 * 1024, f"File size {file_size} should be > 10MB"
            
            resource = factories.Resource()
            resource_id = resource['id']
            
            # Set up mocks with 10MB chunk size
            copy_calls = []
            mock_split_copy = self._create_mock_split_copy(10 * 1024 * 1024)
            mock_copy_file = self._create_mock_copy_file(copy_calls)
            
            with patch('ckanext.xloader.loader.split_copy_by_size', side_effect=mock_split_copy):
                with patch('ckanext.xloader.loader.copy_file', side_effect=mock_copy_file):
                    # Load the CSV with chunked processing
                    fields = loader.load_csv(
                        csv_filepath,
                        resource_id=resource_id,
                        mimetype="text/csv",
                        logger=logger,
                    )
            
            # Verify chunking occurred (should have multiple copy calls)
            assert len(copy_calls) > 1, "Expected multiple chunks but file was not chunked"
            
            # Verify data integrity - check that all rows were loaded
            records = self._get_records(Session, resource_id)
            assert len(records) == expected_rows, f"Expected {expected_rows} records, got {len(records)}"
            
            # Verify column structure
            column_names = self._get_column_names(Session, resource_id)
            expected_columns = ['_id', '_full_text'] + expected_headers
            assert column_names == expected_columns
            
            # Verify first and last records to ensure data integrity
            # Sort records by the 'id' column (index 1) to ensure consistent ordering
            sorted_records = sorted(records, key=lambda x: int(x[1]))
            first_record = sorted_records[0]
            last_record = sorted_records[-1]
            
            # Check first record (excluding _id and _full_text columns)
            # The _get_records method excludes _full_text by default, so indices are:
            # 0: _id, 1: id, 2: name, 3: description, 4: data
            
            assert first_record[1] == '1'  # id column (index 1 after _id)
            assert first_record[2] == 'Name_1'  # name column (index 2)
            
            # Check last record
            assert last_record[1] == str(expected_rows)  # id column
            assert last_record[2] == f'Name_{expected_rows}'  # name column
            
        finally:
            # Clean up temporary file
            if os.path.exists(temp_filepath):
                os.unlink(temp_filepath)

    def test_small_file_no_chunking(self, Session: Any) -> None:
        """Test that small files are not chunked when chunk size is larger than file"""
        
        # Use existing small sample file
        csv_filepath = get_sample_filepath("simple.csv")
        resource = factories.Resource()
        resource_id = resource['id']
        
        # Set up mocks with large chunk size to prevent chunking
        copy_calls = []
        mock_split_copy = self._create_mock_split_copy(10 * 1024 * 1024)  # 10MB
        mock_copy_file = self._create_mock_copy_file(copy_calls)
        
        with patch('ckanext.xloader.loader.split_copy_by_size', side_effect=mock_split_copy):
            with patch('ckanext.xloader.loader.copy_file', side_effect=mock_copy_file):
                fields = loader.load_csv(
                    csv_filepath,
                    resource_id=resource_id,
                    mimetype="text/csv",
                    logger=logger,
                )
        
        # Small file should only have one copy call (no chunking)
        assert len(copy_calls) == 1, f"Small file should not be chunked, got {len(copy_calls)} copy calls"
        
        # Verify data loaded correctly
        records = self._get_records(Session, resource_id)
        assert len(records) == 6  # Known number of records in simple.csv

