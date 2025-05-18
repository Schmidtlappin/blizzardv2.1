"""
Integration tests for the ETL pipeline.
"""

import os
import sys
import pytest
import tempfile
from pathlib import Path

# Add the parent directory to the Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

from src.etl.extractor import XMLExtractor
from src.etl.transformer import XMLTransformer
from src.etl.loader import PostgreSQLLoader
from src.etl.pipeline import ETLPipeline
from src.config.settings import Settings
from src.xml.parser import XMLParser
from src.xml.validator import XMLValidator

from ..conftest import FIXTURES_DIR, TEST_CONFIG_FILE


@pytest.mark.integration
class TestETLPipeline:
    """Integration tests for the full ETL pipeline."""
    
    @pytest.fixture
    def settings(self):
        """Get settings for testing."""
        return Settings.from_file(str(TEST_CONFIG_FILE))
    
    @pytest.fixture
    def sample_xml(self):
        """Create a sample XML file for testing."""
        content = """<?xml version="1.0" encoding="UTF-8"?>
<Return>
  <ReturnHeader>
    <ReturnType>990</ReturnType>
    <Filer>
      <EIN>123456789</EIN>
      <BusinessName>
        <BusinessNameLine1>Test Nonprofit Organization</BusinessNameLine1>
      </BusinessName>
      <USAddress>
        <AddressLine1>123 Main Street</AddressLine1>
        <City>Anytown</City>
        <State>CA</State>
        <ZIPCode>90210</ZIPCode>
      </USAddress>
    </Filer>
    <TaxPeriodEndDt>2020-12-31</TaxPeriodEndDt>
  </ReturnHeader>
  <ReturnData>
    <IRS990>
      <TotalRevenue>1000000</TotalRevenue>
      <MissionDesc>To provide services to the community</MissionDesc>
      <Officers>
        <Officer>
          <PersonNm>John Smith</PersonNm>
          <Title>President</Title>
        </Officer>
        <Officer>
          <PersonNm>Jane Doe</PersonNm>
          <Title>Treasurer</Title>
        </Officer>
      </Officers>
    </IRS990>
  </ReturnData>
</Return>"""
        
        with tempfile.NamedTemporaryFile(suffix=".xml", delete=False, dir=FIXTURES_DIR) as f:
            f.write(content.encode('utf-8'))
            temp_path = Path(f.name)
            
        # Return file path, will be deleted in teardown
        return temp_path
    
    @pytest.mark.skip(reason="Requires a real database connection")
    def test_full_pipeline(self, settings, sample_xml):
        """Test the full ETL pipeline with a sample XML file."""
        # Create pipeline components
        extractor = XMLExtractor()
        transformer = XMLTransformer()
        loader = PostgreSQLLoader(settings.get_db_params())
        
        # Create and run pipeline
        pipeline = ETLPipeline(extractor, transformer, loader)
        results = pipeline.process([str(sample_xml)])
        
        # Verify results
        assert len(results) == 1
        assert results[0]['status'] == 'success'
        
    def test_extract_transform(self, sample_xml):
        """Test extraction and transformation steps."""
        # Create pipeline components
        extractor = XMLExtractor()
        transformer = XMLTransformer()
        
        # Extract data
        extracted_data = extractor.extract(str(sample_xml))
        assert extracted_data is not None
        assert 'xml_content' in extracted_data
        
        # Transform data
        transformed_data = transformer.transform(extracted_data)
        assert transformed_data is not None
        assert 'metadata' in transformed_data
        assert 'organization' in transformed_data
        assert transformed_data['organization'].get('name') == 'Test Nonprofit Organization'
        
        # Check for repeating groups
        assert 'repeating_groups' in transformed_data
        assert len(transformed_data.get('repeating_groups', [])) > 0
        
    def teardown_method(self, method):
        """Clean up after tests."""
        # Remove any temporary files created in fixtures directory
        for file in FIXTURES_DIR.glob("*.xml"):
            try:
                file.unlink()
            except Exception:
                pass
