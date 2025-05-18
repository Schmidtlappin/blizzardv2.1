"""
ETL pipeline framework for the Blizzard system.
"""

from typing import List, Dict, Any, Optional, TypeVar, Generic

T = TypeVar('T')
U = TypeVar('U')

class Extractor(Generic[T]):
    """Base class for data extractors."""
    
    def extract(self, source: Any) -> T:
        """Extract data from source."""
        raise NotImplementedError("Subclasses must implement extract method")

class Transformer(Generic[T, U]):
    """Base class for data transformers."""
    
    def transform(self, data: T) -> U:
        """Transform extracted data."""
        raise NotImplementedError("Subclasses must implement transform method")

class Loader(Generic[U]):
    """Base class for data loaders."""
    
    def load(self, data: U) -> Any:
        """Load transformed data into destination."""
        raise NotImplementedError("Subclasses must implement load method")

class Pipeline:
    """ETL pipeline that coordinates extraction, transformation, and loading."""
    
    def __init__(self, extractors=None, transformers=None, loaders=None):
        """Initialize the pipeline with components."""
        self.extractors = extractors or []
        self.transformers = transformers or []
        self.loaders = loaders or []
    
    def run(self, source: Any) -> List[Any]:
        """Run the complete ETL pipeline."""
        # Extract phase
        extracted_data = source
        for extractor in self.extractors:
            extracted_data = extractor.extract(extracted_data)
        
        # Transform phase
        transformed_data = extracted_data
        for transformer in self.transformers:
            transformed_data = transformer.transform(transformed_data)
        
        # Load phase
        results = []
        for loader in self.loaders:
            result = loader.load(transformed_data)
            results.append(result)
        
        return results
