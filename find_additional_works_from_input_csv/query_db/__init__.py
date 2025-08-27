__version__ = "1.0.0"

from .main import main, parse_arguments
from .db import DatabaseManager
from .config import load_config
from .workflows import FileProcessor, AffiliationSearchProcessor

__all__ = [
    'main',
    'parse_arguments',
    'DatabaseManager', 
    'load_config',
    'FileProcessor',
    'AffiliationSearchProcessor'
]