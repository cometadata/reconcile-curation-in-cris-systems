import re
import os
import logging
from unidecode import unidecode

logger = logging.getLogger(__name__)


def is_latin_char_text(text):
    if not isinstance(text, str):
        return False
    for char in text:
        if '\u0000' <= char <= '\u024F':
            return True
    return False


def normalize_text(text):
    if not isinstance(text, str):
        return text
    if is_latin_char_text(text):
        text = unidecode(text)
    text = text.lower()
    text = re.sub(r'[^\w\s]', '', text)
    text = text.strip()
    return text


def extract_doi(text):
    if not text or not isinstance(text, str):
        return None
    
    text = text.strip().strip('<>').strip('"').strip("'")
    
    url_prefixes = [
        'https://doi.org/', 'http://doi.org/', 'https://dx.doi.org/',
        'http://dx.doi.org/', 'https://www.doi.org/', 'http://www.doi.org/',
        'doi.org/', 'dx.doi.org/', 'www.doi.org/', 'doi:', 'DOI:',
    ]
    
    text_lower = text.lower()
    for prefix in url_prefixes:
        if text_lower.startswith(prefix.lower()):
            text = text[len(prefix):]
            break
    
    if '?' in text: text = text.split('?')[0]
    if '#' in text: text = text.split('#')[0]
    text = text.strip()
    
    doi_pattern = r'^(10\.\d{4,}(?:\.\d+)?/[-._;()\/:a-zA-Z0-9]+)(?:\s|$)'
    match = re.match(doi_pattern, text)
    if match:
        return match.group(1).strip()
    
    if text.startswith('10.') and '/' in text:
        return text.rstrip('.,;:')
    
    return None


def is_likely_acronym(text):
    if not text:
        return False
    text = text.strip()
    if len(text) <= 5 and text.isupper():
        return True
    if len(text.replace('.', '').replace('-', '')) <= 5 and text.replace('.', '').replace('-', '').isupper():
        return True
    return False


def sanitize_file_path_for_sql(file_path, is_output=False):
    if not file_path or not isinstance(file_path, str):
        raise ValueError("File path must be a non-empty string")
    
    file_path = file_path.strip()
    if not file_path:
        raise ValueError("File path cannot be empty or whitespace only")
    
    try:
        abs_path = os.path.abspath(file_path)
    except Exception as e:
        raise ValueError(f"Invalid file path: {e}")
    
    dangerous_chars = ["'", '"', ';', '\0', '\r', '\n']
    for char in dangerous_chars:
        if char in abs_path:
            raise ValueError(f"File path contains dangerous character: {repr(char)}")
    
    if '..' in abs_path or abs_path.startswith('/..'):
        raise ValueError("Path traversal detected in file path")
    
    if is_output:
        parent_dir = os.path.dirname(abs_path)
        if parent_dir and not os.path.exists(parent_dir):
            try:
                os.makedirs(parent_dir, exist_ok=True)
                logger.info(f"Created parent directory: {parent_dir}")
            except OSError as e:
                raise OSError(f"Cannot create parent directory {parent_dir}: {e}")
        
        if parent_dir and not os.access(parent_dir, os.W_OK):
            raise ValueError(f"Parent directory is not writable: {parent_dir}")
    else:
        if not os.path.exists(abs_path):
            raise FileNotFoundError(f"Input file not found: {abs_path}")
        
        if not os.path.isfile(abs_path):
            raise ValueError(f"Path is not a regular file: {abs_path}")
        
        if not os.access(abs_path, os.R_OK):
            raise ValueError(f"File is not readable: {abs_path}")
    
    if len(abs_path) > 4096:
        raise ValueError("File path is too long (>4096 characters)")
    
    logger.debug(f"Sanitized {'output' if is_output else 'input'} file path: {abs_path}")
    return abs_path


def validate_memory_limit(memory_limit):
    if not memory_limit or not isinstance(memory_limit, str):
        raise ValueError("Memory limit must be a non-empty string")
    
    memory_limit = memory_limit.strip().upper()
    
    memory_pattern = r'^\d+[KMGT]B$'
    
    if not re.match(memory_pattern, memory_limit):
        raise ValueError(f"Invalid memory limit format. Expected format like '8GB', '512MB', got: {memory_limit}")
    
    return memory_limit


def validate_column_name(column_name, additional_valid_columns=None):
    if not column_name or not isinstance(column_name, str):
        raise ValueError("Column name must be a non-empty string")
    
    column_name = column_name.strip()
    
    base_valid_columns = {
        'doi', 'work_id', 'authors', 'author_name', 'author_separator',
        'affiliation_name', 'normalized_affiliation_name', 'normalized_affiliation_key',
        'input_doi', 'input_work_id', 'input_author_name', 'input_author',
        'ref_author_name', 'ref_affiliation', 'linkage_status',
        'linking_affiliation', 'discovered_work_id', 'discovered_doi',
        'discovered_author', 'discovered_author_affiliation', 'discovered_ror_id',
        'match_type', 'entity_key', 'source_affiliations', 'extracted_entity',
        'source_embl_affiliation', 'discovered_normalized_affiliation',
        'affiliation_ror', 'clean_doi'
    }
    
    if column_name in base_valid_columns:
        return column_name
    
    if additional_valid_columns and column_name in additional_valid_columns:
        dangerous_patterns = [
            ';', '--', '/*', '*/', 'DROP', 'DELETE', 'INSERT', 'UPDATE', 
            'SELECT', 'UNION', 'EXEC', 'EXECUTE', '\x00', '\r', '\n'
        ]
        
        column_upper = column_name.upper()
        for pattern in dangerous_patterns:
            if pattern in column_upper:
                raise ValueError(f"Column name '{column_name}' contains potentially dangerous pattern: {pattern}")
        
        import re
        if not re.match(r'^[a-zA-Z0-9_\s\(\)\-\.]+$', column_name):
            raise ValueError(f"Column name '{column_name}' contains invalid characters")
        
        return column_name
    
    raise ValueError(f"Column name '{column_name}' is not in the allowlist of valid columns")


