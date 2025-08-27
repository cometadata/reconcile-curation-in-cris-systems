
import logging
from typing import Optional

from rapidfuzz import fuzz
from query_db.analysis.name_matching import are_names_similar

logger = logging.getLogger(__name__)


def are_names_similar_udf(
    name1: Optional[str], 
    name2: Optional[str], 
    name1_style: str, 
    name2_style: str, 
    threshold: float
) -> bool:
    try:
        if name1 is None or name2 is None:
            return False
        
        if not name1.strip() or not name2.strip():
            return False
        
        return are_names_similar(
            name1_str=name1,
            name2_str=name2,
            name1_style=name1_style,
            name2_style=name2_style,
            threshold=threshold
        )
    
    except Exception as e:
        logger.warning(
            f"Error in are_names_similar_udf: {e}. "
            f"Args: name1='{name1}', name2='{name2}', "
            f"name1_style='{name1_style}', name2_style='{name2_style}', "
            f"threshold={threshold}"
        )
        return False


def partial_ratio_udf(str1: Optional[str], str2: Optional[str]) -> float:
    try:
        if str1 is None or str2 is None:
            return 0.0
        
        if not str1.strip() or not str2.strip():
            return 0.0
        
        ratio = fuzz.partial_ratio(str1, str2)
        return ratio / 100.0
    
    except Exception as e:
        logger.warning(
            f"Error in partial_ratio_udf: {e}. "
            f"Args: str1='{str1}', str2='{str2}'"
        )
        return 0.0


def register_name_matching_udf(db_manager) -> None:
    try:
        logger.info("Registering name matching UDF with DuckDB")
        
        if not hasattr(db_manager, 'create_function'):
            raise AttributeError(
                "DatabaseManager instance must have 'create_function' method"
            )
        
        db_manager.create_function(
            name='are_names_similar_udf',
            func=are_names_similar_udf,
            arg_types=['VARCHAR', 'VARCHAR', 'VARCHAR', 'VARCHAR', 'DOUBLE'],
            return_type='BOOLEAN'
        )
        
        logger.info("Successfully registered are_names_similar_udf")
        
    except Exception as e:
        error_msg = f"Failed to register name matching UDF: {e}"
        logger.error(error_msg)
        raise RuntimeError(error_msg) from e


def register_fuzzy_matching_udf(db_manager) -> None:
    try:
        logger.info("Registering fuzzy matching UDF with DuckDB")
        
        if not hasattr(db_manager, 'create_function'):
            raise AttributeError(
                "DatabaseManager instance must have 'create_function' method"
            )
        
        db_manager.create_function(
            name='partial_ratio_udf',
            func=partial_ratio_udf,
            arg_types=['VARCHAR', 'VARCHAR'],
            return_type='DOUBLE'
        )
        
        logger.info("Successfully registered partial_ratio_udf")
        
    except Exception as e:
        error_msg = f"Failed to register fuzzy matching UDF: {e}"
        logger.error(error_msg)
        raise RuntimeError(error_msg) from e


def register_all_udfs(db_manager) -> None:
    try:
        logger.info("Registering all UDFs")
        
        register_name_matching_udf(db_manager)
        
        register_fuzzy_matching_udf(db_manager)
        
        logger.info("Successfully registered all UDFs")
        
    except Exception as e:
        error_msg = f"Failed to register UDFs: {e}"
        logger.error(error_msg)
        raise RuntimeError(error_msg) from e