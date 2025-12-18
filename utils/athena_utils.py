"""
Utility functions for AWS Athena operations.
Handles querying data from Athena for analysis and visualization.
"""

import logging
import boto3
from typing import List, Dict, Any
import pandas as pd
from utils.constants import (
    AWS_ACCESS_KEY_ID,
    AWS_SECRET_ACCESS_KEY,
    AWS_SESSION_TOKEN,
    AWS_REGION,
    ATHENA_DATABASE,
    ATHENA_OUTPUT_LOCATION,
)

logger = logging.getLogger(__name__)


def get_athena_client():
    """
    Create and return an Athena client.

    Returns:
        boto3.client: Athena client
    """
    return boto3.client(
        'athena',
        region_name=AWS_REGION,
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        aws_session_token=AWS_SESSION_TOKEN if AWS_SESSION_TOKEN else None
    )


def execute_query(query: str, database: str = ATHENA_DATABASE) -> str:
    """
    Execute a query in Athena and return the query execution ID.

    Args:
        query (str): SQL query to execute
        database (str): Database name (default: openaq_database)

    Returns:
        str: Query execution ID

    Raises:
        Exception: If query execution fails
    """
    client = get_athena_client()

    try:
        response = client.start_query_execution(
            QueryString=query,
            QueryExecutionContext={'Database': database},
            ResultConfiguration={'OutputLocation': ATHENA_OUTPUT_LOCATION}
        )
        query_id = response['QueryExecutionId']
        logger.info(f"[OK] Query started with ID: {query_id}")
        return query_id
    except Exception as e:
        logger.error(f"[FAIL] Failed to start query execution: {e}")
        raise


def wait_for_query_completion(query_id: str, max_attempts: int = 30) -> bool:
    """
    Wait for Athena query to complete.

    Args:
        query_id (str): Query execution ID
        max_attempts (int): Maximum number of status checks

    Returns:
        bool: True if query succeeded, False otherwise

    Raises:
        Exception: If query fails
    """
    client = get_athena_client()

    for attempt in range(max_attempts):
        try:
            response = client.get_query_execution(QueryExecutionId=query_id)
            status = response['QueryExecution']['Status']['State']

            if status == 'SUCCEEDED':
                logger.info(f"[OK] Query {query_id} completed successfully")
                return True
            elif status == 'FAILED':
                error_msg = response['QueryExecution']['Status'].get('StateChangeReason', 'Unknown error')
                logger.error(f"[FAIL] Query {query_id} failed: {error_msg}")
                raise Exception(f"Query failed: {error_msg}")
            elif status == 'CANCELLED':
                logger.error(f"[FAIL] Query {query_id} was cancelled")
                raise Exception("Query was cancelled")

            logger.info(f"Query {query_id} still running... (attempt {attempt + 1}/{max_attempts})")
        except Exception as e:
            logger.error(f"[FAIL] Error checking query status: {e}")
            raise

    logger.error(f"[FAIL] Query {query_id} timeout after {max_attempts} attempts")
    raise Exception(f"Query timeout: {query_id}")


def get_query_results(query_id: str) -> List[Dict[str, Any]]:
    """
    Retrieve results from a completed Athena query.

    Args:
        query_id (str): Query execution ID

    Returns:
        List[Dict]: List of result rows

    Raises:
        Exception: If query hasn't completed or retrieval fails
    """
    client = get_athena_client()

    try:
        response = client.get_query_results(QueryExecutionId=query_id)

        # Get column names from result set metadata
        columns = [col['Name'] for col in response['ResultSet']['ResultSetMetadata']['ColumnInfo']]

        # Get data rows (skip header row)
        rows = response['ResultSet']['Rows'][1:]

        results = []
        for row in rows:
            data = row['Data']
            result_dict = {}
            for i, col_name in enumerate(columns):
                result_dict[col_name] = data[i].get('VarCharValue', None) if i < len(data) else None
            results.append(result_dict)

        logger.info(f"[OK] Retrieved {len(results)} rows from query {query_id}")
        return results
    except Exception as e:
        logger.error(f"[FAIL] Failed to get query results: {e}")
        raise


def query_to_dataframe(query: str, database: str = ATHENA_DATABASE) -> pd.DataFrame:
    """
    Execute query and return results as pandas DataFrame.

    Args:
        query (str): SQL query to execute
        database (str): Database name

    Returns:
        pd.DataFrame: Query results

    Raises:
        Exception: If query fails
    """
    # Execute query
    query_id = execute_query(query, database)

    # Wait for completion
    wait_for_query_completion(query_id)

    # Get results
    results = get_query_results(query_id)

    # Convert to DataFrame
    df = pd.DataFrame(results)
    logger.info(f"[OK] Converted {len(df)} rows to DataFrame")

    return df


def get_table_count(table_name: str, database: str = ATHENA_DATABASE) -> int:
    """
    Get row count for a table in Athena.

    Args:
        table_name (str): Table name
        database (str): Database name

    Returns:
        int: Number of rows

    Raises:
        Exception: If query fails
    """
    query = f"SELECT COUNT(*) as count FROM {database}.{table_name}"

    query_id = execute_query(query, database)
    wait_for_query_completion(query_id)
    results = get_query_results(query_id)

    if results:
        count = int(results[0]['count'])
        logger.info(f"[OK] Table {table_name} has {count} rows")
        return count

    return 0


def list_tables(database: str = ATHENA_DATABASE) -> List[str]:
    """
    List all tables in Athena database.

    Args:
        database (str): Database name

    Returns:
        List[str]: List of table names

    Raises:
        Exception: If query fails
    """
    query = f"""
    SELECT table_name
    FROM information_schema.tables
    WHERE table_schema = '{database}'
    """

    query_id = execute_query(query)
    wait_for_query_completion(query_id)
    results = get_query_results(query_id)

    tables = [row['table_name'] for row in results]
    logger.info(f"[OK] Found {len(tables)} tables in database {database}")

    return tables


def validate_athena_connection() -> bool:
    """
    Test connection to Athena.

    Returns:
        bool: True if connection successful
    """
    try:
        client = get_athena_client()

        # Try to list databases
        response = client.list_databases()
        databases = [db['Name'] for db in response.get('DatabaseList', [])]

        if ATHENA_DATABASE in databases:
            logger.info(f"[OK] Athena connected. Database '{ATHENA_DATABASE}' found.")
            return True
        else:
            logger.warning(f"[WARNING] Athena connected but database '{ATHENA_DATABASE}' not found")
            logger.warning(f"Available databases: {databases}")
            return False
    except Exception as e:
        logger.error(f"[FAIL] Athena connection failed: {e}")
        return False
