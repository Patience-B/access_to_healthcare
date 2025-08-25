import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, text, inspect
from contextlib import contextmanager
from typing import Generator, List, Optional, Dict, Any
from sqlalchemy.engine import Engine
import logging
import geopandas as gpd
import pandas as pd
import subprocess
import json
import boto3
from sqlalchemy.dialects.postgresql import JSONB


load_dotenv()

logger = logging.getLogger(__name__)


def get_db_credentials(secret_name: str, region_name: str) -> Dict[str, Any]:
    """
    Retrieve database credentials from AWS Secrets Manager.

    This function connects to AWS Secrets Manager to fetch database credentials
    stored as a JSON secret. The secret should contain the necessary database
    connection parameters.

    Args:
        secret_name (str): The name or ARN of the secret in AWS Secrets Manager
        region_name (str): The AWS region where the secret is stored

    Returns:
        Dict[str, Any]: A dictionary containing the database credentials parsed
                       from the JSON secret string. Expected keys include:
                       - username: Database username
                       - password: Database password
                       - host: Database host/endpoint
                       - port: Database port
                       - db_name: Database name

    Raises:
        ClientError: If the secret cannot be retrieved from AWS Secrets Manager
        JSONDecodeError: If the secret string is not valid JSON
        KeyError: If required credentials are missing from the secret

    Example:
        >>> creds = get_db_credentials("prod-db-secret", "us-east-1")
        >>> print(creds["username"])
        'db_user'
    """
    try:
        session = boto3.session.Session()
        client = session.client("secretsmanager", region_name=region_name)
        response = client.get_secret_value(SecretId=secret_name)
        return json.loads(response["SecretString"])
    except Exception as e:
        raise RuntimeError(f"Failed to retrieve database credentials: {str(e)}") from e


def connect_to_aws_db(secret_name: str, region_name: str):
    """
    Create a SQLAlchemy database engine using credentials from AWS Secrets Manager.

    This function retrieves database credentials from AWS Secrets Manager and
    creates a PostgreSQL connection using SQLAlchemy with the psycopg2 driver.

    Args:
        secret_name (str): The name or ARN of the secret in AWS Secrets Manager
        region_name (str): The AWS region where the secret is stored

    Returns:
        sqlalchemy.engine.Engine: A SQLAlchemy engine instance configured for
                                 the PostgreSQL database

    Raises:
        RuntimeError: If database credentials cannot be retrieved
        SQLAlchemyError: If the database engine cannot be created

    Example:
        >>> engine = connect_to_db("prod-db-secret", "us-east-1")
        >>> with engine.connect() as conn:
        ...     result = conn.execute("SELECT 1")
        ...     print(result.fetchone())
        (1,)

    Note:
        The database credentials secret should contain the following JSON structure:
        {
            "username": "db_username",
            "password": "db_password",
            "host": "db_host",
            "port": "5432",
            "dbname": "database_name"
        }
    """
    try:
        creds = get_db_credentials(secret_name, region_name)

        # Validate required credentials
        required_keys = ["username", "password", "host", "port", "dbname"]
        missing_keys = [key for key in required_keys if key not in creds]
        if missing_keys:
            raise KeyError(f"Missing required credentials: {missing_keys}")

        db_url = (
            f"postgresql+psycopg2://{creds['username']}:{creds['password']}@"
            f"{creds['host']}:{creds['port']}/{creds['dbname']}"
        )
        return create_engine(db_url)
    except Exception as e:
        raise RuntimeError(f"Failed to create database connection: {str(e)}") from e


@contextmanager
def connect_to_db() -> Generator[Engine, None, None]:
    """
    Context manager for connecting to a PostgreSQL database using SQLAlchemy.

    Environment variables required:
        - DB_USER: Database username
        - DB_PASSWORD: Database password
        - DB_HOST: Database host
        - DB_PORT: Database port
        - DB_NAME: Database name

    Yields:
        Engine: A SQLAlchemy Engine instance for the PostgreSQL connection.

    Ensures that the engine is properly disposed after usage.
    """
    # Read variables
    user = os.getenv("DB_USER")
    password = os.getenv("DB_PASSWORD")
    host = os.getenv("DB_HOST")
    port = os.getenv("DB_PORT")
    db = os.getenv("DB_NAME")

    # Create connection string and engine
    connection_string = f"postgresql://{user}:{password}@{host}:{port}/{db}"
    engine = create_engine(connection_string)

    try:
        yield engine
    finally:
        engine.dispose()  # Ensures all connections are closed


def create_schema(schema_name: str) -> None:
    """
    Creates a PostgreSQL schema with the given name if it does not already exist.

    The schema name is converted to lowercase before creation.

    Args:
        schema_name (str): The name of the schema to create.

    Returns:
        None
    """
    schema_name = schema_name.lower()

    # Create the schema if it doesn't exist
    with connect_to_db() as engine:
        with engine.connect() as conn:
            logging.info(f"Creating schema {schema_name}")
            conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema_name}"))
            conn.commit()  # Needed to persist the change


def df_to_db(schema_name: str, table_name: str, df: pd.DataFrame) -> None:
    """
    Writes a pandas DataFrame to a PostgreSQL table within a specified schema.

    If the table does not already exist, it will be created and populated with the DataFrame's contents.
    The table will be replaced if it exists.

    Args:
        schema_name (str): The name of the target schema.
        table_name (str): The name of the target table.
        df (pd.DataFrame): The DataFrame to write to the database.

    Returns:
        None
    """
    schema_name = schema_name.lower()
    table_name = table_name.lower()

    # Create schema if it doesn't exist
    create_schema(schema_name)

    if not table_exists(schema_name, table_name):
        logging.info(f"Writing to {schema_name}.{table_name}...")
        with connect_to_db() as engine:
            df.to_sql(
                table_name,
                con=engine,
                schema=schema_name,
                if_exists="replace",
                index=False,
            )


def df_to_db_replace(
    schema_name: str, table_name: str, df: pd.DataFrame, if_exists: str = "replace"
):
    """
    Write DataFrame to database with flexible if_exists parameter

    Args:
        schema_name (str): Database schema name
        table_name (str): Database table name
        df (pd.DataFrame): DataFrame to write

        if_exists (str): How to behave if the table exists. Options:
            - 'fail': Raise a ValueError
            - 'replace': Drop the table before inserting new values
            - 'append': Insert new values to the existing table
    """
    schema_name = schema_name.lower()
    table_name = table_name.lower()

    # Create schema if it doesn't exist
    create_schema(schema_name)

    if if_exists == "replace" or not table_exists(schema_name, table_name):
        logging.info(f"Writing to {schema_name}.{table_name}...")
        with connect_to_db() as engine:
            df.to_sql(
                table_name,
                con=engine,
                schema=schema_name,
                if_exists=if_exists,
                index=False,
            )
    elif if_exists == "append":
        logging.info(f"Appending to {schema_name}.{table_name}...")
        with connect_to_db() as engine:
            df.to_sql(
                table_name,
                con=engine,
                schema=schema_name,
                if_exists="append",
                index=False,
            )
    else:
        logging.info(
            f"Table {schema_name}.{table_name} already exists and if_exists='{if_exists}'. Skipping..."
        )


def df_to_db_upsert(
    schema_name: str,
    table_name: str,
    df: pd.DataFrame,
    conflict_columns: List[str] = None,
):
    """
    Upsert DataFrame to database (insert or update on conflict)

    Args:
        schema_name (str): Database schema name
        table_name (str): Database table name
        df (pd.DataFrame): DataFrame to upsert
        conflict_columns (list): Columns to check for conflicts. If None, will append.
    """
    schema_name = schema_name.lower()
    table_name = table_name.lower()

    # Create schema if it doesn't exist
    create_schema(schema_name)

    if not table_exists(schema_name, table_name) or conflict_columns is None:
        # If table doesn't exist or no conflict columns specified, just append
        df_to_db_replace(schema_name, table_name, df, if_exists="append")
        return

    logging.info(f"Upserting to {schema_name}.{table_name}...")

    with connect_to_db() as engine:
        # Create a temporary table
        temp_table = f"{table_name}_temp"
        df.to_sql(
            temp_table, con=engine, schema=schema_name, if_exists="replace", index=False
        )

        # Build the upsert query
        columns = df.columns.tolist()
        conflict_cols_str = ", ".join(conflict_columns)
        update_assignments = ", ".join(
            [
                f"{col} = EXCLUDED.{col}"
                for col in columns
                if col not in conflict_columns
            ]
        )

        upsert_query = f"""
        INSERT INTO {schema_name}.{table_name} ({", ".join(columns)})
        SELECT {", ".join(columns)} FROM {schema_name}.{temp_table}
        ON CONFLICT ({conflict_cols_str}) 
        DO UPDATE SET {update_assignments}
        """

        with engine.connect() as conn:
            conn.execute(text(upsert_query))
            conn.execute(text(f"DROP TABLE {schema_name}.{temp_table}"))
            conn.commit()


def db_to_df(query: str):
    """
    Executes a SQL query on the connected PostgreSQL database and returns the result as a pandas DataFrame.

    Args:
        query (str): A SQL query string to be executed.

    Returns:
        pd.DataFrame | None: A DataFrame containing the query results, or None if an error occurs.
    """
    try:
        with connect_to_db() as engine:
            logging.info(f"Executing SQL query: {query}")
            df = pd.read_sql_query(text(query), con=engine)
        return df
    except Exception as e:
        logging.error(f"Error pulling data from DB: {e}")
        return None


def gdf_to_postgis(
    schema_name: str,
    table_name: str,
    gdf: gpd.GeoDataFrame,
):
    """
    Writes a GeoPandas GeoDataFrame to a PostGIS-enabled PostgreSQL table within a specified schema.

    If the target table does not exist, it will be created and populated with the GeoDataFrame's data.
    The table will be replaced if it exists.

    Args:
        schema_name (str): Name of the schema in which to write the table.
        table_name (str): Name of the table to create or overwrite.
        gdf (gpd.GeoDataFrame): The GeoDataFrame to write to the database.

    Returns:
        None
    """
    schema_name = schema_name.lower()
    table_name = table_name.lower()
    if not table_exists(schema_name, table_name):
        logging.info(f"Writing to {schema_name}.{table_name}...")
        with connect_to_db() as engine:
            gdf.to_postgis(
                table_name, engine, schema=schema_name, if_exists="replace", index=False
            )


def postgis_to_gdf(
    schema_name: str,
    table_name: str,
    geom_col: str = "geometry",
    crs: str = "EPSG:4326",
    columns: Optional[List[str]] = None,
) -> Optional[gpd.GeoDataFrame]:
    """
    Reads a PostGIS table into a GeoPandas GeoDataFrame.

    Allows the user to specify columns to select, or selects all by default.

    Args:
        schema_name (str): Schema containing the PostGIS table.
        table_name (str): Table name to read.
        geom_col (str, optional): Geometry column name. Defaults to 'geometry'.
        crs (str, optional): Coordinate Reference System to assign. Defaults to 'EPSG:4326'.
        columns (list of str, optional): List of column names to fetch. If None, selects all columns.

    Returns:
        Optional[gpd.GeoDataFrame]: GeoDataFrame containing the table data, or None on error.
    """
    schema_name = schema_name.lower()
    table_name = table_name.lower()

    try:
        with connect_to_db() as engine:
            inspector = inspect(engine)
            if not inspector.has_table(table_name, schema=schema_name):
                logging.error(f"Table {schema_name}.{table_name} does not exist.")
                return None

            # Build column selection
            if columns is None:
                col_clause = "*"
            else:
                col_clause = ", ".join([f'"{col}"' for col in columns])

            sql = text(f'SELECT {col_clause} FROM "{schema_name}"."{table_name}"')
            gdf = gpd.read_postgis(sql, con=engine, geom_col=geom_col)
            gdf.set_crs(crs, inplace=True)
        return gdf

    except Exception as e:
        logging.error(f"Error reading table {schema_name}.{table_name}: {e}")
        return None


def table_exists(schema_name: str, table_name: str) -> bool:
    """
    Checks whether a given table exists in a specified schema of the connected PostgreSQL database.

    Converts both schema and table names to lowercase before checking.

    Args:
        schema_name (str): The name of the schema to search in.
        table_name (str): The name of the table to check for existence.

    Returns:
        bool: True if the table exists, False otherwise.
    """
    schema_name = schema_name.lower()
    table_name = table_name.lower()

    with connect_to_db() as engine:
        inspector = inspect(engine)

        # List of table names in the specified schema
        tables = inspector.get_table_names(schema=schema_name)

        if table_name in tables:
            logger.info(f"Table {schema_name}.{table_name} already exists. Skipping...")
        else:
            logger.info(f"Table {schema_name}.{table_name} does not exist.")
        return table_name in tables


def get_table_row_count(schema_name: str, table_name: str):
    """
    Get the number of rows in a table

    Args:
        schema_name (str): Database schema name
        table_name (str): Database table name

    Returns:
        int: Number of rows in the table, or 0 if table doesn't exist
    """

    schema_name = schema_name.lower()
    table_name = table_name.lower()

    if not table_exists(schema_name, table_name):
        logger.info(f"Table {schema_name}.{table_name} does not exist")
        return 0
    try:
        with connect_to_db() as engine:
            with engine.connect() as conn:
                result = conn.execute(
                    text(f"SELECT COUNT(1) FROM {schema_name}.{table_name}")
                )
                count = result.scalar()
                return count
    except Exception as e:
        logger.error(f"Error getting row count for {schema_name}.{table_name}: {e}")
        return 0


def osm_to_db(osm_file: str, schema: str):
    db_name = os.getenv("DB_NAME")
    db_user = os.getenv("DB_USER")
    db_password = os.getenv("DB_PASSWORD")
    db_host = os.getenv("DB_HOST")
    db_port = os.getenv("DB_PORT")
    mapconfig_path = (
        os.getenv("MAPCONFIG_PATH")
        or "/opt/homebrew/Cellar/osm2pgrouting/2.3.8_16/share/osm2pgrouting/mapconfig.xml"
    )

    if not all([db_name, db_user, db_password, db_host, db_port]):
        raise ValueError("One or more required DB environment variables are not set.")

    command = [
        "osm2pgrouting",
        "--file",
        osm_file,
        "--conf",
        mapconfig_path,
        "--dbname",
        db_name,
        "--username",
        db_user,
        "--host",
        db_host,
        "--port",
        db_port,
        "--schema",
        schema,
        "--chunk",
        "50000",
        "--clean",
    ]

    env = os.environ.copy()
    env["PGPASSWORD"] = db_password

    logger.info("Running osm2pgrouting...")
    try:
        subprocess.run(command, check=True, env=env)
        logger.info("OSM data import completed successfully.")
    except subprocess.CalledProcessError as e:
        logger.error(f"osm2pgrouting failed: {e}")
        raise


def create_table_with_indexes(
    engine,
    schema: str,
    table: str,
    sample_df: pd.DataFrame,
    json_col: str = "population_json",
):
    """
    Create a table for a DataFrame with JSON support.
    """
    try:
        dtype = {}
        if json_col in sample_df.columns:
            dtype[json_col] = JSONB

        sample_df.head(0).to_sql(
            name=table,
            con=engine,
            schema=schema,
            if_exists="fail",
            index=False,
            dtype=dtype,
        )
        logger.info(f"Created table {schema}.{table} successfully")

    except Exception as e:
        logger.error(f"Failed to create table structure: {e}")
        raise


def create_spatial_index(schema: str, table: str, geom_col: str = "geometry"):
    with connect_to_db() as engine:
        index_name = f"{table}_geom_idx"
        sql = f"""
        CREATE INDEX IF NOT EXISTS {index_name}
        ON {schema}.{table}
        USING GIST ({geom_col});
        """
        with engine.connect() as conn:
            conn.execute(text(sql))
        logger.info(f"Spatial index {index_name} created on {schema}.{table}")
