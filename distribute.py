from dataclasses import dataclass
import os
import psycopg2
from contextlib import contextmanager
from psycopg2.extras import execute_values
import logging
import re

##########################
# Database Configuration #
##########################


@dataclass(frozen=True, kw_only=True)
class DatabaseCfg:
    host: str
    name: str
    user: str
    password: str
    port: int


def read_environment_cfg() -> DatabaseCfg:
    return DatabaseCfg(
        host=os.getenv("DISTRIBUTE_DB_HOST"),
        name=os.getenv("DISTRIBUTE_DB_NAME"),
        user=os.getenv("DISTRIBUTE_DB_USER"),
        password=os.getenv("DISTRIBUTE_DB_PASSWORD"),
        port=int(os.getenv("DISTRIBUTE_DB_PORT")),
    )


@contextmanager
def get_db_connection(cfg: DatabaseCfg):
    """Context manager for database connections."""
    connection = psycopg2.connect(
        host=cfg.host,
        database=cfg.name,
        user=cfg.user,
        password=cfg.password,
        port=cfg.port,
        sslmode="require",
        gssencmode="disable",
    )

    try:
        yield connection
        connection.commit()
    except Exception:
        connection.rollback()
        raise
    finally:
        connection.close()


def validate(identifier: str) -> str:
    if not re.match(r"^[a-zA-Z_][a-zA-Z0-9_]*$", identifier):
        raise ValueError(
            f"The identifier {identifier} is not allowed. "
            "Please only use letters, numbers, and underscores."
        )
    return identifier


#################
# API Functions #
#################


def create_job(
    job_name: str,
    keys: list[str],
    cfg: DatabaseCfg = read_environment_cfg(),
) -> None:
    # Ensure that everything is fine to put in a SQL database.
    validate(job_name)
    [validate(key) for key in keys]

    logging.info(f"Creating distributed job {job_name}.")
    with get_db_connection(cfg) as connection:
        cursor = connection.cursor()
        logging.info(f"Creating tables for job {job_name}.")
        cursor.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {job_name} (
                key TEXT PRIMARY KEY,
                status TEXT DEFAULT 'pending',
                timestamp TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
                worker TEXT DEFAULT 'unassigned',
                result BYTEA DEFAULT NULL
            )
            """
        )
        cursor.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {job_name}_workers (
                worker TEXT PRIMARY KEY,
                heartbeat TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
                num_failures INT
            )
            """
        )
        logging.info(
            f"Inserting keys for job {job_name}. This can take a while with many keys."
        )
        execute_values(
            cursor,
            f"INSERT INTO {job_name} (key) VALUES %s ON CONFLICT (key) DO NOTHING",
            [(key,) for key in keys],
        )
        cursor.close()
