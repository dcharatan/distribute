import logging
import os
import re
import secrets
import signal
import socket
import string
from contextlib import contextmanager
from dataclasses import dataclass
from io import BytesIO
from typing import Any, Protocol, runtime_checkable

import psycopg2
from psycopg2.extras import execute_values

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


####################
# Helper Functions #
####################


def make_random_tag() -> str:
    return "".join(secrets.choice(string.ascii_lowercase) for _ in range(8))


#####################
# Worker Management #
#####################


def register_worker(job_name: str, worker_name: str, cfg: DatabaseCfg) -> None:
    with get_db_connection(cfg) as connection:
        cursor = connection.cursor()
        cursor.execute(
            f"""
            INSERT INTO {job_name}_workers (worker, heartbeat, num_failures)
            VALUES (%s, CURRENT_TIMESTAMP, 0)
            """,
            (worker_name,),
        )
        cursor.close()


def start_heartbeat(
    job_name: str,
    worker_name: str,
    interval_seconds: int,
    cfg: DatabaseCfg,
) -> None:
    def heartbeat():
        with get_db_connection(cfg) as connection:
            cursor = connection.cursor()
            cursor.execute(
                f"""
                UPDATE {job_name}_workers
                SET heartbeat = CURRENT_TIMESTAMP
                WHERE worker = %s
                """,
                (worker_name,),
            )
            cursor.close()

    def handler(signum: int, frame: Any) -> None:
        signal.alarm(interval_seconds)
        heartbeat()

    signal.signal(signal.SIGALRM, handler)
    signal.alarm(interval_seconds)


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


@runtime_checkable
class WorkFn(Protocol):
    def __call__(self, key: str, result: BytesIO) -> None:
        """Process the specified key and write the result to the provided BytesIO."""
        pass


def do_work(
    job_name: str,
    work_fn: WorkFn,
    cfg: DatabaseCfg = read_environment_cfg(),
    worker_heartbeat_seconds: int = 60,
) -> None:
    # Determine
    worker_name = socket.gethostname() + make_random_tag()
    register_worker(job_name, worker_name, cfg)
    start_heartbeat(job_name, worker_name, worker_heartbeat_seconds, cfg)

    while True:
        import time

        time.sleep(1)

    a = 1
