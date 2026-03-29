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

import backoff
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
def get_cursor(cfg: DatabaseCfg):
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
    cursor = connection.cursor()
    try:
        yield cursor
        connection.commit()
    except Exception:
        connection.rollback()
        raise
    finally:
        cursor.close()
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
    with get_cursor(cfg) as cursor:
        cursor.execute(
            f"""
            INSERT INTO {job_name}_workers (worker)
            VALUES (%s)
            """,
            (worker_name,),
        )


def start_heartbeat(
    job_name: str,
    worker_name: str,
    interval_seconds: int,
    cfg: DatabaseCfg,
) -> None:
    def heartbeat():
        with get_cursor(cfg) as cursor:
            cursor.execute(
                f"""
                UPDATE {job_name}_workers
                SET heartbeat = CURRENT_TIMESTAMP
                WHERE worker = %s
                """,
                (worker_name,),
            )

    def handler(signum: int, frame: Any) -> None:
        signal.alarm(interval_seconds)
        heartbeat()

    signal.signal(signal.SIGALRM, handler)
    signal.alarm(interval_seconds)


##############
# Task Queue #
##############


class BackoffException(Exception):
    pass


@backoff.on_exception(backoff.expo, BackoffException, factor=0.1)
def claim_task(
    job_name: str,
    worker_name: str,
    worker_timeout_seconds: int,
    cfg: DatabaseCfg,
) -> str | None:
    with get_cursor(cfg) as cursor:
        # Claim a pending key that has either:
        # (1) never been assigned to a worker
        # (2) been assigned to a worker whose last heartbeat was long ago
        cursor.execute(
            f"""
            SELECT t.key FROM {job_name} t
            LEFT JOIN {job_name}_workers w ON t.worker = w.worker
            WHERE t.status IN ('pending', 'processing')
            AND (
                t.worker = 'unassigned'
                OR w.heartbeat < NOW() - INTERVAL '{worker_timeout_seconds} seconds'
            )
            LIMIT 1
            FOR UPDATE OF t SKIP LOCKED
            """
        )
        row = cursor.fetchone()

        # If no such key was found, exit.
        if row is None:
            logging.info("No tasks found. Checking for completion.")

            # If all tasks are complete, return None.
            if all_tasks_complete(job_name, cfg):
                return None

            # If tasks remain, we're waiting for other nodes to finish or time out.
            raise BackoffException()

        # Mark the item as in progress.
        (key,) = row
        cursor.execute(
            f"""
            UPDATE {job_name} t
            SET status = 'processing', timestamp = CURRENT_TIMESTAMP, worker = %s
            FROM {job_name}_workers w
            WHERE t.key = %s
            AND t.status IN ('pending', 'processing')
            AND (
                t.worker = 'unassigned'
                OR w.worker = t.worker AND w.heartbeat < NOW() - INTERVAL '{worker_timeout_seconds} seconds'
            )
            """,  # noqa: E501
            (worker_name, key),
        )

        # If another worked claimed the key, try again.
        if cursor.rowcount == 0:
            logging.info(f"Skipping {key} since it was already claimed.")
            raise BackoffException()

        return key


def all_tasks_complete(job_name: str, cfg: DatabaseCfg) -> bool:
    with get_cursor(cfg) as cursor:
        cursor.execute(
            f"""
            SELECT COUNT(*) FROM {job_name}
            WHERE status NOT IN ('done', 'corrupted')
            """
        )
        (num_not_done,) = cursor.fetchone()
        return num_not_done == 0


def mark_task_failed(
    job_name: str,
    worker_name: str,
    key: str,
    cfg: DatabaseCfg,
) -> None:
    logging.info(f"Marking task {key} as failed.")
    with get_cursor(cfg) as cursor:
        cursor.execute(
            f"""
            UPDATE {job_name}
            SET status = 'pending', worker = 'unassigned', num_failures = num_failures + 1
            WHERE key = %s AND worker = %s AND status = 'processing'
            """,  # noqa: E501
            (key, worker_name),
        )
        if cursor.rowcount == 0:
            cursor.execute(
                f"UPDATE {job_name} SET status = 'corrupted' WHERE key = %s",
                (key,),
            )
        else:
            cursor.execute(
                f"""
                UPDATE {job_name}_workers
                SET num_failures = num_failures + 1
                WHERE worker = %s
                """,
                (worker_name,),
            )


def mark_task_done(
    job_name: str,
    worker_name: str,
    key: str,
    result: bytes,
    cfg: DatabaseCfg,
) -> None:
    logging.info(f"Marking task {key} as done.")
    with get_cursor(cfg) as cursor:
        cursor.execute(
            f"""
            UPDATE {job_name}
            SET status = 'done', timestamp = CURRENT_TIMESTAMP, result = %s
            WHERE key = %s AND worker = %s AND status = 'processing'
            """,
            (psycopg2.Binary(result), key, worker_name),
        )
        if cursor.rowcount == 0:
            cursor.execute(
                f"UPDATE {job_name} SET status = 'corrupted' WHERE key = %s",
                (key,),
            )
        else:
            cursor.execute(
                f"""
                UPDATE {job_name}_workers
                SET num_done = num_done + 1
                WHERE worker = %s
                """,
                (worker_name,),
            )


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

    logging.info(f"Creating distributed job {job_name}")
    with get_cursor(cfg) as cursor:
        logging.info(f"Creating tables for job {job_name}")
        cursor.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {job_name} (
                key TEXT PRIMARY KEY,
                status TEXT DEFAULT 'pending',
                num_failures INT DEFAULT 0,
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
                num_failures INT DEFAULT 0,
                num_done INT DEFAULT 0
            )
            """
        )
        logging.info(
            f"Inserting keys for job {job_name} (this can take a while with many keys)."
        )
        execute_values(
            cursor,
            f"INSERT INTO {job_name} (key) VALUES %s ON CONFLICT (key) DO NOTHING",
            [(key,) for key in keys],
        )


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
    worker_timeout_seconds: int = 300,
) -> None:
    assert worker_timeout_seconds > worker_heartbeat_seconds

    # Register the worker.
    worker_name = f"{socket.gethostname()}_{make_random_tag()}"
    logging.info(f"Worker name: {worker_name}")
    register_worker(job_name, worker_name, cfg)
    start_heartbeat(job_name, worker_name, worker_heartbeat_seconds, cfg)

    while True:
        # Claim a task.
        key = claim_task(job_name, worker_name, worker_timeout_seconds, cfg)
        if key is None:
            # All tasks are done, so exit!
            return

        # Execute the task.
        try:
            result = BytesIO()
            work_fn(key, result)
        except Exception:
            mark_task_failed(job_name, worker_name, key, cfg)
            continue

        mark_task_done(job_name, worker_name, key, result.getvalue(), cfg)
