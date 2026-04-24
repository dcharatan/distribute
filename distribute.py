import logging
import os
import re
import secrets
import signal
import socket
import string
import traceback
from contextlib import contextmanager
from dataclasses import dataclass
from io import BytesIO
from typing import Any, Generator, Protocol, runtime_checkable

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


@backoff.on_exception(backoff.expo, BackoffException, factor=0.1, max_value=60.0)
def claim_task(
    job_name: str,
    worker_name: str,
    worker_timeout_seconds: int,
    cfg: DatabaseCfg,
    soft: bool = False,
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
            """
        )
        row = cursor.fetchone()

        # If no such key was found, exit.
        if row is None:
            logging.info("No tasks found. Checking for completion.")

            # If all tasks are complete, return None.
            if soft or all_tasks_complete(job_name, cfg):
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


@backoff.on_exception(backoff.expo, BackoffException, factor=5.0, max_value=120.0)
def claim_tasks(
    job_name: str,
    worker_name: str,
    worker_timeout_seconds: int,
    cfg: DatabaseCfg,
    num_tasks: int,
) -> tuple[str, ...] | None:
    # Look for available tasks.
    keys = []
    for _ in range(num_tasks):
        key = claim_task(job_name, worker_name, worker_timeout_seconds, cfg, soft=True)
        if key is None:
            break
        keys.append(key)

    # If tasks were found, return them.
    if keys:
        return tuple(keys)

    # If no tasks were found, check for completion. If tasks remain, wait.
    if not all_tasks_complete(job_name, cfg):
        raise BackoffException()

    # On completion, return None.
    return None


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
    validate(job_name)

    logging.info(f"Creating distributed job {job_name}")
    with get_cursor(cfg) as cursor:
        logging.info(f"Creating tables for job {job_name}")
        cursor.execute(
            f"""
            CREATE TABLE {job_name} (
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
            CREATE TABLE {job_name}_workers (
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
class TaskFn(Protocol):
    def __call__(self, key: str, result: BytesIO) -> None:
        """Process the specified key and write the result to the provided BytesIO."""
        pass


@runtime_checkable
class BatchedTaskFn(Protocol):
    def __call__(self, key: tuple[str, ...], result: tuple[BytesIO, ...]) -> None:
        """Process the specified keys and write the result to the provided BytesIOs."""
        pass


def execute_tasks(
    job_name: str,
    task_fn: TaskFn,
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
            task_fn(key, result)
        except Exception:
            traceback.print_exception()
            mark_task_failed(job_name, worker_name, key, cfg)
            continue

        mark_task_done(job_name, worker_name, key, result.getvalue(), cfg)


def execute_tasks_batched(
    job_name: str,
    task_fn: BatchedTaskFn,
    batch_size: int,
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
        # Claim tasks.
        keys = claim_tasks(
            job_name,
            worker_name,
            worker_timeout_seconds,
            cfg,
            batch_size,
        )
        if keys is None:
            # All tasks are done, so exit!
            return

        # Execute the task.
        try:
            result = [BytesIO() for _ in keys]
            task_fn(tuple(keys), tuple(result))
        except Exception:
            [mark_task_failed(job_name, worker_name, key, cfg) for key in keys]
            continue

        [
            mark_task_done(job_name, worker_name, key, result.getvalue(), cfg)
            for key, result in zip(keys, result)
        ]


def iterate_results(
    job_name: str,
    cfg: DatabaseCfg = read_environment_cfg(),
) -> Generator[tuple[str, BytesIO], None, None]:
    """Iterate over completed results in the database."""
    with get_cursor(cfg) as cursor:
        cursor.execute(
            f"""
            SELECT key, result FROM {job_name} 
            WHERE status = 'done'
            """
        )
        rows = cursor.fetchall()
        for key, result_bytes in rows:
            yield key, BytesIO(result_bytes)
