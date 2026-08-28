# `distribute`

This package makes it easy to execute parallel tasks on many workers.

## Installation

This project uses [uv](https://docs.astral.sh/uv/). All dependencies are declared in
`pyproject.toml` and pinned in `uv.lock`.

```bash
uv sync
```

That creates `.venv` with the runtime dependencies plus the `dev` group (`beartype`,
`ruff`) and the `dashboard` extra. Use `uv run <script>` to run anything in it, e.g.
`uv run example.py`. For a runtime-only install (no dev tooling or dashboard), use
`uv sync --no-default-groups`.

To add or remove a dependency, use `uv add <package>` / `uv remove <package>` rather
than editing `pyproject.toml` by hand, so that `uv.lock` stays in sync.

## Usage

It is extremely simple to use. First, define a job (collection of tasks):

```python
from distribute import create_job

create_job("test_job", [f"key_{x}" for x in range(100)])
```

Then, in your worker script (separate process, Slurm job, etc.), call `execute_tasks` with a `task_fn` that executes the task.

```python
from io import BytesIO
from distribute import execute_tasks

def task_fn(key: str, result: BytesIO) -> None:
    # Do whatever your task is. This function must be idempotent!
    print(f"working on task {key}")

execute_tasks("test_job", task_fn)
```

To see this in action, run `uv run example.py`.

## Fault Tolerance

This package assumes that workers may be killed without notice and is designed accordingly. Each worker uses `SIGALRM` to update the PSQL database with a "heartbeat" at a fixed interval. If other workers notice that a task is being executed by a worker that has not been sending regular heartbeats, they will assume that the worker has died and claim its tasks. If the worker was somehow frozen (and not dead) and becomes un-frozen, the code will detect this and mark the task as corrupted.

## New Project Setup

It is recommended to create a new PSQL database and user for each project you use this package with. To create them:

```bash
sudo -u postgres createdb testing
sudo -u postgres psql -c "CREATE USER testing WITH PASSWORD 'TUW2XXIV0kFRyWp3';"
sudo -u postgres psql -c "GRANT ALL PRIVILEGES ON DATABASE testing TO testing;"
ALTER DATABASE testing SET idle_in_transaction_session_timeout = '30s';
```

You will have to update `/etc/postgresql/14/main/pg_hba.conf` to allow access to this database from anywhere on the internet:

```
hostssl testing testing 0.0.0.0/0 scram-sha-256
```

Finally, you will have to reload the psql service:

```bash
sudo systemctl reload postgresql
```