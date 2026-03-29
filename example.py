from beartype.claw import beartyping

with beartyping():
    from distribute import create_job, make_random_tag

import logging
import os
import subprocess
import sys
from pathlib import Path

WORKER_SCRIPT = Path(__file__).parent / "example_worker.py"
NUM_WORKERS = 100


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="[%(levelname)s] %(asctime)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    job_name = f"test_{make_random_tag()}"
    create_job(job_name, [f"key_{x}" for x in range(10000)])
    print(f"Job name: {job_name}")

    processes = [
        subprocess.Popen(
            [sys.executable, str(WORKER_SCRIPT)],
            env={**os.environ, "JOB_NAME": job_name},
        )
        for _ in range(NUM_WORKERS)
    ]
    try:
        [p.wait() for p in processes]
    except KeyboardInterrupt:
        for p in processes:
            p.terminate()
        for p in processes:
            p.wait()
