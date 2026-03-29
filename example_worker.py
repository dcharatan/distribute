import logging
import os
import random
from io import BytesIO
from json import dumps
from time import sleep

from beartype.claw import beartyping

with beartyping():
    from distribute import execute_tasks


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="[%(levelname)s] %(asctime)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    def task_fn(key: str, result: BytesIO) -> None:
        if random.random() < 0.1:
            raise Exception("Task failed!")
        sleep(0.1)
        result.write(dumps({"key": 1}).encode())

    execute_tasks(os.getenv("JOB_NAME"), task_fn)
