import logging
import os
from io import BytesIO
from time import sleep

from beartype.claw import beartyping

with beartyping():
    from distribute import do_work


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="[%(levelname)s] %(asctime)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    def work_fn(key: str, result: BytesIO) -> None:
        # A dummy task.
        sleep(0.1)

    do_work(os.getenv("JOB_NAME"), work_fn)
