import secrets
import string

from beartype.claw import beartyping

with beartyping():
    from distribute import create_job


def random_tag() -> str:
    return "".join(secrets.choice(string.ascii_lowercase) for _ in range(8))


if __name__ == "__main__":
    name = f"test_{random_tag()}"
    create_job(name, [f"key_{x}" for x in range(100)])
    print(f"Job name: {name}")
