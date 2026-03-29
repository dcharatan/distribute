from beartype.claw import beartyping

with beartyping():
    from distribute import create_job, make_random_tag


if __name__ == "__main__":
    name = f"test_{make_random_tag()}"
    create_job(name, [f"key_{x}" for x in range(100)])
    print(f"Job name: {name}")
