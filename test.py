import os



RUN_EXPIRATION_JOB = os.environ.get("RUN_EXPIRATION_JOB", 1)


if RUN_EXPIRATION_JOB:
    print("hi")
