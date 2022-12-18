import os

NEO4J_URL = os.environ.get("NEO4J_URL", "bolt://0.0.0.0:7687")


def get(name):
    return globals().get(name)
