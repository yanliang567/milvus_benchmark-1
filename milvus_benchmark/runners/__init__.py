from .insert import InsertRunner
from .locust import LocustRunner


def get_runner(name, env, metric):
    return {
        "InsertRunner": InsertRunner(env, metric),
        "LocustRunner": LocustRunner(env, metric),
    }.get(name)
