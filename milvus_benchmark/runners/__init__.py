from .insert import InsertRunner
from .locust import LocustRunner
from .search import SearchRunner
from .search import InsertSearchRunner
from .build import InsertBuildRunner
from .build import BuildRunner


def get_runner(name, env, metric):
    return {
        "insert_performance": InsertRunner(env, metric),
        "search_performance": SearchRunner(env, metric),
        "insert_search_performance": InsertSearchRunner(env, metric),
        "insert_build_performance": InsertBuildRunner(env, metric),
        "build_performance": BuildRunner(env, metric),
        "locust_performance": LocustRunner(env, metric),
    }.get(name)
