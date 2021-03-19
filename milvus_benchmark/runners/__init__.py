from .insert import InsertRunner
from .locust import LocustInsertRunner
from .search import SearchRunner
from .search import InsertSearchRunner
from .build import InsertBuildRunner
from .build import BuildRunner
from .accuracy import AccuracyRunner


def get_runner(name, env, metric):
    return {
        "insert_performance": InsertRunner(env, metric),
        "search_performance": SearchRunner(env, metric),
        "insert_search_performance": InsertSearchRunner(env, metric),
        "locust_insert_performance": LocustInsertRunner(env, metric),
        "insert_build_performance": InsertBuildRunner(env, metric),
        "build_performance": BuildRunner(env, metric),
        "accuracy": AccuracyRunner(env, metric)
    }.get(name)
