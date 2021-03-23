from .insert import InsertRunner
from .locust import LocustInsertRunner, LocustSearchRunner, LocustRandomRunner
from .search import SearchRunner, InsertSearchRunner
from .build import BuildRunner, InsertBuildRunner
from .accuracy import AccuracyRunner
from .accuracy import AccAccuracyRunner


def get_runner(name, env, metric):
    return {
        "insert_performance": InsertRunner(env, metric),
        "search_performance": SearchRunner(env, metric),
        "insert_search_performance": InsertSearchRunner(env, metric),
        "locust_insert_performance": LocustInsertRunner(env, metric),
        "locust_search_performance": LocustSearchRunner(env, metric),
        "locust_random_performance": LocustRandomRunner(env, metric),
        "insert_build_performance": InsertBuildRunner(env, metric),
        "build_performance": BuildRunner(env, metric),
        "accuracy": AccuracyRunner(env, metric),
        "ann_accuracy": AccAccuracyRunner(env, metric)
    }.get(name)
