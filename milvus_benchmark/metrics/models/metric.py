import json
import hashlib
import datetime


class Metric:
    """
    {
        "_version": "0.1",
        "_type": "metric",
        "run_id": string,
        "server": string,
        "hardware": string,
        "env": string,
        "table": {
            "dimension": integer,
            "metric_type": string,
            "dataset_name": string
        },
        "index_type": string,
        "index_nlist": integer,
        "search": {
            "nq": integer,
            "nprobe": integer,
            "topk": integer
        },
        "metrics": {
            "cost": double,
            "precision": double,
        },
        "datetime": string
    }
    """

    def __init__(self):
        self._version = '0.1'
        self._type = 'metric'
        self.run_id = "todo"
        self.server = 'todo'
        self.hardware = 'todo'
        self.env = 'todo'
        self.table = {
            "dimension": None,
            "metric_type": None,
            "dataset_name": None
        }
        self.index = {}
        self.search = {}
        self.run_params = {}
        self.metrics = {
            "type": "search_performance", # "accuracy"/"build_performance"/"insert_performance"/"mix_stability"/"search_stability"
            "value": None,
        }
        self.datetime = str(datetime.datetime.now())

    def set_run_id(self, id_value):
        self.run_id = id_value

    def json_md5(self):
        json_str = json.dumps(vars(self), sort_keys=True)
        return hashlib.md5(json_str.encode('utf-8')).hexdigest()
