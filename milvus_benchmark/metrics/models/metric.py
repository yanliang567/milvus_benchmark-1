import time
import datetime
import json
import hashlib


class Metric(object):
    def __init__(self):
        self._version = '0.1'
        self._type = 'metric'
        self.run_id = None
        self.mode = None
        self.server = 'todo'
        self.hardware = 'todo'
        self.env = 'todo'
        self.status = "INIT"
        self.index = {}
        self.search = {}
        self.run_params = {}
        self.metrics = {
            "type": "",
            "value": None,
        }
        self.datetime = str(datetime.datetime.now())

    def set_run_id(self):
        self.run_id = int(time.time())

    def set_mode(self, mode):
        self.mode = mode

    def json_md5(self):
        json_str = json.dumps(vars(self), sort_keys=True)
        return hashlib.md5(json_str.encode('utf-8')).hexdigest()

    def update(self, status):
        self.status = status
