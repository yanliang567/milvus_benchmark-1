import os
import threading
import datetime
import time

from milvus_benchmark.metrics.client_influx_db import ClientInfluxDB
from logs.log import global_params
from milvus_benchmark.utils import read_json_file


def insert_influx_db(field, tag=None, out_put=True):
    config_dict = read_json_file(global_params.config_path)
    if "influx_db" in config_dict:
        params = config_dict["influx_db"]
    else:
        raise print("[insert_influx_db] Can not get keys of influxdb.")
    c = ClientInfluxDB(**params)

    measurement_name = "locust"
    tag = tag

    c.insert(measurement_name=measurement_name, tag=tag, field=field)
    # c.delete(time={"days": 1})
    # res = c.query(measurement_name=measurement_name, out_put=False)
    # print(len(res))


class DataReport:
    def __init__(self):
        self.data_path = global_params.locust_report_path
        self.parser = DataParser(self.data_path)

    def init_report(self):
        pass

    def time_tick(self):
        pass

    def final_report(self):
        pass


def locust_data(Method, api, reqs, fails, Avg, Min, Max, Median, rps, failures):
    return {'Method': Method, 'api_name': api}, {'reqs': reqs, 'fails': fails, 'average_ms': Avg, 'min_ms': Min,
                                                 'max_ms': Max, 'median_ms': Median, 'current_rps': rps,
                                                 'failures_s': failures}


def data_parser(data):
    for i in data.split('\n'):
        if "locust.stats_logger:730" in i:
            j = i.split('-')[-1]
            k = j.split()
            tag, data_ = locust_data(k[0], k[1], int(k[2]), k[3], int(k[5]), int(k[6]),
                                     int(k[7]), int(k[8]), float(k[10]), float(k[11]))
            if global_params.metric is not None:
                tag.update({"run_id": int(global_params.metric.run_id)})
            insert_influx_db(data_, tag)


class DataParser:
    def __init__(self, file_path, read_flag=True):
        self.file_path = file_path
        self.read_flag = read_flag

    def read_file(self, file_path=None):
        file_path = self.file_path if file_path is None else file_path

        with open(file_path) as fd:
            file_content = fd.read()
            data_parser(file_content)
            fd.close()

        # os.remove(file_path)

    # def read_file(self, file_path=None, time_sleep=5):
    #     file_path = self.file_path if file_path is None else file_path
    #
    #     global last_position
    #     last_position = 0
    #
    #     with open(file_path) as fd:
    #         file_content = fd.read()
    #         data_parser(file_content)
    #         while self.read_flag:
    #             current_position = fd.tell()  # 记录文件当前位置
    #
    #             if current_position != last_position:  # 如果两者相等，说明没有新增文件
    #                 fd.seek(current_position, 0)
    #
    #             incremental_content = fd.read()  # 读取增量内容
    #             last_position = current_position  # 移动指针到当前位置
    #
    #             data_parser(incremental_content)
    #             time.sleep(time_sleep)
    #
    #             if self.read_flag is False:
    #                 break
    #
    #         fd.close()
    #
    #     return last_position, incremental_content

    # def read_file(self, last_position, file_path=None, time_sleep=5):
    #     file_path = self.file_path if file_path is None else file_path
    #
    #     # global last_position
    #     # last_position = 0
    #     time.sleep(time_sleep)
    #
    #     with open(file_path) as fd:
    #         file_content = fd.read()
    #         self.data_parser(file_content)
    #         current_position = fd.tell()  # 记录文件当前位置
    #         print("current_position1:")
    #         print(current_position)
    #         if current_position == last_position:  # 如果两者相等，说明没有新增文件
    #             pass
    #         else:
    #             fd.seek(current_position, 0)
    #             print("current_position2:")
    #             print(current_position)
    #         incremental_content = fd.read()  # 读取增量内容
    #         last_position = current_position  # 移动指针到当前位置
    #
    #         self.data_parser(incremental_content)
    #
    #         fd.close()
    #
    #         return last_position, incremental_content
    #
    # def start_read_file(self, file_path=None):
    #     file_path = self.file_path if file_path is None else file_path
    #     with open(file_path) as fd:
    #         file_content = fd.read()
    #         current_position = fd.tell()  # 记录文件当前位置
    #         print("start_read_file:")
    #         print(current_position)
    #         fd.close()
    #
    #     return current_position, file_content
