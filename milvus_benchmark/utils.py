# -*- coding: utf-8 -*-
import time
import logging
import string
import random
import os
import json
from yaml import full_load, dump
import yaml
import tableprint as tp
from pprint import pprint
import config

logger = logging.getLogger("milvus_benchmark.utils")


def timestr_to_int(time_str):
    time_int = 0
    if isinstance(time_str, int) or time_str.isdigit():
        time_int = int(time_str)
    elif time_str.endswith("s"):
        time_int = int(time_str.split("s")[0])
    elif time_str.endswith("m"):
        time_int = int(time_str.split("m")[0]) * 60
    elif time_str.endswith("h"):
        time_int = int(time_str.split("h")[0]) * 60 * 60
    else:
        raise Exception("%s not support" % time_str)
    return time_int


class literal_str(str): pass


def change_style(style, representer):
    def new_representer(dumper, data):
        scalar = representer(dumper, data)
        scalar.style = style
        return scalar

    return new_representer


from yaml.representer import SafeRepresenter

# represent_str does handle some corner cases, so use that
# instead of calling represent_scalar directly
represent_literal_str = change_style('|', SafeRepresenter.represent_str)

yaml.add_representer(literal_str, represent_literal_str)


def retry(times):
    """
    This decorator prints the execution time for the decorated function.
    """
    def wrapper(func):
        def newfn(*args, **kwargs):
            attempt = 0
            while attempt < times:
                try:
                    result = func(*args, **kwargs)
                    if result:
                        break
                    else:
                        raise Exception("Result false")
                except Exception as e:
                    logger.info(str(e))
                    time.sleep(3)
                    attempt += 1
            return result
        return newfn
    return wrapper


def convert_nested(dct):
    def insert(dct, lst):
        for x in lst[:-2]:
            dct[x] = dct = dct.get(x, dict())
        dct.update({lst[-2]: lst[-1]})

        # empty dict to store the result

    result = dict()

    # create an iterator of lists  
    # representing nested or hierarchial flow 
    lsts = ([*k.split("."), v] for k, v in dct.items())

    # insert each list into the result 
    for lst in lsts:
        insert(result, lst)
    return result


def get_unique_name(prefix=None):
    if prefix is None:
        prefix = "distributed-benchmark-test-"
    return prefix + "".join(random.choice(string.ascii_letters + string.digits) for _ in range(8)).lower()


def get_current_time():
    return time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())


def print_table(headers, columns, data):
    bodys = []
    for index, value in enumerate(columns):
        tmp = [value]
        tmp.extend(data[index])
        bodys.append(tmp)
    tp.table(bodys, headers)


def get_deploy_mode(deploy_params):
    deploy_mode = None
    if deploy_params:
        milvus_params = None
        if "milvus" in deploy_params:
            milvus_params = deploy_params["milvus"]
        if not milvus_params:
            deploy_mode = config.DEFUALT_DEPLOY_MODE
        elif "deploy_mode" in milvus_params:
            deploy_mode = milvus_params["deploy_mode"]
            if deploy_mode not in [config.SINGLE_DEPLOY_MODE, config.CLUSTER_DEPLOY_MODE, config.CLUSTER_3RD_DEPLOY_MODE]:
                raise Exception("Invalid deploy mode: %s" % deploy_mode)
    return deploy_mode


def get_server_tag(deploy_params):
    """
    Get service deployment configuration
    e.g.:
        server:
          server_tag: "8c16m"
    """
    server_tag = ""
    if deploy_params and "server" in deploy_params:
        server = deploy_params["server"]
        # server_name = server["server_name"] if "server_name" in server else ""
        server_tag = server["server_tag"] if "server_tag" in server else ""
    return server_tag


def get_server_resource(deploy_params):
    server_resource = {}
    if deploy_params and "server_resource" in deploy_params:
        server_resource = deploy_params["server_resource"]
    return server_resource


def dict_add(source, target):
    for key, value in source.items():
        if isinstance(value, dict) and key in target:
            dict_add(source[key], target[key])
        else:
            target[key] = value
    return target


def update_dict_value(server_resource, values_dict):
    if not isinstance(server_resource, dict) or not isinstance(values_dict, dict):
        return values_dict

    target = dict_add(server_resource, values_dict)

    return target


def search_param_analysis(vector_query, filter_query):

    if "vector" in vector_query:
        vector = vector_query["vector"]
    else:
        logger.debug("[search_param_analysis] vector not in vector_query")
        return False

    data = []
    anns_field = ""
    param = {}
    limit = 1
    if isinstance(vector, dict) and len(vector) == 1:
        for key in vector:
            anns_field = key
            data = vector[key]["query"]
            param = {"metric_type": vector[key]["metric_type"],
                     "params": vector[key]["params"]}
            limit = vector[key]["topk"]
    else:
        logger.debug("[search_param_analysis] vector not dict or len != 1: %s" % str(vector))
        return False

    if isinstance(filter_query, list) and len(filter_query) != 0 and "range" in filter_query[0]:
        filter_range = filter_query[0]["range"]
        if isinstance(filter_range, dict) and len(filter_range) == 1:
            for key in filter_range:
                field_name = key
                expression = None
                if 'GT' in filter_range[key]:
                    exp1 = "%s > %s" % (field_name, str(filter_range[key]['GT']))
                    expression = exp1
                if 'LT' in filter_range[key]:
                    exp2 = "%s < %s" % (field_name, str(filter_range[key]['LT']))
                    if expression:
                        expression = expression + ' && ' + exp2
                    else:
                        expression = exp2

        else:
            logger.debug("[search_param_analysis] filter_range not dict or len != 1: %s" % str(filter_range))
            return False
    else:
        # logger.debug("[search_param_analysis] range not in filter_query: %s" % str(filter_query))
        expression = None

    result = {
        "data": data,
        "anns_field": anns_field,
        "param": param,
        "limit": limit,
        "expression": expression
    }
    # logger.debug("[search_param_analysis] search_param_analysis: %s" % str(result))
    return result


def modify_file(file_path_list, is_modify=False, input_content=""):
    """
    file_path_list : file list -> list[<file_path>]
    is_modify : does the file need to be reset
    input_content ：the content that need to insert to the file
    """
    if not isinstance(file_path_list, list):
        print("[modify_file] file is not a list.")

    for file_path in file_path_list:
        folder_path, file_name = os.path.split(file_path)
        if not os.path.isdir(folder_path):
            print("[modify_file] folder(%s) is not exist." % folder_path)
            os.makedirs(folder_path)

        if not os.path.isfile(file_path):
            print("[modify_file] file(%s) is not exist." % file_path)
            os.mknod(file_path)
        else:
            if is_modify is True:
                print("[modify_file] start modifying file(%s)..." % file_path)
                with open(file_path, "r+") as f:
                    f.seek(0)
                    f.truncate()
                    f.write(input_content)
                    f.close()
                print("[modify_file] file(%s) modification is complete." % file_path_list)


def read_json_file(file_name):
    with open(file_name) as f:
        file_dict = json.load(f)
    return file_dict
