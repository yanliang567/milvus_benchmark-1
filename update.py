import os
import sys
import time
import re
import logging
import traceback
import argparse
from yaml import full_load, dump


DEFUALT_DEPLOY_MODE = "single"
IDC_NAS_URL = "//172.16.70.249/test"

def parse_server_tag(server_tag):
    # tag format: "8c"/"8c16m"/"8c16m1g"
    if server_tag[-1] == "c":
        p = r"(\d+)c"
    elif server_tag[-1] == "m":
        p = r"(\d+)c(\d+)m"
    elif server_tag[-1] == "g":
        p = r"(\d+)c(\d+)m(\d+)g"
    m = re.match(p, server_tag)
    cpus = int(m.groups()[0])
    mems = None
    gpus = None
    if len(m.groups()) > 1:
        mems = int(m.groups()[1])
    if len(m.groups()) > 2:
        gpus = int(m.groups()[2])
    return {"cpus": cpus, "mems": mems, "gpus": gpus}


"""
description: update values.yaml
return: no return
"""
def update_values(src_values_file, deploy_params_file):
    # deploy_mode, hostname, server_tag, milvus_config, server_config=None
    try:
        with open(src_values_file) as f:
            values_dict = full_load(f)
            f.close()
        with open(deploy_params_file) as f:
            deploy_params = full_load(f)
            f.close()
    except Exception as e:
        logging.error(str(e))
        raise Exception("File not found")
    deploy_mode = deploy_params["deploy_mode"] if "deploy_mode" in deploy_params else DEFUALT_DEPLOY_MODE
    cluster = False
    if deploy_mode != DEFUALT_DEPLOY_MODE:
        cluster = True

    if "server" in deploy_params:
        server = deploy_params["server"]
        server_name = server["server_name"] if "server_name" in server else ""
        server_tag = server["server_tag"] if "server_tag" in server else ""
    else:
        raise Exception("No server specified in {}".format(deploy_params_file))
    if "milvus" in deploy_params:
        milvus_config = deploy_params["milvus"]
        for k, v in milvus_config.items():
            if k.find("primary_path") != -1:
                suffix_path = milvus_config["suffix_path"] if "suffix_path" in milvus_config else None
                path_value = v
                if suffix_path:
                    path_value = v + "_" + str(int(time.time()))
                values_dict["primaryPath"] = path_value 
                values_dict['wal']['path'] = path_value+"/wal"
                values_dict['logs']['path'] = path_value+"/logs"
            # elif k.find("use_blas_threshold") != -1:
            #     values_dict['useBLASThreshold'] = int(v)
            elif k.find("gpu_search_threshold") != -1:
                values_dict['gpu']['gpuSearchThreshold'] = int(v)
            elif k.find("cpu_cache_capacity") != -1:
                values_dict['cache']['cacheSize'] = v
            # elif k.find("cache_insert_data") != -1:
            #     values_dict['cache']['cacheInsertData'] = v
            elif k.find("insert_buffer_size") != -1:
                values_dict['cache']['insertBufferSize'] = v
            elif k.find("gpu_resource_config.enable") != -1:
                values_dict['gpu']['enabled'] = v
            elif k.find("gpu_resource_config.cache_capacity") != -1:
                values_dict['gpu']['cacheSize'] = v
            elif k.find("build_index_resources") != -1:
                values_dict['gpu']['buildIndexDevices'] = v
            elif k.find("search_resources") != -1:
                values_dict['gpu']['searchDevices'] = v
            # wal
            elif k.find("auto_flush_interval") != -1:
                values_dict['storage']['autoFlushInterval'] = v
            elif k.find("wal_enable") != -1:
                values_dict['wal']['enabled'] = v

        # if values_dict['nodeSelector']:
        #     logger.warning("nodeSelector has been set: %s" % str(values_dict['engine']['nodeSelector']))
        #     return
        values_dict["wal"]["recoveryErrorIgnore"] = True
        # enable monitor
        # values_dict["metrics"]["enabled"] = False
        # values_dict["metrics"]["address"] = "192.168.1.237"
        # values_dict["metrics"]["port"] = 9091
        # Using sqlite for single mode
        if cluster is False:
            values_dict["mysql"]["enabled"] = False

    # # update values.yaml with the given host
    node_config = None
    perf_tolerations = [{
            "key": "node-role.kubernetes.io/benchmark",
            "operator": "Exists",
            "effect": "NoSchedule"
        }]  
    # if server_name:
    #     node_config = {'kubernetes.io/hostname': server_name}
    # elif server_tag:
    #     # server tag
    #     node_config = {'instance-type': server_tag}
    cpus = None
    mems = None
    gpus = None
    if server_tag:
        res = parse_server_tag(server_tag)
        cpus = res["cpus"]
        mems = res["mems"]
        gpus = res["gpus"]
    if cpus:
        resources = {
                "limits": {
                    "cpu": str(int(cpus)) + ".0"
                },
                "requests": {
                    "cpu": str(int(cpus) // 2 + 1) + ".0"
                    # "cpu": "4.0"
                    # "cpu": str(int(cpus) - 1) + ".0"
                }
            }    
    if cluster is False:
        if cpus:
            # # set limit/request cpus in resources
            values_dict['images']['resources'] = resources
        if mems:
            values_dict['images']['resources']["limits"].update({"memory": str(int(mems)) + "Gi"})
            values_dict['images']['resources']["requests"].update({"memory": str(int(mems) // 2 + 1) + "Gi"})
 
    # add extra volumes
    values_dict['extraVolumes'] = [{
        'name': 'test',
        'flexVolume': {
            'driver': "fstab/cifs",
            'fsType': "cifs",
            'secretRef': {
                'name': "cifs-test-secret"
            },
            'options': {
                'networkPath': IDC_NAS_URL,
                'mountOptions': "vers=1.0"
            }
        }
    }]
    values_dict['extraVolumeMounts'] = [{
        'name': 'test',
        'mountPath': '/test'
    }]

    with open(src_values_file, 'w') as f:
        dump(values_dict, f, default_flow_style=False)
    f.close()



if __name__ == "__main__":
    arg_parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    arg_parser.add_argument(
        '--src-values',
        help='src values.yaml')
    arg_parser.add_argument(
        '--deploy-params',
        help='deploy params')

    args = arg_parser.parse_args()
    src_values_file = args.src_values
    deploy_params_file = args.deploy_params
    if not src_values_file or not deploy_params_file:
        logging.error("No valid file input")
        sys.exit(-1)
    try:
        update_values(src_values_file, deploy_params_file)
        logging.info("Values.yaml updated")
    except Exception as e:
        logging.error(str(e))
        logging.error(traceback.format_exc())
        sys.exit(-1)