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
MINIO_HOST = "minio-test"
MINIO_PORT = 9000


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
    values_dict["service"]["type"] = "ClusterIP"
    if deploy_mode != DEFUALT_DEPLOY_MODE:
        cluster = True
        values_dict["cluster"]["enabled"] = True
    if "server" in deploy_params:
        server = deploy_params["server"]
        server_name = server["server_name"] if "server_name" in server else ""
        server_tag = server["server_tag"] if "server_tag" in server else ""
    else:
        raise Exception("No server specified in {}".format(deploy_params_file))
    # TODO: update milvus config
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
    # use external minio/s3
    values_dict['minio']['enabled'] = False
    values_dict["externalS3"]["enabled"] = True
    values_dict["externalS3"]["host"] = MINIO_HOST
    values_dict["externalS3"]["port"] = MINIO_PORT
    values_dict["externalS3"]["accessKey"] = "minioadmin"
    values_dict["externalS3"]["secretKey"] = "minioadmin"
    values_dict["externalS3"]["bucketName"] = "test"

    if cluster is False:
        # TODO: support pod affinity for standalone mode
        if cpus:
            # values_dict['standalone']['nodeSelector'] = node_config
            # values_dict['minio']['nodeSelector'] = node_config
            # values_dict['etcd']['nodeSelector'] = node_config
            # # set limit/request cpus in resources
            values_dict['standalone']['resources'] = resources
        if mems:
            values_dict['standalone']['resources']["limits"].update({"memory": str(int(mems)) + "Gi"})
            values_dict['standalone']['resources']["requests"].update({"memory": str(int(mems) // 2 + 1) + "Gi"})
        if gpus:
            logging.info("TODO: Need to schedule pod on GPU server")
        logging.debug("Add tolerations into standalone server")
        values_dict['standalone']['tolerations'] = perf_tolerations
        # values_dict['minio']['tolerations'] = perf_tolerations
        values_dict['etcd']['tolerations'] = perf_tolerations
    else:
        # TODO: mem limits on distributed mode
        # values_dict['pulsar']["broker"]["configData"].update({"maxMessageSize": "52428800", "PULSAR_MEM": BOOKKEEPER_PULSAR_MEM})
        # values_dict['pulsar']["bookkeeper"]["configData"].update({"nettyMaxFrameSizeBytes": "52428800", "PULSAR_MEM": BROKER_PULSAR_MEM})
        if cpus:
            # values_dict['standalone']['nodeSelector'] = node_config
            # values_dict['minio']['nodeSelector'] = node_config
            # values_dict['etcd']['nodeSelector'] = node_config
            # # set limit/request cpus in resources
            # values_dict['proxy']['resources'] = resources
            values_dict['queryNode']['resources'] = resources
            values_dict['indexNode']['resources'] = resources
            values_dict['dataNode']['resources'] = resources
            # values_dict['minio']['resources'] = resources
            # values_dict['pulsarStandalone']['resources'] = resources
        if mems:
            logging.debug("TODO: Update mem resources")
        # # pulsar distributed mode
        # values_dict['pulsar']["enabled"] = True
        # values_dict['pulsar']['autoRecovery']['nodeSelector'] = node_config
        # values_dict['pulsar']['proxy']['nodeSelector'] = node_config
        # values_dict['pulsar']['broker']['nodeSelector'] = node_config
        # values_dict['pulsar']['bookkeeper']['nodeSelector'] = node_config
        # values_dict['pulsar']['zookeeper']['nodeSelector'] = node_config
        
        logging.debug("Add tolerations into cluster server")
        values_dict['proxy']['tolerations'] = perf_tolerations
        values_dict['queryNode']['tolerations'] = perf_tolerations
        values_dict['indexNode']['tolerations'] = perf_tolerations
        values_dict['dataNode']['tolerations'] = perf_tolerations
        values_dict['etcd']['tolerations'] = perf_tolerations
        # values_dict['minio']['tolerations'] = perf_tolerations
        values_dict['pulsarStandalone']['tolerations'] = perf_tolerations
        # TODO: for distributed deployment
        # values_dict['pulsar']['autoRecovery']['tolerations'] = perf_tolerations
        # values_dict['pulsar']['proxy']['tolerations'] = perf_tolerations
        # values_dict['pulsar']['broker']['tolerations'] = perf_tolerations
        # values_dict['pulsar']['bookkeeper']['tolerations'] = perf_tolerations
        # values_dict['pulsar']['zookeeper']['tolerations'] = perf_tolerations

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