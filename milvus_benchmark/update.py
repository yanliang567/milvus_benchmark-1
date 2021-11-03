import sys
import re
import logging
import traceback
import argparse
from yaml import full_load, dump
import config
import utils
import requests
import json


def get_token(url):
    rep = requests.get(url)
    data = json.loads(rep.text)
    if 'token' in data:
        token = data['token']
    else:
        token = ''
        print("Can not get token.")
    return token


def get_tags(url, token):
    headers = {'Content-type': "application/json",
               "charset": "UTF-8",
               "Accept": "application/vnd.docker.distribution.manifest.v2+json",
               "Authorization": "Bearer %s" % token}
    try:
        rep = requests.get(url, headers=headers)
        data = json.loads(rep.text)

        tags = []
        if 'tags' in data:
            tags = data["tags"]
        else:
            print("Can not get the tag list")
        return tags
    except:
        print("Can not get the tag list")
        return []


def get_perf_tags(tags_list):
    _list = []

    if not isinstance(tags_list, list):
        print("tags_list is not a list.")
        return _list

    for tag in tags_list:
        # if "master" in tag and tag != "master-latest":
        if "perf" in tag and tag != "perf-latest":
            _list.append(tag)
    return _list


def get_config_digest(url, token):
    headers = {'Content-type': "application/json",
               "charset": "UTF-8",
               "Accept": "application/vnd.docker.distribution.manifest.v2+json",
               "Authorization": "Bearer %s" % token}
    try:
        rep = requests.get(url, headers=headers)
        data = json.loads(rep.text)

        digest = ''
        if 'config' in data and 'digest' in data["config"]:
            digest = data["config"]["digest"]
        else:
            print("Can not get the digest")
        return digest
    except:
        print("Can not get the digest")
        return ""


def get_latest_tag(limit=200):
    service = "registry.docker.io"
    repository = "milvusdb/milvus-dev"

    auth_url = "https://auth.docker.io/token?service=%s&scope=repository:%s:pull" % (service, repository)
    tags_url = "https://index.docker.io/v2/%s/tags/list" % repository
    tag_url = "https://index.docker.io/v2/milvusdb/milvus-dev/manifests/"

    master_latest_digest = get_config_digest(tag_url + "perf-latest", get_token(auth_url))
    tags = get_tags(tags_url, get_token(auth_url))
    tag_list = get_perf_tags(tags)

    latest_tag = ""
    for i in range(1, len(tag_list) + 1):
        tag_name = str(tag_list[-i])
        tag_digest = get_config_digest(tag_url + tag_name, get_token(auth_url))
        if tag_digest == master_latest_digest:
            latest_tag = tag_name
            break
        if i > limit:
            break

    if latest_tag == "":
        # latest_tag = "master-latest"
        raise print("Can't find the latest image name")
    print("The image name used is %s" % str(latest_tag))
    return latest_tag


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
    print("[benchmark update] deploy_params: %s" % str(deploy_params))
    deploy_mode = utils.get_deploy_mode(deploy_params)
    print("[benchmark update] deploy_mode: %s" % str(deploy_mode))
    cluster = False
    values_dict["service"]["type"] = "ClusterIP"
    # milvus: deploy_mode: \"cluster\"
    if deploy_mode == config.CLUSTER_DEPLOY_MODE:
        cluster = True
    elif deploy_mode == config.CLUSTER_3RD_DEPLOY_MODE:
        cluster = True
    elif deploy_mode == config.SINGLE_DEPLOY_MODE:
        values_dict["cluster"]["enabled"] = False
        values_dict["etcd"]["replicaCount"] = 1
        values_dict["minio"]["mode"] = "standalone"
        values_dict["pulsar"]["enabled"] = False

    if deploy_mode == config.SINGLE_DEPLOY_MODE:
        values_dict["standalone"]["persistence"]["persistentVolumeClaim"]["size"] = "100Gi"

    server_tag = utils.get_server_tag(deploy_params)
    print("[benchmark update] server_tag: %s" % str(server_tag))

    server_resource = utils.get_server_resource(deploy_params)
    print("[benchmark update] server_resource: %s" % str(server_resource))

    # TODO: update milvus config
    # # update values.yaml with the given host
    # node_config = None
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
    if cpus and mems:
        resources_cluster = {
                "limits": {
                    "cpu": str(int(cpus)) + ".0",
                    "memory": str(int(mems)) + "Gi"
                },
                "requests": {
                    "cpu": str(int(cpus) // 2 + 1) + ".0",
                    "memory": str(int(mems) // 2 + 1) + "Gi"
                    # "cpu": "4.0"
                    # "cpu": str(int(cpus) - 1) + ".0"
                }
            }
    # use external minio/s3
    
    # TODO: disable temp
    # values_dict['minio']['enabled'] = False
    values_dict['minio']['enabled'] = True
    # values_dict["externalS3"]["enabled"] = True
    values_dict["externalS3"]["enabled"] = False
    values_dict["externalS3"]["host"] = config.MINIO_HOST
    values_dict["externalS3"]["port"] = config.MINIO_PORT
    values_dict["externalS3"]["accessKey"] = config.MINIO_ACCESS_KEY
    values_dict["externalS3"]["secretKey"] = config.MINIO_SECRET_KEY
    values_dict["externalS3"]["bucketName"] = config.MINIO_BUCKET_NAME
    logging.debug(values_dict["externalS3"])

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
        # values_dict['standalone']['tolerations'] = perf_tolerations
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
            values_dict['queryNode']['resources'] = resources_cluster
            values_dict['indexNode']['resources'] = resources_cluster
            values_dict['dataNode']['resources'] = resources_cluster
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
        # values_dict['proxy']['tolerations'] = perf_tolerations
        # values_dict['queryNode']['tolerations'] = perf_tolerations
        # values_dict['indexNode']['tolerations'] = perf_tolerations
        # values_dict['dataNode']['tolerations'] = perf_tolerations
        values_dict['etcd']['tolerations'] = perf_tolerations
        # values_dict['minio']['tolerations'] = perf_tolerations
        if deploy_mode == config.SINGLE_DEPLOY_MODE:
            values_dict['pulsarStandalone']['tolerations'] = perf_tolerations
        # TODO: for distributed deployment
        # values_dict['pulsar']['autoRecovery']['tolerations'] = perf_tolerations
        # values_dict['pulsar']['proxy']['tolerations'] = perf_tolerations
        # values_dict['pulsar']['broker']['tolerations'] = perf_tolerations
        # values_dict['pulsar']['bookkeeper']['tolerations'] = perf_tolerations
        # values_dict['pulsar']['zookeeper']['tolerations'] = perf_tolerations
        milvus_params = deploy_params["milvus"]
        if "datanode" in milvus_params:
            if "replicas" in milvus_params["datanode"]:
                values_dict['dataNode']["replicas"] = milvus_params["datanode"]["replicas"]
        if "querynode"in milvus_params:
            if "replicas" in milvus_params["querynode"]:
                values_dict['queryNode']["replicas"] = milvus_params["querynode"]["replicas"]
        if "indexnode"in milvus_params:
            if "replicas" in milvus_params["indexnode"]:
                values_dict['indexNode']["replicas"] = milvus_params["indexnode"]["replicas"]
        if "proxy"in milvus_params:
            if "replicas" in milvus_params["proxy"]:
                values_dict['proxy']["replicas"] = milvus_params["proxy"]["replicas"]
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
                'networkPath': config.IDC_NAS_URL,
                'mountOptions': "vers=1.0"
            }
        }
    }]
    values_dict['extraVolumeMounts'] = [{
        'name': 'test',
        'mountPath': '/test'
    }]

    tag = get_latest_tag()
    values_dict["image"]["all"]["tag"] = tag

    values_dict = utils.update_dict_value(server_resource, values_dict)

    print("[benchmark update] value.yaml: %s" % str(values_dict))
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