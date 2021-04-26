import os
import logging
import traceback
import yaml
from kubernetes import client, config
config.load_kube_config()
client.rest.logger.setLevel(logging.WARNING)
    

def create_client_pod(name, namespace):
    body = None
    try:
        v1 = client.CoreV1Api()
        with open(os.path.join(os.path.dirname(__file__), "client.yaml")) as f:
            body = yaml.safe_load(f)
        body["metadata"]["name"] = name
        resp = v1.create_namespaced_pod(
            body=body, namespace=namespace)
        print("Pod created. status='%s'" % resp.metadata.name)
    except Exception as e:
        logging.error(traceback.format_exc())
        logging.error(str(e))
        raise Exception("Create client pod failed")
    

def delete_client_pod(name, namespace):
    pass