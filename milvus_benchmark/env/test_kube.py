import os
import logging
import traceback
import yaml
from kubernetes import client, config
config.load_kube_config()
client.rest.logger.setLevel(logging.WARNING)


def get_host_cpus(hostname):
    try:
        v1 = client.CoreV1Api()
        cpus = v1.read_node(hostname).status.allocatable.get("cpu")
    except Exception as e:
        logging.error(traceback.format_exc())
        logging.error(str(e))
        cpus = 0
    finally:
        return cpus


def main():
    body = None
    try:
        v1 = client.CoreV1Api()
        with open(os.path.join(os.path.dirname(__file__), "client.yaml")) as f:
            body = yaml.safe_load(f)
        body["metadata"]["generateName"] = "test"
        resp = v1.create_namespaced_pod(
            body=body, namespace="milvus")
        print("Pod created. status='%s'" % resp.metadata.name)
    except Exception as e:
        logging.error(traceback.format_exc())
        logging.error(str(e))


if __name__ == "__main__":
    main()