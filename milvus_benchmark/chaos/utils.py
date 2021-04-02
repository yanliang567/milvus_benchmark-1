from kubernetes import client, config
from milvus_benchmark import config as cf

config.load_kube_config()
v1 = client.CoreV1Api()


def list_pod_for_namespace(label_selector="app.kubernetes.io/instance=zong-single"):
    ret = v1.list_namespaced_pod(namespace=cf.NAMESPACE, label_selector=label_selector)
    pods = []
    # label_selector = 'release=zong-single'
    for i in ret.items:
        pods.append({"name": i.metadata.name})
        # print("%s\t%s\t%s" % (i.status.pod_ip, i.metadata.namespace, i.metadata.name))
    return pods
