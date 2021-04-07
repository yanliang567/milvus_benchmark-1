from gevent import monkey
monkey.patch_all()
from yaml import full_load, dump
from chaos.chaos_opt import ChaosOpt
from milvus_benchmark.chaos.chaos_mesh import PodChaos, NetworkChaos
from milvus_benchmark import config

kind_chaos_mapping = {
    "PodChaos": PodChaos,
    "NetworkChaos": NetworkChaos
}


if __name__ == '__main__':
    with open('./pod.yaml') as f:
        conf = full_load(f)
        f.close()
    chaos_config = conf["chaos"]
    kind = chaos_config["kind"]
    spec = chaos_config["spec"]
    metadata_name = config.NAMESPACE + "-" + kind.lower()
    metadata = {"name": metadata_name}
    chaos_mesh = kind_chaos_mapping[kind](config.DEFAULT_API_VERSION, kind, metadata, spec)
    experiment_params = chaos_mesh.gen_experiment_config()
    # print(experiment_params)
    # with open('./pod-new-chaos.yaml', "w") as f:
    #     dump(experiment_params, f)
    #     f.close()
    chaos_opt = ChaosOpt(chaos_mesh.kind)
    res = chaos_opt.list_chaos_object()
    if len(res["items"]) != 0:
        print(len(res["items"]))
        print(res["items"][0]["metadata"]["annotations"]["kubectl.kubernetes.io/last-applied-configuration"])
    # chaos_opt.create_chaos_object(experiment_params)
