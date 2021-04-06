from yaml import full_load, dump
from chaos.chaos_opt import ChaosOpt
from milvus_benchmark.chaos.chaos_mesh import PodChaos, NetworkChaos
from milvus_benchmark import config

kind_chaos_mapping = {
    "PodChaos": PodChaos,
    "NetworkChaos": NetworkChaos
}


class Person:
    def __init__(self, name, age):
        self.name = name
        self.age = age

    # @property
    # def name(self):
    #     return self.name
    #
    # @name.setter
    # def name(self, n):
    #     self.name = n

    def pprin(self):
        if self.age > 18:
            print(self.name)


if __name__ == '__main__':
    from locust import User, events
    import gevent
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
    print(experiment_params)
    with open('./pod-new.yaml', "w") as f:
        dump(experiment_params, f)
        f.close()
    chaos_opt = ChaosOpt(chaos_mesh.kind)
    data = chaos_opt.list_chaos_object()
    print(data)
    # chaos_opt.create_chaos_object(experiment_params)
