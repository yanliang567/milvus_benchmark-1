import logging

logger = logging.getLogger("milvus_benchmark.chaos.base")


class BaseChaos(object):
    def __init__(self, api_version, kind, metadata, spec):
        self.api_version = api_version,
        self.kind = kind,
        self.metadata = metadata,
        self.spec = spec

    def get_metadata_mame(self):
        if "name" not in self.metadata:
            raise Exception("please specify metadata name")
        return self.metadata["name"]

    def gen_experiment_params(self):
        pass
        """
        1. load dict from default yaml
        2. merge dict between dict and self.x
        """


class PodChaos(BaseChaos):
    def __init__(self, api_version, kind, metadata, spec):
        super(PodChaos, self).__init__(api_version, kind, metadata, spec)

    def gen_experiment_params(self):
        pass


class NetworkChaos(BaseChaos):
    def __init__(self, api_version, kind, metadata, spec):
        super(NetworkChaos, self).__init__(api_version, kind, metadata, spec)

    def gen_experiment_params(self):
        pass
