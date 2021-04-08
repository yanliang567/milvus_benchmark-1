import os
from milvus_cloud import Milvus


endpoint = "i-606e82e658f0dc000da4ae47-ingress.azure-test.cloud-db.zilliz.com"
port = 443
token = "cfca3069b57331a6727ef5ba58c8b87bc01dc13c165c7b55309ed288a0c6d2fd"
os.environ["tls_path"] = "/home/zw/Downloads/azure-test.crt"
c = Milvus(host=endpoint, port=port, token=token)
print(c.list_collections())
