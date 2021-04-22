import argparse

from milvus import Milvus


def main():
    arg_parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    arg_parser.add_argument(
        '--host',
        help='server host ip param for argo mode',
        default='127.0.0.1')
    arg_parser.add_argument(
        '--port',
        help='server port param for argo mode',
        default='19530')
    args = arg_parser.parse_args()
    host = args.host
    port = args.port
    m = Milvus(host=host, port=port)
    print(m.list_collections())


if __name__ == "__main__":
    main()
