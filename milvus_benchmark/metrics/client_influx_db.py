import logging
import traceback
from datetime import datetime, timedelta

from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS

from .client_base import ClientBase

logger = logging.getLogger("milvus_benchmark.metric.client_influx_db")


def influxdb_try_catch():
    def wrapper(func):
        def inner_wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except:
                logger.error("[InfluxDB Exception] %s" % str(traceback.format_exc()))
                return False
        return inner_wrapper
    return wrapper


class ClientInfluxDB(ClientBase):

    def __init__(self, url='http://localhost:8086', token="", org='primary', bucket='test', measurement_name='test',
                 timeout=10000):
        super().__init__()
        self.url = url
        self.token = token
        self.org = org
        self.bucket = bucket
        self.measurement_name = measurement_name

        self.client = InfluxDBClient(url=self.url, token=self.token, org=self.org, timeout=timeout)
        self.write_api = self.client.write_api(write_options=SYNCHRONOUS)
        self.query_api = self.client.query_api()
        self.delete_api = self.client.delete_api()

    @influxdb_try_catch()
    def query(self, bucket=None, org=None, measurement_name=None, tag=None, field=None, time="30d", query_content=None,
              out_put=False):
        bucket, org, measurement_name = self.params_value(bucket, org, measurement_name)

        if query_content is None:
            query_content = 'from(bucket: "%s") |> range(start: -%s)' % (bucket, time)
            query_content += ' |> filter(fn: (r) => r["_measurement"] == "%s")' % measurement_name
            if tag:
                query_content += ' |> filter(fn: (r) => r["tag"] == "%s")' % tag
            if field:
                query_content += ' |> filter(fn: (r) => r["_field"] == "%s")' % field

        logger.debug("[InfluxDB API] Query content： %s" % query_content)
        result = self.query_api.query(org=org, query=query_content)
        return self.parse_query_results(result, out_put)

    @influxdb_try_catch()
    def insert(self, bucket=None, org=None, measurement_name=None, tag=None, field=None):
        bucket, org, measurement_name = self.params_value(bucket, org, measurement_name)

        p = Point(measurement_name)

        if tag is not None and isinstance(tag, dict):
            for key, value in tag.items():
                p.tag(key, value)

        if field is not None and isinstance(field, dict):
            for key, value in field.items():
                p.field(key, value)

        logger.debug("[InfluxDB API] Insert data tags:%s, fields:%s, into measurement %s"
                     % (p._tags, p._fields, p._name))
        self.write_api.write(bucket=bucket, org=org, record=p)

    @influxdb_try_catch()
    def delete(self, bucket=None, org=None, predicate='', time=None):
        """
        e.g.:
        time = {"seconds": 1} / {"minutes": 1} / {"days": 1}
        """
        bucket, org, measurement_name = self.params_value(bucket, org)

        _time = datetime.now() - timedelta(**time) if time is not None else datetime.now()
        logger.debug("[InfluxDB API] Start deleting data in the database: bucket:%s, org:%s, time:%s, predicate:%s "
                     % (bucket, org, str(time), predicate))
        self.delete_api.delete(start=_time, stop=datetime.now(), predicate=predicate, bucket=bucket, org=org)

    def params_value(self, bucket=None, org=None, measurement_name=None):
        bucket = self.bucket if bucket is None else bucket
        org = self.org if org is None else org
        measurement_name = self.measurement_name if measurement_name is None else measurement_name
        return bucket, org, measurement_name

    @staticmethod
    def parse_query_results(result, out_put=False):
        results = []
        for table in result:
            for r in table.records:
                results.append((r.get_start(), r.get_stop(), r.get_time(), r.get_value(), r.get_field(),
                                r.get_measurement()))
        if out_put:
            logger.debug("[InfluxDB API] Query result: (_start, _stop, _time, _value, _filed, _measurement)")
            for res in results:
                logger.debug(res)
            logger.debug("[InfluxDB API] Query finished.")
        return results


if __name__ == "__main__":
    pass

