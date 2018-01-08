"""
Locust resplog reader. Read chunks from resplog and produce data frames
"""
import pandas as pd
import numpy as np
import logging
import json
import time
import datetime
import itertools as itt
from StringIO import StringIO

logger = logging.getLogger(__name__)

locout_columns = [
    'send_ts', 'host_loglevel_logger', 'tag', 'http_method', 'http_code', 'url',
    'resp_time', 'content_size'
]

dtypes = {
    'send_ts': np.str,
    'host_loglevel': np.str,
    'tag': np.str,
    'http_method': np.str,
    'http_code': np.int64,
    'url': np.str,
    'resp_time': np.int64,
    'content_size': np.int64
}


def string_to_df(data):
    start_time = time.time()
    chunk = pd.read_csv(
        StringIO(data), sep='\t', names=locout_columns, dtype=dtypes)

    # format locust log date to timestamp : '[2017-12-28 14:46:34,327]' -> 1514468794.327
    locout_dt_obj = datetime.strptime(chunk.send_ts.replace('[','').replace(']',''), '%Y-%m-%d %H:%M:%S,%f')
    chunk['ts'] = time.mktime(locout_dt_obj.timetuple()) + locout_dt_obj.microsecond / 1e3

    ##chunk['receive_ts'] = chunk.send_ts + chunk.interval_real / 1e6
    chunk['receive_sec'] = chunk.ts.astype(np.int64)

    # split host_loglevel in (host, loglevel, logger) : 'localhost/INFO/resplog' -> ('localhost','INFO','resplog')
    (chunk['host'], chunk['loglevel'], chunk['logger']) = tuple(chunk.host_loglevel.rsplit('/'))

    #chunk['tag'] = chunk.tag.str.rsplit('#', 1, expand=True)[0]
    chunk.set_index(['receive_sec'], inplace=True)

    logger.debug("Chunk decode time: %.2fms", (time.time() - start_time) * 1000)
    return chunk


class LocustReader(object):
    def __init__(self, filename, cache_size=1024 * 1024 * 50):
        self.buffer = ""
        self.locout = open(filename, 'r')
        self.closed = False
        self.cache_size = cache_size

    def _read_locout_chunk(self):
        data = self.locout.read(self.cache_size)
        if data:
            parts = data.rsplit('\n', 1)
            if len(parts) > 1:
                ready_chunk = self.buffer + parts[0] + '\n'
                self.buffer = parts[1]
                return string_to_df(ready_chunk)
            else:
                self.buffer += parts[0]
        else:
            self.buffer += self.locout.readline()
        return None

    def __iter__(self):
        while not self.closed:
            yield self._read_locout_chunk()
        yield self._read_locout_chunk()
        self.locout.close()

    def close(self):
        self.closed = True


class LocustStatsReader(object):
    def __init__(self, filename, locust_info):
        self.locust_info = locust_info
        self.buffer = ""
        self.stat_buffer = ""
        self.stat_filename = filename
        self.closed = False
        self.start_time = 0

    def _decode_stat_data(self, chunk):
        """
        Return all items found in this chunk
        """
        for date_str, statistics in chunk.iteritems():
            date_obj = datetime.datetime.strptime(
                date_str.split(".")[0], '%Y-%m-%d %H:%M:%S')
            chunk_date = int(time.mktime(date_obj.timetuple()))
            instances = 0
            for benchmark_name, benchmark in statistics.iteritems():
                if not benchmark_name.startswith("benchmark_io"):
                    continue
                for method, meth_obj in benchmark.iteritems():
                    if "mmtasks" in meth_obj:
                        instances += meth_obj["mmtasks"][2]

            offset = chunk_date - 1 - self.start_time
            reqps = 0
            if offset >= 0 and offset < len(self.locust_info.steps):
                reqps = self.locust_info.steps[offset][0]
            yield {
                'ts': chunk_date - 1,
                'metrics': {
                    'instances': instances,
                    'reqps': reqps
                }
            }

    def _read_stat_data(self, stat_file):
        chunk = stat_file.read(1024 * 1024 * 50)
        if chunk:
            self.stat_buffer += chunk
            parts = self.stat_buffer.rsplit('\n},', 1)
            if len(parts) > 1:
                ready_chunk = parts[0]
                self.stat_buffer = parts[1]
                chunks = [
                    json.loads('{%s}}' % s) for s in ready_chunk.split('\n},')
                ]
                return list(
                    itt.chain(
                        *(self._decode_stat_data(chunk) for chunk in chunks)))
        else:
            self.stat_buffer += stat_file.readline()

    def __iter__(self):
        """
        Union buffer and chunk, split using '\n},',
        return splitted parts
        """
        self.start_time = int(time.time())
        with open(self.stat_filename, 'r') as stat_file:
            while not self.closed:
                yield self._read_stat_data(stat_file)
            yield self._read_stat_data(stat_file)

    def close(self):
        self.closed = True
