"""
Locust resplog reader. Read chunks from resplog and produce data frames
"""
import pandas as pd
import numpy as np
import queue as q
import logging
import json
import time
import datetime
import itertools as itt
from StringIO import StringIO

from yandextank.aggregator import TimeChopper
from yandextank.aggregator import aggregator as agg


logger = logging.getLogger(__name__)

locust_log_columns = [
    'send_ts', 'host_loglevel_logger', 'tag', 'http_method', 'http_code', 'url',
    'resp_time', 'content_size'
]

dtypes = {
    'send_ts': np.str,
    'host_loglevel_logger': np.str,
    'tag': np.str,
    'http_method': np.str,
    #'http_code': np.uint16,
    'http_code': np.str,
    'url': np.str,
    'resp_time': np.float32,
    #'content_size': np.int64
    'content_size': np.str
}


def string_to_df(data):
    start_time = time.time()

    # DEBUG
    #logger.info("####### DATA : {}".format(data))
    #logger.info("####### STRINGIO(DATA) : {}".format(StringIO(data)))
    chunk = pd.read_csv(StringIO(data), sep='\t', names=locust_log_columns, dtype=dtypes, engine='python')

    # DEBUG
    logger.debug("\n\n####### Chunk :\n{}\n\n".format(chunk))

    # format locust log date to timestamp : '[2017-12-28 14:46:34,327]' -> 1514468794.327
    locust_log_dt_obj = datetime.datetime.strptime(chunk.send_ts[0].replace('[','').replace(']',''), '%Y-%m-%d %H:%M:%S,%f')
    chunk['ts'] = time.mktime(locust_log_dt_obj.timetuple()) + locust_log_dt_obj.microsecond / 1e3
    chunk['test'] = chunk.http_code.astype(np.str) + chunk.http_method
    logger.debug("\n\n####### Chunk :\n{}\n\n".format(chunk))

    ##chunk['receive_ts'] = chunk.send_ts + chunk.interval_real / 1e6
    chunk['receive_sec'] = chunk.ts.astype(np.int64)

    # split host_loglevel in (host, loglevel, logger) : 'localhost/INFO/resplog' -> ('localhost','INFO','resplog')
    [chunk['host'], chunk['loglevel'], chunk['logger']] = chunk.host_loglevel_logger.str.rsplit('/')[0]

    #chunk['tag'] = chunk.tag.str.rsplit('#', 1, expand=True)[0]
    chunk.set_index(['receive_sec'], inplace=True)

    logger.debug("Chunk decode time: %.2fms", (time.time() - start_time) * 1000)
    logger.debug("\n####### Chunk : {}".format(chunk))
    return chunk


class LocustReader(object):

    def __init__(self, filename):
        self.buffer = ""
        self.stat_buffer = ""
        self.locust_log = filename
        self.locust_finished = False
        self.agg_finished = False
        self.closed = False
        self.stat_queue = q.Queue()
        self.stats_reader = LocustStatAggregator(
            TimeChopper(self._read_stat_queue(), 300000000))

    def _read_stat_queue(self):
        print("############ self.closed : {}".format(self.closed))
        print("############ self.stat_queue.qsize() : {}".format(self.stat_queue.qsize()))
        while not self.closed:
            for _ in range(self.stat_queue.qsize()):
                print("###########  stat_queue.qsize() = {}".format(self.stat_queue.qsize()))
                try:
                    print("########### in the try")
                    si = self.stat_queue.get_nowait()
                    if si is not None:
                        yield si
                        print("########### in the try/yield")
                except q.Empty:
                    print("########### in the except")
                    break

    def _read_locust_log_chunk(self, locust_log):
        data = locust_log.read(1024 * 1024 * 10)
        if data:
            parts = data.rsplit('\n', 1)
            if len(parts) > 1:
                ready_chunk = self.buffer + parts[0] + '\n'
                self.buffer = parts[1]
                df = string_to_df(ready_chunk)
                self.stat_queue.put(df)
                return df
            else:
                self.buffer += parts[0]
        else:
            if self.locust_finished:
                self.agg_finished = True
            locust_log.readline()
        return None

    def __iter__(self):
        with open(self.locust_log, 'r') as locust_log:
            while not self.closed:
                yield self._read_locust_log_chunk(locust_log)
            yield self._read_locust_log_chunk(locust_log)

    def close(self):
        self.closed = True




class LocustStatAggregator(object):
    def __init__(self, source):
        self.worker = agg.Worker({"allThreads": ["max"]}, False)
        self.source = source

    def __iter__(self):
        for ts, chunk in self.source:
            stats = self.worker.aggregate(chunk)
            yield [{
                'ts': ts,
                'metrics': {
                    'instances': stats['allThreads']['max'],
                    'reqps': 0
                }
            }]

    def close(self):
        pass



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
