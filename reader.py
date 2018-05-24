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
    'resp_time', 'content_size'#, 'size_in'
]

dtypes = {
    'send_ts': np.str,
    'host_loglevel_logger': np.str,
    'tag': np.str,
    'http_method': np.str,
    #)'http_code': np.uint16,
    'http_code': np.str,
    'url': np.str,
    'resp_time': np.float32,
    #'content_size': np.int64
    'content_size': np.str,
    'size_in': np.int64,
    'latency': np.int64,
    'net_code': np.int64,
    'interval_event': np.int64
}


def string_to_df(data):
    start_time = time.time()

    # DEBUG
    #logger.info("####### DATA : {}".format(data))
    #logger.info("####### STRINGIO(DATA) : {}".format(StringIO(data)))
    chunk = pd.read_csv(StringIO(data), sep='\t', names=locust_log_columns, dtype=dtypes, engine='python')

    # DEBUG
    #logger.debug("\n\n####### Locust reader:\n##### chunk =\n{}\n\n".format(chunk))

    # format locust log date to timestamp : '[2017-12-28 14:46:34,327]' -> 1514468794.327
    locust_log_dt_obj = datetime.datetime.strptime(chunk.send_ts[0].replace('[','').replace(']',''), '%Y-%m-%d %H:%M:%S,%f')
    chunk['ts'] = time.mktime(locust_log_dt_obj.timetuple()) + locust_log_dt_obj.microsecond / 1e3

    # DEBUG
    #chunk['test'] = chunk.http_code.astype(np.str) + chunk.http_method
    #logger.debug("\n\n####### Locust reader:\n##### chunk =\n{}\n\n".format(chunk))

    ##chunk['receive_ts'] = chunk.send_ts + chunk.interval_real / 1e6
    chunk['receive_sec'] = chunk.ts.astype(np.int64)

    # split host_loglevel in (host, loglevel, logger) : 'localhost/INFO/resplog' -> ('localhost','INFO','resplog')
    [chunk['host'], chunk['loglevel'], chunk['logger']] = chunk.host_loglevel_logger.str.rsplit('/')[0]

    #chunk['tag'] = chunk.tag.str.rsplit('#', 1, expand=True)[0]
    chunk.set_index(['receive_sec'], inplace=True)

    # DEBUG / workaround for 'size_in' + misc missing keys
    chunk['time'] = chunk.ts
    chunk['size_in'] = -1
    chunk['size_out'] = chunk.content_size.astype(np.int64)
    chunk['latency'] = -1
    chunk['net_code'] = 0
    chunk['proto_code'] = chunk.http_code.astype(np.int64)
    chunk['interval_event'] = 1
    chunk['interval_real'] = 1
    chunk['receive_time'] = chunk.resp_time
    chunk['connect_time'] = -1
    chunk['send_time'] = -1

    # DEBUG
    #logger.debug("Chunk decode time: %.2fms", (time.time() - start_time) * 1000)
    #logger.info("\n\n####### Locust reader:\n##### chunk =\n{}\n\n".format(chunk))

    return chunk


class LocustReader(object):

    def __init__(self, filename, cache_size=1024*1024*50):
        self.buffer = ""
        self.stat_buffer = ""
        self.locust_log = open(filename, 'r')
        self.locust_finished = False
        self.closed = False
        self.cache_size = cache_size
        self.stat_queue = q.Queue()
        self.stats_reader = LocustStatAggregator(TimeChopper(self._read_stat_queue(), 1))

    def _read_stat_queue(self):
        while not self.closed:
            logger.info("######## DEBUG: _read_stat_queue() / self.stat_queue.qsize() = {}".format(self.stat_queue.qsize()))
            time.sleep(1)
            for _ in range(self.stat_queue.qsize()):
                try:
                    logger.info("######## DEBUG: _read_stat_queue() / self.stat_queue.qsize() = {}".format(self.stat_queue.qsize()))
                    si = self.stat_queue.get_nowait()
                    if si is not None:
                        yield si
                #except q.Empty:
                except q.Empty:
                    logger.info("######## DEBUG: _read_stat_queue() -> queue empty")
                    break

    def _read_locust_log_chunk(self):
        data = self.locust_log.read(self.cache_size)
        if data:
            parts = data.rsplit('\n', 1)
            if len(parts) > 1:
                ready_chunk = self.buffer + parts[0] + '\n'
                self.buffer = parts[1]
                self.stat_queue.put(string_to_df(ready_chunk))
                logger.info("######## DEBUG: self.stat_queue.put(string_to_df(ready_chunk)), ready_chunk = {}".format(ready_chunk))
                #logger.info("######## DEBUG: self.stat_queue.get() = {}".format(self.stat_queue.get()))
                #self.stat_queue.put(string_to_df(ready_chunk))
                logger.info("######## DEBUG: self.stat_queue.qsize() = {}".format(self.stat_queue.qsize()))
                return string_to_df(ready_chunk)
            else:
                self.buffer += parts[0]
        else:
            self.buffer += self.locust_log.readline()
        return None

    def __iter__(self):
        while not self.closed:
            yield self._read_locust_log_chunk()
        # read end
        chunk = self._read_locust_log_chunk()
        while chunk is not None:
            yield chunk
            chunk = self._read_locust_log_chunk()
        # don't forget the buffer
        if self.buffer:
            yield string_to_df(self.buffer)

        #self.locust_log.close()


    def close(self):
        self.closed = True

class LocustStatAggregator(object):
    def __init__(self, source):
        self.worker = agg.Worker({"ts": ["max"]}, False)
        self.source = source

    def __iter__(self):
        for ts, chunk in self.source:
            stats = self.worker.aggregate(chunk)
            logger.info("######## DEBUG: LocustStatAggregator().__iter__ ")
            yield [{
                'ts': ts,
                'metrics': {
                    'instances': stats['ts']['max'],
                    'reqps': 0
                }
            }]

    def close(self):
        pass



#class LocustStatsReader(object):
#   def __init__(self, filename, locust_info, cache_size=1024 * 1024 * 50):
#       self.locust_info = locust_info
#       self.stat_buffer = ""
#       self.stat_filename = filename
#       self.closed = False
#       self.start_time = 0
#       self.cache_size = cache_size
#
#    def _decode_stat_data(self, chunk):
#        """
#        Return all items found in this chunk
#        """
#        for date_str, statistics in chunk.iteritems():
#            date_obj = datetime.datetime.strptime(
#                date_str.split(".")[0], '%Y-%m-%d %H:%M:%S')
#            chunk_date = int(time.mktime(date_obj.timetuple()))
#            instances = 0
#            for benchmark_name, benchmark in statistics.iteritems():
#                if not benchmark_name.startswith("benchmark_io"):
#                    continue
#                for method, meth_obj in benchmark.iteritems():
#                    if "mmtasks" in meth_obj:
#                        instances += meth_obj["mmtasks"][2]
#
#            offset = chunk_date - 1 - self.start_time
#            reqps = 0
#            #if offset >= 0 and offset < len(self.locust_info.steps):  #DEBUG
#            #    reqps = self.locust_info.steps[offset][0]             #DEBUG
#            yield {
#                'ts': chunk_date - 1,
#                'metrics': {
#                    'instances': instances,
#                    'reqps': reqps
#                }
#            }
#
#    def _read_stat_data(self, stat_file):
#        chunk = stat_file.read(self.cache_size)
#        if chunk:
#            self.stat_buffer += chunk
#            parts = self.stat_buffer.rsplit('\n},', 1)
#            if len(parts) > 1:
#                ready_chunk = parts[0]
#                self.stat_buffer = parts[1]
#                chunks = [
#                    json.loads('{%s}}' % s) for s in ready_chunk.split('\n},')
#                ]
#                return list(
#                    itt.chain(
#                        *(self._decode_stat_data(chunk) for chunk in chunks)))
#        else:
#            self.stat_buffer += stat_file.readline()
#
#    def __iter__(self):
#        """
#        Union buffer and chunk, split using '\n},',
#        return splitted parts
#        """
#        self.start_time = int(time.time())
#        with open(self.stat_filename, 'r') as stat_file:
#            while not self.closed:
#                yield self._read_stat_data(stat_file)
#            yield self._read_stat_data(stat_file)
#
#    def close(self):
#        self.closed = True
