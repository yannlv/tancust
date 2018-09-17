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
    #'http_code': np.uint16,
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


def string_to_df(data, active_threads):
    start_time = time.time()

    # DEBUG
    #logger.info("####### DATA : {}".format(data))
    #logger.info("####### STRINGIO(DATA) : {}".format(StringIO(data)))
    csv_reader = pd.read_csv(StringIO(data), sep='\t', names=locust_log_columns, dtype=dtypes, iterator=True, chunksize=2048, engine='python')

    # DEBUG
    #logger.debug("\n\n####### Locust reader:\n##### chunk =\n{}\n\n".format(chunk))

    # Format locust log date to timestamp : '[2017-12-28 14:46:34,327]' -> 1514468794.327
    #locust_log_dt_obj = datetime.datetime.strptime(chunk.send_ts[0].replace('[','').replace(']',''), '%Y-%m-%d %H:%M:%S,%f')

    lines = []
    for line in csv_reader:

        buff_ts = line.send_ts.iloc[0]
        buff_ts = buff_ts.replace('[','').replace(']','')

        locust_dt = datetime.datetime.strptime(buff_ts, '%Y-%m-%d %H:%M:%S,%f')
        line['ts'] = time.mktime(locust_dt.timetuple()) + locust_dt.microsecond / 1e6

        # DEBUG
        #chunk['test'] = chunk.http_code.astype(np.str) + chunk.http_method
        #logger.debug("\n\n####### Locust reader:\n##### chunk =\n{}\n\n".format(chunk))

        ##chunk['receive_ts'] = chunk.send_ts + chunk.interval_real / 1e6
        line['receive_sec'] = line.ts.astype(np.int64)

        # split host_loglevel_logger in (host, loglevel, logger) : 'localhost/INFO/resplog' -> ('localhost','INFO','resplog')
        [line['host'], line['loglevel'], line['logger']] = line.host_loglevel_logger.iloc[0].split('/')

        #line['tag'] = line.tag.str.rsplit('#', 1, expand=True)[0]
        line.set_index(['receive_sec'], inplace=True)

        # Adding 'size_in' + misc missing keys
        line['time'] = line.ts
        line['size_in'] = line.content_size.astype(np.int64)
        line['size_out'] = 0
        line['latency'] = 0
        line['net_code'] = 0
        line['proto_code'] = line.http_code.astype(np.int64)
        line['interval_event'] = 1
        line['interval_real'] = 1
        line['receive_time'] = line.resp_time * 1000
        line['connect_time'] = 0
        line['send_time'] = 0
        line['active_threads'] = active_threads


        lines.append(line)

    chunk = pd.concat(lines)

    # DEBUG
    #logger.debug("Chunk decode time: %.2fms", (time.time() - start_time) * 1000)
    #logger.debug("\n\n####### Locust reader:\n##### chunk =\n{}\n\n".format(chunk))

    return chunk


class LocustReader(object):

    def __init__(self, owner, filename, cache_size=1024*1024*50):
        self.buffer = ""
        self.stat_buffer = ""
        self.locust_log = open(filename, 'r')
        self.locust_finished = False
        self.closed = False
        self.cache_size = cache_size
        self.stat_queue = q.Queue()
        self.stats_reader = LocustStatAggregator(TimeChopper(self._read_stat_queue(), 2))
        self.locust = owner

    def _read_stat_queue(self):
        while not self.closed:
            logger.debug("######## DEBUG: _read_stat_queue() / self.stat_queue.qsize() = {}".format(self.stat_queue.qsize()))
            time.sleep(1)
            for _ in range(self.stat_queue.qsize()):
                try:
                    logger.debug("######## DEBUG: _read_stat_queue() / self.stat_queue.qsize() = {}".format(self.stat_queue.qsize()))
                    si = self.stat_queue.get_nowait()
                    if si is not None:
                        yield si
                except q.Empty:
                    logger.debug("######## DEBUG: _read_stat_queue() -> queue empty")
                    break

    def _read_locust_log_chunk(self):
        data = self.locust_log.read(self.cache_size)
        if data:
            parts = data.rsplit('\n', 1)
            if len(parts) > 1:
                ready_chunk = self.buffer + parts[0] + '\n'
                self.buffer = parts[1]
                #logger.info("######## DEBUG: self.stat_queue.put(string_to_df(ready_chunk)), ready_chunk =\n##### {}".format(string_to_df(ready_chunk)))
                #logger.info("######## DEBUG: self.stat_queue.get() = {}".format(self.stat_queue.get()))
                #self.stat_queue.put(string_to_df(ready_chunk))
                logger.debug("######## DEBUG: _read_locust_log_chunk()/self.stat_queue.qsize() = {}".format(self.stat_queue.qsize()))
                logger.debug("######## DEBUG: _read_locust_log_chunk()/len(ready_chunk) = {}".format(len(ready_chunk)))
                if self.locust._locustrunner:
                    logger.debug("######## DEBUG: _read_locust_log_chunk()/self.locust._locustrunner.user_count = {}".format(self.locust._locustrunner.user_count))
                    self.stat_queue.put(string_to_df(ready_chunk, self.locust._locustrunner.user_count))
                    return string_to_df(ready_chunk, self.locust._locustrunner.user_count)
                else:
                    logger.debug("######## DEBUG: _read_locust_log_chunk()/self.locust._locustrunner.user_count : NO RUNNER YET")
                    self.stat_queue.put(string_to_df(ready_chunk, 0))
                    return string_to_df(ready_chunk, 0)

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



    def close(self):
        self.closed = True

class LocustStatAggregator(object):
    def __init__(self, source):
        self.worker_resptime = agg.Worker({"resp_time" : ["mean"]}, False)
        self.worker_instances_rps = agg.Worker({"active_threads" : ["max"]}, False)
        self.source = source
        self.groupby = 'tag'


    def __iter__(self):
        for ts, chunk in self.source:
            by_tag = list(chunk.groupby([self.groupby]))
            stats = self.worker_resptime.aggregate(chunk)
            logger.debug("######## DEBUG: LocustStatAggregator().__iter__\n  ##### LSA.ts= {}\n  ##### LSA.stats= {}\n  ##### LSA.chunk= {}".format(ts, stats, chunk))
            result = [{
                "ts": ts,
                "metrics": {
                    "run": self.worker_instances_rps.aggregate(chunk),
                    "reqps": 0,
                    "tagged": {tag: self.worker_resptime.aggregate(data) for tag, data in by_tag},
                    "overall": self.worker_resptime.aggregate(chunk)
                }
            }]
            logger.debug("######## DEBUG: LocustStatAggregator().__iter__\n  ##### result= {}\n".format(result))
            yield result

    def close(self):
        pass



