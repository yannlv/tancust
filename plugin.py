from pkg_resources import resource_string
from yandextank.plugins.Aggregator import AbstractReader, AggregatorPlugin, \
    AggregateResultListener, SecondAggregateDataItem
from yandextank.plugins.ConsoleOnline import \
    ConsoleOnlinePlugin, AbstractInfoWidget
from yandextank.core import AbstractPlugin
import yandextank.core as tankcore
import yandextank.plugins.ConsoleScreen as ConsoleScreen

#from ...common.interfaces import AbstractPlugin, AggregateResultListener, AbstractInfoWidget, GeneratorPlugin


from locust import main as lm
from locust import log as ll
import locust


from .reader import LocustReader, LocustStatsReader

from locust import runners

import gevent
import sys
#import os
#import signal
#import inspect
import logging
#import socket
#from optparse import OptionParser
#
#import locust.web 
#from locust.log import setup_logging, setup_resplogging, console_logger
from locust.stats import stats_printer, print_percentile_stats, print_error_report, print_stats
#from locust.inspectlocust import print_task_ratio, get_task_ratio_dict
#from locust.core import Locust, HttpLocust
from locust.runners import MasterLocustRunner, SlaveLocustRunner, LocalLocustRunner
import locust.events as events

#_internals = [Locust, HttpLocust]
version = locust.__version__

class LocustPlugin(AbstractPlugin, GeneratorPlugin):

    """ Locust tank plugin """
    SECTION = 'locust'


    def __init__(self, core):
        AbstractPlugin.__init__(self, core)
        self.host = None
        self.web_host = ""
        self.port = 8089
        self.locustfile = 'locustfile'
        self.master = False
        self.slave = False
        self.master_host = "127.0.0.1"
        self.master_port = 5557
        self.master_bind_host = "*"
        self.master_bind_port = 5557
        self.no_web = True
        self.num_clients = int(1)
        self.hatch_rate = float(1)
        self.num_requests = None
        self.loglevel = 'INFO'
        self.logfile = None
        self.csvfilebase = None
        self.csvappend = True
        self.print_stats = True
        self.only_summary = True
        self.list_commands = False
        self.show_task_ratio = False
        self.show_task_ratio_json = False
        self.show_version = False
        self.locustlogfile = False
        self.locustloglevel = 'INFO'

        # setup logging
        ll.setup_logging(self.loglevel, self.logfile)
        if self.locustlogfile:
            ll.setup_resplogging(self.locustloglevel, self.locustlogfile)

        logger = logging.getLogger(__name__)


    def get_available_options(self):
        return [
            "host", "port", "locustfile",
            "num_clients", "hatch_rate", "num_requests",
            "logfile", "loglevel", "csvfilebase"
        ]


    def _get_variables(self):
        res = {}
        for option in self.core.config.get_options(self.SECTION):
            if option[0] not in self.get_available_options():
                res[option[0]] = option[1]
        logging.debug("Variables: %s", res)
        return res


    def configure(self):
        self.host = self.get_option("host")
        self.port = self.get_option("port")
        self.locustfile = self.get_option("locustfile")
        self.num_clients = int(self.get_option ("num_clients"))
        self.hatch_rate = float(self.get_option("hatch_rate"))
        self.num_requests = int(self.get_option("num_requests"))
        self.logfile = self.get_option("logfile")
        self.loglevel = self.get_option("loglevel")
        self.csvfilebase = self.get_option("csv")
        self.locustlogfile = self.get_option("locustlogfile")
        self.locustloglevel = self.get_option("locustloglevel")


    def get_options(self):
        options = {optname : self.__getattribute__(optname) for optname in self.get_available_options()}
        print "get_options() : options = {}".format(options)
        return options

    def prepare_test(self):
        logger = logging.getLogger(__name__)
        aggregator = None
        try:
            aggregator = self.core.get_plugin_of_type(AggregatorPlugin)
        except Exception as ex:
            logger.warning("No aggregator found: %s", ex)

        if aggregator:
            aggregator.reader = LocustReader(self.locustlogfile)
            aggregator.stats_reader = aggregator.reader.stats_reader

        try:
            console = self.core.get_plugin_of_type(ConsolePlugin)
        except Exception as ex:
            logger.debug("Console not found: %s", ex)
            console = None

        if console:
            widget = JMeterInfoWidget(self)
            console.add_info_widget(widget)
            if aggregator:
                aggregator.add_result_listener(widget)

        return None


    def start_test(self):
        # setup logging
        ll.setup_logging(self.loglevel, self.logfile)
        if self.locustlogfile:
            ll.setup_resplogging(self.locustloglevel, self.locustlogfile)
        logger = logging.getLogger(__name__)

        if self.show_version:
            print "Locust %s".format(version)
            sys.exit(0)
        try:
            locustfile = lm.find_locustfile(self.locustfile)
            if not locustfile:
                logger.error("Locust plugin: Could not find any locustfile! Ensure file ends in '.py' and see --help for available options.")
                sys.exit(1)

            if locustfile == "locust.py":
                logger.error("Locust plugin: The locustfile must not be named `locust.py`. Please rename the file and try again.")
                sys.exit(1)

            docstring, locusts = lm.load_locustfile(locustfile)

            logger.info("Locust plugin: locustfile = {}".format(locustfile))
            logging.info("Locust plugin: locustfile = {}".format(locustfile))


#        logger.info("locustclass ? : {}".format(


        #if options.list_commands:
        #    console_logger.info("Available Locusts:")
        #    for name in locusts:
        #        console_logger.info("    " + name)
        #    sys.exit(0)

            if not locusts:
                logger.error("Locust plugin: No Locust class found!")
                sys.exit(1)
            else:
                logger.info("Locust plugin: Locust classes found in {} : {}".format(locustfile, locusts))

            locust_classes = list(locusts.values())
            options = Opts(**self.get_options())
            print "main() : options = {}".format(options)

            # run that shit
            runners.locust_runner = LocalLocustRunner(locust_classes, options)

            # spawn client spawning/hatching greenlet
            runners.locust_runner.start_hatching(wait=True)
            main_greenlet = runners.locust_runner.greenlet
            # spawn stats printing greenlet
            #if not options.only_summary and (options.print_stats or (options.no_web and not options.slave)):
            if not self.only_summary and (self.print_stats or (self.no_web and not self.slave)):
                gevent.spawn(stats_printer)
                print('### len(request_stats) = ' + str(len(runners.locust_runner.request_stats)) + '\n')

        except Exception as e:
            logger.error("{}".format(e))
            sys.exit(1)


        def shutdown(code=0):
            """
            Shut down locust by firing quitting event, printing stats and exiting
            """
            logger.info("Locust plugin: Shutting down (exit code %s), bye." % code)

            events.quitting.fire()
            print_stats(runners.locust_runner.request_stats)
            print_percentile_stats(runners.locust_runner.request_stats)

            print_error_report()
            sys.exit(code)

        # install SIGTERM handler
        def sig_term_handler():
            logger.info("Locust plugin: Got SIGTERM signal")
            shutdown(0)
            gevent.signal(signal.SIGTERM, sig_term_handler)

        try:
            logger.info("Locust plugin: Starting Locust %s" % version)
            main_greenlet.join()
            code = 0
            if len(runners.locust_runner.errors):
                code = 1
                shutdown(code=code)
        except KeyboardInterrupt as e:
            shutdown(0)






class Opts:
    def __init__(self, **entries):
        self.__dict__.update(entries)
        self.no_reset_stats = False
