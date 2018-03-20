from pkg_resources import resource_string

from yandextank.common.interfaces import AbstractPlugin, AggregateResultListener, AbstractInfoWidget, GeneratorPlugin, AbstractCriterion
#from yandextank.plugins.Console import Plugin as ConsolePlugin
from yandextank.plugins.Autostop import Plugin as AutostopPlugin
#from ..Console import Plugin as ConsolePlugin
#from ..Autostop import Plugin as AutostopPlugin
from yandextank.plugins.Console import screen as ConsoleScreen

#from ...common.util import splitstring
from yandextank.common.util import splitstring
#from .reader import LocustReader, LocustStatsReader
#from ...stepper import StepperWrapper
from yandextank.stepper import StepperWrapper

from locust import runners as lr
from locust import main as lm
from locust import log as ll
import locust


import gevent
import sys
import logging
from locust.stats import stats_printer, print_percentile_stats, print_error_report, print_stats
from  locust.runners import MasterLocustRunner, SlaveLocustRunner, LocalLocustRunner
import locust.events as events
from locust.util.time import parse_timespan

#_internals = [Locust, HttpLocust]
version = locust.__version__

logger = logging.getLogger(__name__)

class Plugin(AbstractPlugin, GeneratorPlugin):

    """ Locust tank plugin """
    SECTION = 'locust'


    def __init__(self, core, cfg, cfg_updater):
        AbstractPlugin.__init__(self, core, cfg, cfg_updater)
 #       self.stats_reader = None
 #       self.reader = None
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
        self.run_time = '60s'
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



    def get_available_options(self):
        return [
            "host", "port", "locustfile",
            "num_clients", "hatch_rate", "run_time", #"num_requests",
            "logfile", "loglevel", "csvfilebase"
        ]


    def _get_variables(self):
        res = {}
        for option in self.core.config.get_options(self.SECTION):
            if option[0] not in self.get_available_options():
                res[option[0]] = option[1]
        logging.debug("Variables: %s", res)
        return res

#    def get_reader(self):
#        if self.reader is None:
#            self.reader = LocustReader(self.locustlogfile)
#        return self.reader
#
#    def get_stats_reader(self):
#        if self.stats_reader is None:
#            self.stats_reader = self.reader.stats_reader
#        return self.stats_reader

    def configure(self):
        self.host = self.get_option("host")
        self.port = self.get_option("port")
        self.locustfile = self.get_option("locustfile")
        self.num_clients = int(self.get_option ("num_clients"))
        self.hatch_rate = float(self.get_option("hatch_rate"))
        #self.num_requests = int(self.get_option("num_requests"))
        self.run_time = self.get_option("run_time")
        self.logfile = self.get_option("logfile")
        self.loglevel = self.get_option("loglevel")
        self.csvfilebase = self.get_option("csv")
        self.locustlogfile = self.get_option("locustlogfile")
        self.locustloglevel = self.get_option("locustloglevel")
#        try:
#            autostop = self.core.get_plugin_of_type(AutostopPlugin)
#            autostop.add_criterion_class(UsedInstancesCriterion)
#        except KeyError:
#            logging.debug(
#                "No autostop plugin found, not adding instances criterion")

    def get_options(self):
        options = {optname : self.__getattribute__(optname) for optname in self.get_available_options()}
        print "get_options() : options = {}".format(options)
        return options

    def prepare_test(self):
        logger = logging.getLogger(__name__)
#        aggregator = None

#        try:
#            console = self.core.get_plugin_of_type(ConsolePlugin)
#        except Exception as ex:
#            logger.debug("Console not found: %s", ex)
#            console = None

#       if console:
#            widget = LocustInfoWidget(self)
#            console.add_info_widget(widget)
#            self.core.job.aggregator.add_result_listener(widget)


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
            #locust_classes = locusts
            options = Opts(**self.get_options())
            print "main() : options = {}".format(options)

            # run that shit
            if self.run_time:
                if not self.no_web:
                    logger.error("The --run-time argument can only be used together with --no-web")
                    sys.exit(1)
                try:
                    self.run_time = parse_timespan(self.run_time)
                except ValueError:
                    logger.error("Valid --time-limit formats are: 20, 20s, 3m, 2h, 1h20m, 3h30m10s, etc.")
                    sys.exit(1)
                def spawn_run_time_limit_greenlet():
                    logger.info("Run time limit set to %s seconds" % self.run_time)
                    def timelimit_stop():
                        logger.info("Time limit reached. Stopping Locust.")
                        lr.locust_runner.quit()
                    gevent.spawn_later(self.run_time, timelimit_stop)

            if not self.no_web and not self.slave:
                # spawn web greenlet
                logger.info("Starting web monitor at %s:%s" % (self.web_host or "*", self.port))
                main_greenlet = gevent.spawn(web.start, locust_classes, options)


            if not self.master and not self.slave:
                logger.info("Locust plugin : LocalLocustRunner about to be launched")
                lr.locust_runner = LocalLocustRunner(locust_classes, options)
                # spawn client spawning/hatching greenlet
                if self.no_web:
                    logger.info("Locust plugin : LocalLocustRunner.start_hatching()")
                    lr.locust_runner.start_hatching(wait=True)
                    main_greenlet = lr.locust_runner.greenlet
                if self.run_time:
                    logger.info("Locust plugin : spawn_run_time_limit_greenlet()")
                    spawn_run_time_limit_greenlet()
            elif self.master:
                lr.locust_runner = MasterLocustRunner(locust_classes, options)
                if self.no_web:
                    while len(lr.locust_runner.clients.ready)<self.expect_slaves:
                        logging.info("Waiting for slaves to be ready, %s of %s connected",
                                     len(lr.locust_runner.clients.ready), self.expect_slaves)
                        time.sleep(1)

                    lr.locust_runner.start_hatching(self.num_clients, self.hatch_rate)
                    main_greenlet = lr.locust_runner.greenlet
                    if self.run_time:
                        spawn_run_time_limit_greenlet()
            elif self.slave:
                if self.run_time:
                    logger.error("--run-time should be specified on the master node, and not on slave nodes")
                    sys.exit(1)
                try:
                    lr.locust_runner = SlaveLocustRunner(locust_classes, options)
                    main_greenlet = lr.locust_runner.greenlet
                except socket.error as e:
                    logger.error("Failed to connect to the Locust master: %s", e)
                    sys.exit(-1)

            # spawn client spawning/hatching greenlet
            #lr.locust_runner.start_hatching(wait=True)
            #main_greenlet = lr.locust_runner.greenlet
            # spawn stats printing greenlet
            #if not options.only_summary and (options.print_stats or (options.no_web and not options.slave)):
            #if not self.only_summary and (self.print_stats or (self.no_web and not self.slave)):
            #    gevent.spawn(stats_printer)
            #    print('### len(request_stats) = ' + str(len(lr.locust_runner.request_stats)) + '\n')

        except Exception as e:
            logger.error("### CRITICAL ERROR : %s", e)
            sys.exit(1)


        def shutdown(code=0):
            """
            Shut down locust by firing quitting event, printing stats and exiting
            """
            logger.info("Locust plugin: Shutting down (exit code %s), bye." % code)

            events.quitting.fire()
            print_stats(lr.locust_runner.request_stats)
            print_percentile_stats(lr.locust_runner.request_stats)

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
            if len(lr.locust_runner.errors):
                code = 1
                shutdown(code=code)
        except KeyboardInterrupt as e:
            shutdown(0)



    def is_test_finished(self):
        logger.info("Locust plugin: fetching locust status")
        return 0
#        if lr.locust_runner.user_count() > 1:
#            return -1
#        else:
#            logger.info("Locust finished")
#            return 0

    def end_test(self, retcode):
        if lr.locust_runner.user_count > 1:
            logger.info("Terminating Locust")
            shutdown(code=0)
        else:
            logger.info("Locust has been terminated")
        return retcode

class Opts:
    def __init__(self, **entries):
        self.__dict__.update(entries)
        self.no_reset_stats = False
        self.reset_stats = True


class LocustInfoWidget(AbstractInfoWidget, AggregateResultListener):
    """ Right panel widget with Locust test info """

    def __init__(self, locust):
        AbstractInfoWidget.__init__(self)
        self.krutilka = ConsoleScreen.krutilka()
        self.locust = locust
        self.active_threads = 0
        self.RPS = 0

    def get_index(self):
        return 0

    def on_aggregated_data(self, data, stats):
        self.active_threads = stats['metrics']['instances']
        self.RPS = data['overall']['interval_real']['len']



class UsedInstancesCriterion(AbstractCriterion):
    """
    Autostop criterion, based on active instances count
    """
    RC_INST = 24

    @staticmethod
    def get_type_string():
        return 'instances'

    def __init__(self, autostop, param_str):
        AbstractCriterion.__init__(self)
        self.seconds_count = 0
        self.autostop = autostop
        self.threads_limit = 1

        level_str = param_str.split(',')[0].strip()
        if level_str[-1:] == '%':
            self.level = float(level_str[:-1]) / 100
            self.is_relative = True
        else:
            self.level = int(level_str)
            self.is_relative = False
        self.seconds_limit = expand_to_seconds(param_str.split(',')[1])

        try:
            locust = autostop.core.get_plugin_of_type(Plugin)
            info = locust.get_info()
            if info:
                self.threads_limit = info.instances
            if not self.threads_limit:
                raise ValueError(
                    "Cannot create 'instances' criterion"
                    " with zero instances limit")
        except KeyError:
            logger.warning("No locust module, 'instances' autostop disabled")

    def notify(self, data, stat):
        threads = stat["metrics"]["instances"]
        if self.is_relative:
            threads = float(threads) / self.threads_limit
        if threads > self.level:
            if not self.seconds_count:
                self.cause_second = (data, stat)

            logger.debug(self.explain())

            self.seconds_count += 1
            self.autostop.add_counting(self)
            if self.seconds_count >= self.seconds_limit:
                return True
        else:
            self.seconds_count = 0

        return False

    def get_rc(self):
        return self.RC_INST

    def get_level_str(self):
        """
        String value for instances level
        """
        if self.is_relative:
            level_str = str(100 * self.level) + "%"
        else:
            level_str = self.level
        return level_str

    def explain(self):
        items = (
            self.get_level_str(), self.seconds_count,
            self.cause_second[0].get('ts'))
        return (
            "Testing threads (instances) utilization"
            " higher than %s for %ss, since %s" % items)

    def widget_explain(self):
        items = (self.get_level_str(), self.seconds_count, self.seconds_limit)
        return "Instances >%s for %s/%ss" % items, float(
            self.seconds_count) / self.seconds_limit

