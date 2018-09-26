from pkg_resources import resource_string

from yandextank.common.interfaces import AbstractPlugin, AggregateResultListener, AbstractInfoWidget, GeneratorPlugin, AbstractCriterion
from yandextank.plugins.Console import Plugin as ConsolePlugin
from yandextank.plugins.Autostop import Plugin as AutostopPlugin
#from ..Console import Plugin as ConsolePlugin
#from ..Autostop import Plugin as AutostopPlugin
from yandextank.plugins.Console import screen as ConsoleScreen

#from ...common.util import splitstring
from yandextank.common.util import splitstring
from .reader import LocustReader #, LocustStatsReader
#from ...stepper import StepperWrapper
from yandextank.stepper import StepperWrapper

from locust import runners as lr
from locust import main as lm
from locust import log as ll
import locust


import gevent
import sys
import logging
import time

from locust.stats import stats_printer, stats_writer, write_stat_csvs, print_percentile_stats, print_error_report, print_stats
from  locust.runners import MasterLocustRunner, SlaveLocustRunner, LocalLocustRunner
import locust.events as events
from locust.util.time import parse_timespan

import tempfile

#_internals = [Locust, HttpLocust]
version = locust.__version__

logger = logging.getLogger(__name__)

class Plugin(AbstractPlugin, GeneratorPlugin):

    """ Locust tank plugin """
    SECTION = 'locust'


    def __init__(self, core, cfg, cfg_updater):
        AbstractPlugin.__init__(self, core, cfg, cfg_updater)
        self.core = core
        self._locustrunner = None
        self._locustclasses = None
        self._options = None
        self._user_count = 0
        self._state = ''
        self._locuststats = ''
        self.stats_reader = None
        self.reader = None
        self.host = None
        self.web_host = ''
        self.port = 8089
        self.locustfile = 'locustfile'
        self.master = False
        self.slave = False
        self.master_host = "127.0.0.1"
        self.master_port = 5557
        self.master_bind_host = "*"
        self.master_bind_port = 5557
        self.expect_slaves = 0
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
        self.show_version = True
        #self.locustlog_file = None
        #self.locustlog_fd = None
        self.locustlog_level = 'INFO'
        #self._stat_log = None
        self.cfg = cfg

        # setup logging
        ll.setup_logging(self.loglevel, self.logfile)

    @property
#    def set_locustlog_file(self):
    def locustlog_file(self):
        logger.info("######## DEBUG: self.core.artifacts_dir = {}".format(self.core.artifacts_dir))
        return "{}/locust.log".format(self.core.artifacts_dir)


    def get_available_options(self):
        return [
            "host", "port", "locustfile",
            "num_clients", "hatch_rate", "run_time", #"num_requests",
            "logfile", "loglevel", "csvfilebase",
            "master", "master_bind_host", "master_bind_port", "expect_slaves",
            "master_host", "master_port"
        ]


    def _get_variables(self):
        res = {}
        for option in self.core.config.get_options(self.SECTION):
            if option[0] not in self.get_available_options():
                res[option[0]] = option[1]
        logger.debug("Variables: %s", res)
        return res


    def get_reader(self):
        if self.reader is None:
            self.reader = LocustReader(self, self.locustlog_file)
        return self.reader

    def get_stats_reader(self):
        if self.stats_reader is None:
            self.stats_reader = self.reader.stats_reader
            logger.debug("######## DEBUG: plugin.reader.stats_reader.source = %s" % self.stats_reader.source)
            return self.stats_reader

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
        self.csvfilebase = self.get_option("csvfilebase")
        #self.locustlog_file = self.get_option("locustlog_file")
        #self.locustlog_file = self.set_locustlog_file()
        self.locustlog_level = self.get_option("locustlog_level")
        self.show_version = True
        self.master = self.get_option("master")
        self.master_bind_host = self.get_option("master_bind_host")
        self.master_bind_port = self.get_option("master_bind_port")
        self.expect_slaves = self.get_option("expect_slaves")
        self.master_host = self.get_option("master_host")
        self.master_port = self.get_option("master_port")


        if self.locustlog_file:
            logger.debug("######## DEBUG: configuring Locust resplog")
            ll.setup_resplogging(self.locustlog_level, self.locustlog_file)

    def get_options(self):
        options = {optname : self.__getattribute__(optname) for optname in self.get_available_options()}
        logger.debug("##### Locust plugin: get_options() : options = {}".format(options))
        return options

    def prepare_test(self):
        logger = logging.getLogger(__name__)


        try:
            logger.debug("######## DEBUG: looking for a console object")
            ### DEBUG: enable/disable Console
            console = self.core.get_plugin_of_type(ConsolePlugin)
        except Exception as ex:
            logger.debug("######## DEBUG: Console not found: %s", ex)
            console = None

        if console:
            logger.debug("######## DEBUG: console found")
            widget = LocustInfoWidget(self)
            console.add_info_widget(widget)
            logger.debug("######## DEBUG: locust widget added to console")
#            self.core.job.aggregator.add_result_listener(widget)
            logger.debug("######## DEBUG: add result listener")


        try:

            locustfile = lm.find_locustfile(self.locustfile)
            if not locustfile:
                logger.error("##### Locust plugin: Could not find any locustfile! Ensure file ends in '.py' and see --help for available options.")
                sys.exit(1)

            if locustfile == "locust.py":
                logger.error("##### Locust plugin: The locustfile must not be named `locust.py`. Please rename the file and try again.")
                sys.exit(1)

            docstring, locusts = lm.load_locustfile(locustfile)

            logger.info("##### Locust plugin: locustfile = {}".format(locustfile))

            if not locusts:
                logger.error("##### Locust plugin: No Locust class found!")
                sys.exit(1)
            else:
                logger.info("##### Locust plugin: Locust classes found in {} : {}".format(locustfile, locusts))

            self._locustclasses = list(locusts.values())
            options = Opts(**self.get_options())
            self._options = options
            logger.debug("##### Locust plugin: main() : options = {}".format(options))


        except Exception as e:
            logger.error("##### Locust plugin: prepare_test() CRITICAL ERROR : %s", e)
            sys.exit(1)


    def start_test(self):
#        # setup logging
#        ll.setup_logging(self.loglevel, self.logfile)
#        if self.locustlog_file:
#            ll.setup_resplogging(self.locustlog_level, self.locustlog_file)
        #logger = logging.getLogger(__name__)

        if self.show_version:
            logger.info("##### Locust plugin: Locust version = %s" % version)
            #print "Locust plugin: Locust version = %s".format(version)
            #sys.exit(0)

        # install SIGTERM handler
        def sig_term_handler():
            logger.info("##### Locust plugin: Got SIGTERM signal")
            self.shutdown(0)
            gevent.signal(signal.SIGTERM, sig_term_handler)

        try:
            logger.info("##### Locust plugin: Starting Locust %s" % version)

            # run the locust
            if self.csvfilebase:
                gevent.spawn(stats_writer, self.csvfilebase)

            if self.run_time:
                if not self.no_web:
                    logger.error("##### Locust plugin: The --run-time argument can only be used together with --no-web")
                    sys.exit(1)
                try:
                    self.run_time = parse_timespan(self.run_time)
                except ValueError:
                    logger.error("##### Locust plugin: Valid --time-limit formats are: 20, 20s, 3m, 2h, 1h20m, 3h30m10s, etc.")
                    sys.exit(1)
                def spawn_run_time_limit_greenlet():
                    logger.info("##### Locust plugin: Run time limit set to %s seconds" % self.run_time)
                    def timelimit_stop():
                        logger.info("##### Locust plugin: Time limit reached. Stopping Locust.")
                        self._locustrunner.quit()
                    gevent.spawn_later(self.run_time, timelimit_stop)





            # locust runner : web monitor
            if not self.no_web and not self.slave and self._locustrunner is None:
                # spawn web greenlet
                logger.info("##### Locust plugin: Starting web monitor at %s:%s" % (self.web_host or "*", self.port))
                main_greenlet = gevent.spawn(web.start, self._locustclasses, self._options)


            # locust runner : standalone
            if not self.master and not self.slave and self._locustrunner is None:
                logger.info("##### Locust plugin: LocalLocustRunner about to be launched")
                self._locustrunner = LocalLocustRunner(self._locustclasses, self._options)
                # spawn client spawning/hatching greenlet
                if self.no_web:
                    logger.info("##### Locust plugin: LocalLocustRunner.start_hatching()")
                    self._locustrunner.start_hatching(wait=True)
                    main_greenlet = self._locustrunner.greenlet
                if self.run_time:
                    logger.info("##### Locust plugin: spawn_run_time_limit_greenlet()")
                    spawn_run_time_limit_greenlet()
                    logger.info("##### Locust plugin: spawn_run_time_limit_greenlet() passed")

            # locust runner : master/slave mode (master here)
            elif self.master and self._locustrunner is None:
                self._locustrunner = MasterLocustRunner(self._locustclasses, self._options)
                if self.no_web:
                    while len(self._locustrunner.clients.ready) < self.expect_slaves:
                        logger.info("##### Locust plugin: Waiting for slaves to be ready, %s of %s connected",
                                     len(self._locustrunner.clients.ready), self.expect_slaves)
                        time.sleep(1)

                    self._locustrunner.start_hatching(self.num_clients, self.hatch_rate)
                    main_greenlet = self._locustrunner.greenlet
                    if self.run_time:
                        spawn_run_time_limit_greenlet()

            # locust runner : master/slave mode (slave here)
            elif self.slave and self._locustrunner is None:
                if self.run_time:
                    logger.error("##### Locust plugin: --run-time should be specified on the master node, and not on slave nodes")
                    sys.exit(1)
                try:
                    self._locustrunner = SlaveLocustRunner(self._locustclasses, self._options)
                    main_greenlet = self._locustrunner.greenlet
                except socket.error as e:
                    logger.error("##### Locust plugin: Failed to connect to the Locust master: %s", e)
                    sys.exit(-1)
            return self._locustrunner


            self._locustrunner.greenlet.join()
            code = 0
            if len(self._locustrunner.errors):
                code = 1
                self.shutdown(code=code)
        except KeyboardInterrupt as e:
            self.shutdown(0)


    def shutdown(self, code=0):
        """
        Shut down locust by firing quitting event, printing stats and exiting
        """

        logger.info("##### Locust plugin: Waiting 120 sec to aggregate latest data within Tank")

        if self._locustrunner is not None:
            #if self.csvfilebase:
            #    write_stat_csvs(self.csvfilebase)
            self._locustrunner.quit()
        events.quitting.fire(reverse=True)
        time.sleep(10)
        print_stats(self._locustrunner.request_stats)
        print_percentile_stats(self._locustrunner.request_stats)
        print_error_report()
        self.reader.close()
        logger.info("##### Locust plugin: Shutting down (exit code %s), bye." % code)
        sys.exit(code)

    def is_test_finished(self):
        """
        Fetch locustrunner stats: min/max/median/avg response time, current RPS, fail ratio
        """
        if self._locustrunner:
            self._locuststats = self._locustrunner.stats.total

        """
        Fetch locustrunner status: 'ready', 'hatching', 'running', 'stopped' and returns status code
        """
        logger.debug("######## DEBUG: is_test_finished()? -> Fetching locust status")
        logger.debug("######## DEBUG: is_test_finished() -> self._locustrunner.state = {}".format(self._locustrunner.state))
        self._state = self._locustrunner.state
        if self._locustrunner.state == 'stopped':
            self._user_count = 0
            return 0
        else:
            self._user_count = self._locustrunner.user_count
            return -1

    def end_test(self, retcode):
        if self.is_test_finished() < 0:
            logger.info("##### Locust plugin: Terminating Locust")
            self.shutdown(retcode)
        else:
            logger.info("##### Locust plugin: Locust has been terminated already")
            self.shutdown(retcode)
            sys.exit(retcode)
        return retcode

class Opts:
    def __init__(self, **entries):
        self.__dict__.update(entries)
        self.no_reset_stats = False
        self.reset_stats = True


class LocustInfoWidget(AbstractInfoWidget, AggregateResultListener):
    """ Right panel widget with Locust test info """

    def __init__(self, sender):
        AbstractInfoWidget.__init__(self)
        self.krutilka = ConsoleScreen.krutilka()
        self.owner = sender
        self.active_threads = 0
        self.RPS = 0

    def get_index(self):
        logger.debug('######## DEBUG: LocustInfoWidget get_index()')
        return 20

    def on_aggregated_data(self, data, stats):
        ### DEBUG
        self.active_threads = self.owner._user_count
        self.RPS = self.owner._locuststats.current_rps  # data['overall']['interval_real']['len']
        logger.debug('######## DEBUG: LocustInfoWidget on_aggregated_data(): %s' % str(stats))
        logger.debug('######## DEBUG: LocustInfoWidget on_aggregated_data() / self.RPS = %s' % str(self.RPS))

    def render(self, ConsoleScreen):
#        color_bg = ConsoleScreen.markup.BG_CYAN
        res = ''
        info_banner  = '########## LOCUST INFO ###########'
        stats_banner = '########## LOCUST STATS ##########'
        space = ConsoleScreen.right_panel_width - len(info_banner) - 1
        left_spaces = space / 2
        right_spaces = space / 2

        #dur_seconds = int(time.time()) - int(self.locust.start_time)
        #duration = str(datetime.timedelta(seconds=dur_seconds))

        info_template = ConsoleScreen.markup.GREEN + '#' * left_spaces + info_banner
        info_template += '#' * right_spaces + ConsoleScreen.markup.RESET + "\n"
        info_template += "\t## Target host: {}:{}\n".format(self.owner.host, self.owner.port)
        info_template += "\t## Max users: {}\n".format(self.owner.num_clients)
        info_template += "\t## Hatch rate: {}\n".format(self.owner.hatch_rate)
#       info_ template += "\t## Locust file: {}\n".format(self.owner.locustfile)
        info_template += "\t## Locust state: {}\n".format(self.owner._state)
        info_template += "\t## Active users: {}\n".format(self.active_threads)
#        info_template += "\n\n"
#        info_template += ConsoleScreen.markup.BG_CYAN + '#' * (ConsoleScreen.right_panel_width - 1) + '\n\n\n\n\n' #+ ConsoleScreen.markup.RESET

        res += "{}".format(info_template)

        stats_template = ConsoleScreen.markup.GREEN + '#' * left_spaces + stats_banner
        stats_template += '#' * right_spaces + ConsoleScreen.markup.RESET + "\n"
        stats_template += "\t## Current RPS: {0:.2f}\n".format(self.owner._locuststats.current_rps)
        stats_template += "\t## Fail ratio (%): {0:.2f}\n".format(100 * self.owner._locuststats.fail_ratio)
        stats_template += "\t## Min resp time (ms): {}\n".format(self.owner._locuststats.min_response_time)
        stats_template += "\t## Max resp time (ms): {}\n".format(self.owner._locuststats.max_response_time)
        stats_template += "\t## Average resp time (ms): {0:.2f}\n".format(self.owner._locuststats.avg_response_time)
        stats_template += "\t## Median resp time (ms): {}\n".format(self.owner._locuststats.median_response_time)
#        stats_template += "\n\n"
        stats_template += ConsoleScreen.markup.GREEN + '#' * (ConsoleScreen.right_panel_width - 1) + ConsoleScreen.markup.RESET

        res += "{}".format(stats_template)

        logger.debug('######## DEBUG: LocustInfoWidget render()')

        return res

#class UsedInstancesCriterion(AbstractCriterion):
#    """
#    Autostop criterion, based on active instances count
#    """
#    RC_INST = 24
#
#    @staticmethod
#    def get_type_string():
#        return 'instances'
#
#    def __init__(self, autostop, param_str):
#        AbstractCriterion.__init__(self)
#        self.seconds_count = 0
#        self.autostop = autostop
#        self.threads_limit = 1
#
#        level_str = param_str.split(',')[0].strip()
#        if level_str[-1:] == '%':
#            self.level = float(level_str[:-1]) / 100
#            self.is_relative = True
#        else:
#            self.level = int(level_str)
#            self.is_relative = False
#        self.seconds_limit = expand_to_seconds(param_str.split(',')[1])
#
#        try:
#            locust = autostop.core.get_plugin_of_type(Plugin)
#            info = locust.get_info()
#            if info:
#                self.threads_limit = info.instances
#            if not self.threads_limit:
#                raise ValueError(
#                    "Cannot create 'instances' criterion"
#                    " with zero instances limit")
#        except KeyError:
#            logger.warning("No locust module, 'instances' autostop disabled")
#
#    def notify(self, data, stat):
#        threads = stat["metrics"]["instances"]
#        if self.is_relative:
#            threads = float(threads) / self.threads_limit
#        if threads > self.level:
#            if not self.seconds_count:
#                self.cause_second = (data, stat)
#
#            logger.debug(self.explain())
#
#            self.seconds_count += 1
#            self.autostop.add_counting(self)
#            if self.seconds_count >= self.seconds_limit:
#                return True
#        else:
#            self.seconds_count = 0
#
#        return False
#
#    def get_rc(self):
#        return self.RC_INST
#
#    def get_level_str(self):
#        """
#        String value for instances level
#        """
#        if self.is_relative:
#            level_str = str(100 * self.level) + "%"
#        else:
#            level_str = self.level
#        return level_str
#
#    def explain(self):
#        items = (
#            self.get_level_str(), self.seconds_count,
#            self.cause_second[0].get('ts'))
#        return (
#            "Testing threads (instances) utilization"
#            " higher than %s for %ss, since %s" % items)
#
#    def widget_explain(self):
#        items = (self.get_level_str(), self.seconds_count, self.seconds_limit)
#        return "Instances >%s for %s/%ss" % items, float(
#            self.seconds_count) / self.seconds_limit
#
