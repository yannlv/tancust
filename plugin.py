from pkg_resources import resource_string

from yandextank.common.interfaces import AbstractPlugin, AggregateResultListener, AbstractInfoWidget, GeneratorPlugin, AbstractCriterion
from yandextank.plugins.Console import Plugin as ConsolePlugin
from yandextank.plugins.Autostop import Plugin as AutostopPlugin
from yandextank.plugins.Console import screen as ConsoleScreen

from yandextank.common.util import splitstring
from .reader import LocustReader #, LocustStatsReader
from yandextank.stepper import StepperWrapper

from locust import runners as lr
from locust import main as lm
from locust import log as ll
import locust


import gevent
import sys
import subprocess
import socket
import logging
import time

from locust.stats import stats_printer, stats_writer, print_percentile_stats, print_error_report, print_stats #,write_stat_csvs
from  locust.runners import MasterLocustRunner, SlaveLocustRunner, LocalLocustRunner
import locust.events as events
from locust.util.time import parse_timespan

import tempfile

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
        self._locustslaves = None
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
        self.run_time = None
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
        self.locustlog_level = 'INFO'
        self.cfg = cfg

        # setup logging
        ll.setup_logging(self.loglevel, self.logfile)

    @property
    def locustlog_file(self):
        logger.debug("######## DEBUG: self.core.artifacts_dir = {}".format(self.core.artifacts_dir))
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
        self.run_time = self.get_option("run_time")
        self.logfile = self.get_option("logfile")
        self.loglevel = self.get_option("loglevel")
        self.csvfilebase = self.get_option("csvfilebase")
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


    def is_any_slave_up(self):
        if self.master and self._locustslaves is not None:
            poll_slaves = [s.poll() for s in self._locustslaves]
            res = any([False if x is not None else True for x in poll_slaves])
            logger.debug("######## DEBUG: is_any_slave_up/any(res) = {}".format(res))
            return res
        elif self.master:
            logger.error("##### Locust plugin: no slave alive to poll")
            return False
        else:
            return False


    def start_test(self):

        # install SIGTERM handler
        def sig_term_handler():
            logger.info("##### Locust plugin: Got SIGTERM signal")
            self.shutdown(0)
            gevent.signal(signal.SIGTERM, sig_term_handler)

        def spawn_local_slaves(count):
            """
            Spawn *local* locust slaves : data aggregation will NOT work with *remote* slaves
            """
            try:
                args = ['locust']
                args.append('--locustfile={}'.format(str(self.locustfile)))
                args.append('--slave')
                args.append('--master-host={}'.format(self.master_host))
                args.append('--master-port={}'.format(self.master_port))
                args.append('--resplogfile={}'.format(self.locustlog_file))
                logger.info("##### Locust plugin: slave args = {}".format(args))

                # Spawning the slaves in shell processes (security warning with the use of 'shell=True')
                self._locustslaves = [subprocess.Popen(' '.join(args), shell=True, stdin=None,
                            stdout=open('{}/locust-slave-{}.log'.format(self.core.artifacts_dir, i), 'w'),
                            stderr=subprocess.STDOUT) for i in range(count)]
                #slaves = [SlaveLocustRunner(self._locustclasses, self._options) for _ in range(count)] # <-- WRONG: This will spawn slave running on the same CPU core as master
                time.sleep(1)

                logger.info("##### Locust plugin: Started {} new locust slave(s)".format(len(self._locustslaves)))
                logger.info("##### Locust plugin: locust slave(s) PID = {}".format(self._locustslaves))
            except socket.error as e:
                logger.error("##### Locust plugin: Failed to connect to the Locust master: %s", e)
                sys.exit(-1)
            except Exception as e:
                logger.error("##### Locust plugin: Failed to spawn locust slaves: %s", e)
                sys.exit(-1)

        try:
            logger.info("##### Locust plugin: Starting Locust %s" % version)

            # run the locust 

            ### FIXME
            #if self.csvfilebase:
            #    gevent.spawn(stats_writer, self.csvfilebase)
            ### /FIXME

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
                        logger.debug("######## DEBUG: timelimit_stop()/self._locustrunner.quit() passed")
                    def on_greenlet_completion():
                        logger.debug("######## DEBUG: Locust plugin: on_greenlet_completion()")

                    #gevent.spawn_later(self.run_time, timelimit_stop)
                    gl = gevent.spawn_later(self.run_time, timelimit_stop)
                    # linking timelimit greenlet to main greenlet and get a feedback of its execution
                    #gl.link(on_greenlet_completion)



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
                logger.info("##### Locust plugin: MasterLocustRunner started")
                time.sleep(1)
                if self.no_web:
                    gevent.spawn(spawn_local_slaves(self.expect_slaves))
                    while len(self._locustrunner.clients.ready) < self.expect_slaves:
                        logger.info("##### Locust plugin: Waiting for slaves to be ready, %s of %s connected",
                                     len(self._locustrunner.clients.ready), self.expect_slaves)
                        time.sleep(1)
                    self._locustrunner.start_hatching(self.num_clients, self.hatch_rate)
                    logger.debug("######## DEBUG: MasterLocustRunner/start_hatching()")
                    main_greenlet = self._locustrunner.greenlet
                if self.run_time:
                    try:
                        spawn_run_time_limit_greenlet()
                    except Exception as e:
                        logger.error("##### Locust plugin: exception raised in spawn_run_time_limit_greenlet() = {}".format(e))

            # locust runner : master/slave mode (slave here)
            #elif self.slave and self._locustrunner is None:
            #    if self.run_time:
            #        logger.error("##### Locust plugin: --run-time should be specified on the master node, and not on slave nodes")
            #        sys.exit(1)
            #    try:
            #        self._locustrunner = SlaveLocustRunner(self._locustclasses, self._options)
            #        main_greenlet = self._locustrunner.greenlet
            #    except socket.error as e:
            #        logger.error("##### Locust plugin: Failed to connect to the Locust master: %s", e)
            #        sys.exit(-1)
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


        logger.debug("######## DEBUG: shutdown()/_locustrunner = {}".format(self._locustrunner))
        logger.info("##### Locust plugin: Cleaning up runner...")
        if self._locustrunner is not None and self.is_any_slave_up():
            #if self.csvfilebase:
            #    write_stat_csvs(self.csvfilebase)
            retcode = self._locustrunner.quit()
            logger.debug("######## DEBUG: shutdown()/_locustrunner.quit() passed # retcode = {}".format(retcode))
        logger.info("##### Locust plugin: Running teardowns...")

        while not self.reader.is_stat_queue_empty():
            logger.info("##### Locust plugin: {} items remaining is stats queue".format(self.reader.stat_queue.qsize()))
            time.sleep(1)

        ### FIXME : possibly causing a greenlet looping infinitely
        #events.quitting.fire(reverse=True)
        print_stats(self._locustrunner.request_stats)
        print_percentile_stats(self._locustrunner.request_stats)
        print_error_report()
        self.reader.close()
        logger.info("##### Locust plugin: Shutting down (exit code %s), bye." % code)

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
        logger.debug("######## DEBUG: is_test_finished() -> is_any_slave_up() = {}".format(self.is_any_slave_up()))
        self._state = self._locustrunner.state
        if self._locustrunner.state == 'stopped' or self.master and not self.is_any_slave_up():
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
        return retcode

class Opts:
    def __init__(self, **entries):
        self.__dict__.update(entries)
        self.no_reset_stats = True
        self.reset_stats = False


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
        return 0

    def on_aggregated_data(self, data, stats):
        ### DEBUG
        self.active_threads = self.owner._user_count
        self.RPS = self.owner._locuststats.current_rps  # data['overall']['interval_real']['len']
        logger.debug('######## DEBUG: LocustInfoWidget on_aggregated_data(): %s' % str(stats))
        logger.debug('######## DEBUG: LocustInfoWidget on_aggregated_data() / self.RPS = %s' % str(self.RPS))

    def render(self, ConsoleScreen):
#        color_bg = ConsoleScreen.markup.BG_CYAN
        res = ""
        info_banner  = "########## LOCUST INFO ###########"
        stats_banner = "########## LOCUST STATS ##########"
        space = ConsoleScreen.right_panel_width - len(info_banner) - 1
        left_spaces = space / 2
        right_spaces = space / 2

        #dur_seconds = int(time.time()) - int(self.locust.start_time)
        #duration = str(datetime.timedelta(seconds=dur_seconds))

        info_template = ConsoleScreen.markup.GREEN + '#' * left_spaces + info_banner
        info_template += "#" * right_spaces + ConsoleScreen.markup.RESET + "\n"
        info_template += "\t## Target host: {}\n".format(self.owner.host)
        info_template += "\t## Max users: {}\n".format(self.owner.num_clients)
        info_template += "\t## Hatch rate: {}\n".format(self.owner.hatch_rate)
#       info_ template += "\t## Locust file: {}\n".format(self.owner.locustfile)
        info_template += "\t## Locust state: {}\n".format(self.owner._state)
        info_template += "\t## Active users: {}\n".format(self.owner._user_count)
        if self.owner.master:
            info_template += "\t## Active slaves: {}\n".format(len(self.owner._locustrunner.clients.ready)
                                                               + len(self.owner._locustrunner.clients.hatching)
                                                               + len (self.owner._locustrunner.clients.running)
                                                              ) 
#        info_template += "\n\n"
#        info_template += ConsoleScreen.markup.BG_CYAN + '#' * (ConsoleScreen.right_panel_width - 1) + '\n\n\n\n\n' #+ ConsoleScreen.markup.RESET

        res += "{}".format(info_template)

        stats_template = ConsoleScreen.markup.GREEN + "#" * left_spaces + stats_banner
        stats_template += "#" * right_spaces + ConsoleScreen.markup.RESET + "\n"
        stats_template += "\t## Current RPS: {0:.2f}\n".format(self.owner._locuststats.current_rps)
        stats_template += "\t## Fail ratio (%): {0:.2f}\n".format(100 * self.owner._locuststats.fail_ratio)
        stats_template += "\t## Min resp time (ms): {}\n".format(self.owner._locuststats.min_response_time)
        stats_template += "\t## Max resp time (ms): {}\n".format(self.owner._locuststats.max_response_time)
        stats_template += "\t## Average resp time (ms): {0:.2f}\n".format(self.owner._locuststats.avg_response_time)
        stats_template += "\t## Median resp time (ms): {}\n".format(self.owner._locuststats.median_response_time)
        stats_template += ConsoleScreen.markup.GREEN + '#' * (ConsoleScreen.right_panel_width - 1) + ConsoleScreen.markup.RESET

        res += "{}".format(stats_template)

        logger.debug('######## DEBUG: LocustInfoWidget render()')

        return res

