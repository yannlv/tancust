from pkg_resources import resource_string
from yandextank.plugins.Aggregator import AbstractReader, AggregatorPlugin, \
    AggregateResultListener, SecondAggregateDataItem
from yandextank.plugins.ConsoleOnline import \
    ConsoleOnlinePlugin, AbstractInfoWidget
from yandextank.core import AbstractPlugin
import yandextank.core as tankcore
import yandextank.plugins.ConsoleScreen as ConsoleScreen


from .locust import main as llm
import locust




#from locust import runners

#import gevent
import sys
#import os
#import signal
#import inspect
import logging
#import socket
#from optparse import OptionParser
#
#import locust.web 
from locust.log import setup_logging, console_logger
#from locust.stats import stats_printer, print_percentile_stats, print_error_report, print_stats
#from locust.inspectlocust import print_task_ratio, get_task_ratio_dict
#from locust.core import Locust, HttpLocust
##from locust.runners import MasterLocustRunner, SlaveLocustRunner, LocalLocustRunner
#import locust.events

#_internals = [Locust, HttpLocust]
#version = locust.__version__

class LocustPlugin(AbstractPlugin):

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
		self.num_clients = 1
		self.hatch_rate = 1
		self.num_requests = None
		self.loglevel = 'INFO'
		self.logfile = None
		self.print_stats = False
		self.only_summary = False
		self.list_commands = False
		self.show_task_ratio = False
		self.show_task_ratio_json = False
		self.show_version = False


	def get_available_options(self):
		return [
			"host", "port", "locustfile",
			"num_clients", "hatch_rate", "num_requests", "logfile", "loglevel"
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
		self.num_clients = self.get_option ("num_clients")
		self.hatch_rate = self.get_option("hatch_rate")
		self.num_requests = self.get_option("num_requests")
		self.logfile = self.get_option("logfile")
		self.loglevel = self.get_option("loglevel")


	def prepare_test(self):
		return None


	def start_test(self):
		# setup logging
		setup_logging(self.loglevel, self.logfile)
		logger = logging.getLogger(__name__)

		if self.show_version:
			print "Locust %s" % (version,)
			sys.exit(0)
		try:
			locustfile = llm.find_locustfile(self.locustfile)
			if not locustfile:
				logger.error("Could not find any locustfile! Ensure file ends in '.py' and see --help for available options.")
				sys.exit(1)

			docstring, locusts = llm.load_locustfile(locustfile)

			logger.info("locustfile : {}".format(locustfile))


#		logger.info("locustclass ? : {}".format(


		#if options.list_commands:
		#	console_logger.info("Available Locusts:")
		#	for name in locusts:
		#		console_logger.info("    " + name)
		#	sys.exit(0)

			if not locusts:
				logger.error("No Locust class found!")
				sys.exit(1)

		except Exception as e:
			logger.error("{}".format(e))
			sys.exit(1)

		# make sure specified Locust exists
		if arguments:
			missing = set(arguments) - set(locusts.keys())
			if missing:
				logger.error("Unknown Locust(s): %s\n" % (", ".join(missing)))
				sys.exit(1)
			else:
				names = set(arguments) & set(locusts.keys())
				locust_classes = [locusts[n] for n in names]
		else:
			locust_classes = locusts.values()




