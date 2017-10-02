""" MPI queue module 

This module sequentially distribute jobs to sub-nodes. Specifically, a function 
and a collection of parameters will be distributed to the sub-nodes. 

Main node
---------

In the main node, a FUNCTION and a list of argument sets should be defined, and 
given as its input. The results of the functions will be returned to the main 
node and stored in the 'results' list. When a function execution was not 
successful, its arguments will be returned to the main node and stored in 
'errors' list. 

   .set_function(function=FUNCTION)
   .set_arguments(args=ARGS)
   .results  # list of results 
   .errors   # list of errors (the arguments)

   CAUTION: a argument set should be a list. 

Sub node
---------

In the sub node the FUNCTION will be executed for single argument set and the 
result will be returned. 



Example
-------
Default queue (test.py)::

   import mpi_queue 
   import time

   def add(a, b): return a+b

   mq = mpi_queue.mpi_queue()
   if mq.flag_main:
       data = [[1,2], [3,4], [5,6]]
       mq.set_function(add)
       mq.set_args(data)
       mq.execute()

       time.sleep(1)
       print 'RESULTS:', mq.results
       print 'ERRORS:', mq.errors
   else:
       mq.execute()

The script should be run with mpirun 

eg)
$ mpirun -np 4 python test.py 


Todo
----
List of updates to be made

* Allow a single argument instead of using list. 


"""
from mpi4py import MPI
import MainNode
import SubNode

class mpi_queue:
	def __init__(self, debug=False):
		# connection (MPI)
		self.comm = MPI.COMM_WORLD
		self.size = self.comm.Get_size()
		self.rank = self.comm.Get_rank()
		self.function_init = ''
		self.function_analysis = ''

		self.flag_initialize = False 
		self.flag_terminate = False

		# argument to be passed to the ''function'' 
		self.args = [] 

		self.results = []
		self.errors = []
		if self.rank == 0: 
			self.flag_main = True
		else:
			self.flag_main = False

		self.debug = debug

	def __del__(self):
		if self.flag_main:
			if not self.flag_terminate:
				self.terminate()
	
	def set_function(self, function, function_init='', function_analysis=''):
		""" Set functions 
		
		Parameters
		==========
		function : 
		   The main calculation function 

		function_init : 
		   Initialization function

		function_analysis : 
		   A function executed when a result is executed.

		(CAUTION) The functions are defined only for the main node. 
		"""
		if self.flag_main:
			self.function = function
			if function_init != '':
				self.function_init = function_init
			if function_analysis != '':
				self.function_analysis = function_analysis

	def set_args(self, args):
		""" Set queues 

		Parameters
		==========
		args : 
		   List of arguments (arguments = a list of arguments)
		   args should be a list of list. 
		
		Example
		=======
		eg) single argument case
		   args =[[1], [2], [3]]

		eg) multiple arguments case
		   args = [[1,2], [2,3], [3,4]]
		"""
		if self.flag_main:
			# check 
			if type(args) != list: 
				print '[set_args] The argument should be a list of arguments'
				self.terminate()
				exit()
			if len(args) == 0: 
				print '[set_args] One or more  argument is required.'
				self.terminate()
				exit()
			if type(args[0]) != list:
				print '[set_argf] The argument should be a list)'
				print '           args is a list of lists.'
				self.terminate()
				exit()

			# set arguments 
			self.args = args 
			self.node.queues = args

	def initialize(self):
		if self.flag_main:
			self.flag_initialize=True
			self.node = MainNode.MainNode(self.function, args = self.args, 
										  function_init=self.function_init,
										  function_analysis=self.function_analysis,
										  debug=self.debug)
		
	def execute(self):
		""" Main job execution 

		Main node : 
		   If the job is not initialized, initialize the job 
		
		"""
		if self.flag_main:
			if not self.flag_initialize:
				self.initialize()
			self.node.execute()
			self.results += self.node.results
			self.errors += self.node.errors
		else:
			self.node = SubNode.SubNode(debug=self.debug)

	def terminate(self):
		if self.flag_main:
			self.node.terminate()
