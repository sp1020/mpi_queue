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
		if self.rank == 0: 
			self.flag_main = True
		else:
			self.flag_main = False

		self.debug = debug
	
	def set_function(self, function):
		if self.flag_main:
			self.function = function

	def set_args(self, args):
		if self.flag_main:
			self.args = args
		
	def execute(self):
		if self.flag_main:
			self.node = MainNode.MainNode(self.function, self.args, 
										  debug=self.debug)
			self.results = self.node.results
			self.errors = self.node.errors
		else:
			self.node = SubNode.SubNode(debug=self.debug)

