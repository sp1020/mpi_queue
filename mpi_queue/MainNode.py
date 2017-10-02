from mpi4py import MPI
import time 

class _task_wrapper:
	""" A wrapper for result object """
	def __init__(self, function, args):
		self.function = function 
		self.args = args

class MainNode:
	""" MainNode module

	Main nodes
	"""

	def __init__(self, function, args=[], function_init='', 
				 function_analysis='', debug=False):
		""" Main node cycle 

		Parameters
		==========
		function : 
		   The main calculation function 

		function_init : 
		   Initialization function

		function_analysis : 
		   A function executed when a result is executed.

		Variables (Results)
		===================
		queues : 
		   A list of queues 

		results: 
		   The stored results 

		errors: 
		   The stored errors 
		"""

		# task and queues
		self.function = function 
		self.function_init = function_init
		self.function_analysis = function_analysis
		self.queues = args 

		# connection (MPI)
		self.comm = MPI.COMM_WORLD
		self.size = self.comm.Get_size()
		self.rank = self.comm.Get_rank()
		self.status = MPI.Status()

		# internal parameters
		self.control_signal = ''
		self.wait_time = 5
		self.debug = debug
		self.working_node = {}

		# result variable 
		self.results = []
		self.errors = []

		# initialize 
		if self.function_init != '':
			self.function_init(self)

	def execute(self):
		""" Main node cycle 

		"""
		
		# main-node cycle 
		while True:
			# receive sub-node status 
			signal, src = self._receive_status_signal()
			if signal == 'ready':
				self._send_task(src)
			elif signal == 'done':
				self._receive_result()
			elif signal == 'error':
				self._error_treatment()

			if len(self.working_node) == 0 and len(self.queues) == 0:
				break

	def terminate(self):
		""" Termination code 
		
		Check whether queues are empty and all jobs are finished 
		"""
		while True:
			if self._finish_condition():
				break
			
	def _receive_status_signal(self):
		signal = self.comm.recv(source=MPI.ANY_SOURCE, tag=0, 
								status=self.status)
		src = self.status.source
		if self.debug:
			print '[main] receive status signal %s from %s'%(signal, src)
		return (signal, src)

	def _send_control_signal(self, dest, signal):
		self.comm.send(signal, dest=dest, tag=0)

	def _send_task(self, src):
		if self.queues == []:
			print '[main] empty queue'
			self._send_control_signal(src, 'wait')
		else:
			args = self.queues[0]
			self.queues = self.queues[1:]
			
			self._send_control_signal(src, 'task')
			self.comm.send(_task_wrapper(self.function, args), dest=src, tag=1)
			self.working_node[src] = time.time()
	
	def _receive_result(self):
		result = self.comm.recv(source=self.status.Get_source(), tag=2, 
								status=self.status)
		src = self.status.source
		self.working_node.pop(src)
		if self.debug:
			print '[main] Result from node %s: %s'%(src, result.result)
		if self.function_analysis != '':
			self.function_analysis(self, result.result)
		else:
			self.results.append(result.result)

	def _error_treatment(self):
		task = self.comm.recv(source=self.status.Get_source(), tag=2, 
							  status=self.status)
		src = self.status.source
		self.working_node.pop(src)
		self.errors.append(task.args)

	def _finish_condition(self):
		if self.queues != []:
			return False 
		if len(self.working_node) != 0:
			return False

		if self.debug:
			print '[main] All tasks finished, execute termination code'

		for i in range(self.size):
			if i != 0: 
				if self.debug:
					print '[main] Send termination signal to node %d'%i
				self._send_control_signal(i, 'end')
		
		return True 
