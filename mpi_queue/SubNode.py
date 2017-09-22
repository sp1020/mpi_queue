from mpi4py import MPI
import time 

class _result_wrapper:
	""" A wrapper for result object """
	def __init__(self, result):
		self.result = result

class SubNode:
	""" SubNode module 

	SubNode cycle 
	

	"""

	def __init__(self, debug=False):
		""" SubNode cycle 
		
		"""
		# connection (MPI)
		self.comm = MPI.COMM_WORLD
		self.size = self.comm.Get_size()
		self.rank = self.comm.Get_rank()

		# internal parameters
		self.control_signal = ''
		self.wait_time = 5
		self.debug = debug

		# sub-node cycle 
		while True:
			# send standby signal 
			self._send_status_signal('ready')
			
			# receive control signal 
			self._receive_control_signal()

			# control 
			if self.control_signal == 'wait': 
				if self.debug:
					print '[node %s] receiving WAIT signal'%self.rank
				self._wait()
			elif self.control_signal == 'end':
				if self.debug:
					print '[node %s] receiving END signal'%self.rank
				break
			elif self.control_signal == 'task':
				if self.debug:
					print '[node %s] receiving TASK signal'%self.rank
				self._execute_task()
		print '[node %s] TERMINATED'%self.rank

	def _send_status_signal(self, signal):
		""" Send status signal to the main node"""
		if self.debug:
			print '[node %d] send status signal: %s' %(self.rank, signal)
		self.comm.send(signal, dest=0, tag=0)

	def _receive_control_signal(self):
		""" Receive control signal from the main node """
		self.control_signal = self.comm.recv(source=0, tag=0)
		if self.debug:
			print '[node %d] receive control signal: %s' %(self.rank, 
														   self.control_signal)

	def _wait(self):
		""" Wait """
		if self.debug:
			print '[node %d] wait'%self.rank
		time.sleep(self.wait_time)

	def _execute_task(self):
		""" Receive task """
		task = self.comm.recv(source=0, tag=1)
		if self.debug:
			print '[node %d] execute task: %s'%(self.rank, task.args)
		flag = True
		try: 
			res = task.function(*task.args)
			if self.debug:
				print '[node %d] task done: %s'%(self.rank, task.args)
		except:
			flag = False
		if flag:
			self._send_status_signal('done')
			self.comm.send(_result_wrapper(res), dest=0, tag=2)
		else:
			self._send_status_signal('error')
			self.comm.send(task, dest=0, tag=2)



