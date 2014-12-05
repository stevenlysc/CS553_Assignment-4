# -*- coding: utf-8 -*-

import boto
import socket
import argparse

class Scheduler(object):
	def __init__(self, port):
		self.tasks = list()
		self.resutls = list()
		self.port = port
		return

	def receiveTasks(self):
		print 'Waiting for the tasks from client...'
		
		scheduler_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		scheduler_socket.bind(('', self.port))
		scheduler_socket.listen(5)

		clientSock, addr = scheduler_socket.accept()
		print 'Connected by {}' .format(addr)

		while 1:
			task = clientSock.recv(2048)
			if task == 'End of task':
				break
			print '{} received.' .format(task)
			self.tasks.append(task.strip())

		print 'All {} tasks have been received from client.' .format(len(self.tasks))

		scheduler_socket.close()
		return

	def sendResults(self):
		print 'Sending results back to client...'
		
		resultSent = list()
		scheduler_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

		while 1:
			if len(self.results) > 0:
				for item in self.results:
					print 'Sending result of {}' .format(item)
					scheduler_socket.send(item)
					resultSent.append(item)
				for item in resultSent:
					self.results.remove(item)
				resultSent.clear()




if __name__ == '__main__':
	
	# scheduler -p <PORT> --local --remote
	parser = argparse.ArgumentParser()
	parser.add_argument('-p', metavar='PORT', type=int, required=True,
						help='the port used by socket.')
	group = parser.add_mutually_exclusive_group()
	group.add_argument('-lw', '--local', help='local worker', action='store_true')
	group.add_argument('-rw', '--remote', help='remote worker', action='store_true')
	
	args = parser.parse_args()

	port = args.p

	scheduler = Scheduler(port)
	if args.local:
		scheduler.receiveTasks()
		scheduler.sendResults()
		#scheduler.createLocalWorker()
	
	if args.remote:
		pass
		#scheduler.createRemoteWorker()
