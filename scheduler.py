# -*- coding: utf-8 -*-

import boto
import socket
import argparse

class Scheduler(object):
	def __init__(self, port):
		self.tasks = list()
		self.results = list()
		self.port = port
		self.clientIP = str()
		return

	def receiveTasks(self):
		print 'Waiting for the tasks from client...\n'
		
		scheduler_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		scheduler_socket.bind(('', self.port))
		scheduler_socket.listen(5)

		clientSock, addr = scheduler_socket.accept()
		self.clientIP = addr[0]

		task = str()
		while 1:
			char = clientSock.recv(1)
			if char == 'Q':
				break
			elif char == '\n':
				print '{} received.' .format(task)
				self.tasks.append(task.strip())
				task = str()
			else:
				task = task + str(char)

		print '\nAll {} tasks have been received from client.' .format(len(self.tasks))

		scheduler_socket.close()
		return

	def sendResults(self):
		print 'Sending results back to client...'
		print self.clientIP, self.port+1

		resultSent = list()

		scheduler_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		scheduler_socket.connect((self.clientIP, self.port + 100))
		scheduler_socket.send('testing')

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
