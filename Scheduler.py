# -*- coding: utf-8 -*-

import boto
import socket
import argparse
from LocalWorker import LocalWorker

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

		print '\nAll {} tasks have been received from client.\n' .format(len(self.tasks))

		scheduler_socket.close()
		return

	def sendResults(self):
		print 'Sending results back to client...\n'

		resultSent = list()

		scheduler_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		scheduler_socket.connect((self.clientIP, self.port + 100))

		for result in self.results:
			msg = 'Receiving result: {}' .format(result)
			print 'Sending result: {}' .format(result)
			scheduler_socket.send('{}\n' .format(msg))

		scheduler_socket.send('Q')
		print 'All results have been sent to client successfully.\n'
		return
	
	def createLocalWorker(self, nWorkers):
		workers = list()
		
		for n in range(nWorkers):
			lw = LocalWorker(self.tasks[(n*len(self.tasks)/nWorkers):((n+1)*len(self.tasks)/nWorkers)])
			workers.append(lw)
			lw.start()

		for worker in workers:
			worker.join()

		for worker in workers:
			self.results += worker.results

		return


if __name__ == '__main__':
	
	# scheduler -p <PORT> --local --remote
	parser = argparse.ArgumentParser()
	parser.add_argument('-p', metavar='PORT', type=int, required=True,
						help='the port used by socket.')
	group = parser.add_mutually_exclusive_group()
	group.add_argument('-lw', '--local', type=int, help='local worker')
	group.add_argument('-rw', '--remote',type=int, help='remote worker')
	
	args = parser.parse_args()

	port = args.p

	scheduler = Scheduler(port)
	if args.local:
		scheduler.receiveTasks()
		scheduler.createLocalWorker(args.local)
		scheduler.sendResults()
	
	if args.remote:
		pass
		#scheduler.createRemoteWorker()
