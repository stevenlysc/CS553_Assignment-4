# -*- coding: utf-8 -*-

from LocalWorker import LocalWorker
import socket
import argparse
import boto.sqs
from boto.sqs.message import Message
import boto.dynamodb

#Class of front-end scheduler
class Scheduler(object):

	#Initializing...
	def __init__(self, port):
		self.tasks = list()
		self.results = list()
		self.port = port
		self.clientIP = str()
		return

	#Using socket, receive jobs from client
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
				print '\t{} received.' .format(task)
				self.tasks.append(task.strip())
				task = str()
			else:
				task = task + str(char)

		print 'All {} tasks have been received from client.\n' .format(len(self.tasks))

		scheduler_socket.close()
		return

	#Method of sending the result to client
	def sendResults(self):
		print 'Sending results back to client...'

		resultSent = list()

		scheduler_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		scheduler_socket.connect((self.clientIP, self.port + 100))

		for result in self.results:
			msg = 'Receiving result: {}' .format(result)
			print '\tSending result: {}' .format(result)
			scheduler_socket.send('{}\n' .format(msg))

		scheduler_socket.send('`')
		print 'All results have been sent to client successfully.\n'
		
		scheduler_socket.close()
		return
	
	#Method of creating workers using multi thread
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
	
	# Remote Worker
	def sendTaskToSQS(self):
		print 'Sending tasks to SQS...'
		sqsConn = boto.sqs.connect_to_region('us-west-2')
		taskQueue = sqsConn.get_queue('taskQueue')
		for task in self.tasks:
			print '\tSending {} to SQS' .format(task)
			msg = Message()
			msg.set_body(task)
			taskQueue.write(msg)
		print 'All tasks have been sent to SQS successfully.\n'
		return

	#Getting result from sqs
	def getResultFromSQS(self):
		print 'Retreiving results from SQS'
		sqsConn = boto.sqs.connect_to_region('us-west-2')
		resultQueue = sqsConn.get_queue('resultQueue')

		scheduler_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		scheduler_socket.connect((self.clientIP, self.port + 100))

		while len(self.results) < len(self.tasks):
			rs = resultQueue.get_messages(10)
			for result in rs:
				self.results.append(result.get_body())
				resultQueue.delete_message(result)
				print '\t{}' .format(result.get_body())
				msg = 'Receiving result: {}' .format(result.get_body())
				print '\tSending result: {}' .format(result.get_body())
				scheduler_socket.send('{}\n' .format(msg))
		print 'All results have been retreived from SQS.\n'
		scheduler_socket.send('`')
		scheduler_socket.close()
		return


if __name__ == '__main__':
	
	# scheduler -p <PORT> --local --remote
	parser = argparse.ArgumentParser()
	parser.add_argument('-p', metavar='PORT', type=int, required=True,
						help='the port used by socket.')
	group = parser.add_mutually_exclusive_group()
	group.add_argument('-lw', '--local', type=int, help='local worker')
	group.add_argument('-rw', '--remote', help='remote worker', action='store_true')
	
	args = parser.parse_args()

	port = args.p

	scheduler = Scheduler(port)
	if args.local:
		scheduler.receiveTasks()
		scheduler.createLocalWorker(args.local)
		scheduler.sendResults()
	
	if args.remote:
		scheduler.receiveTasks()
		scheduler.sendTaskToSQS()
		scheduler.getResultFromSQS()
