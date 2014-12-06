# -*- coding: utf-8 -*-

from LocalWorker import LocalWorker
import socket
import argparse
import boto.sqs
from boto.sqs.message import Message
import boto.dynamodb

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

	def getResultFromSQS(self):
		print 'Retreiving results from SQS\n'
		sqsConn = boto.sqs.connect_to_region('us-west-2')
		while len(self.results) < len(self.tasks):
			resultQueue = sqsConn.get_queue('resultQueue')
			results = resultQueue.get_messages()
			for result in results:
				if not result in self.results:
					self.results.append(result.get_body())
		return

	def createDynamoDB(self):
		print 'Creating a table with DynamoDB...'
		dynamodbConn = boto.dynamodb.connect_to_region('us-west-2')
		message_table_schema = dynamodbConn.create_schema(
			hash_key_name = 'task_id',
			hash_key_proto_value = str,
			range_key_name = 'task_content',
			range_key_proto_value = str
		)
		myTable = dynamodbConn.create_table(
			name = 'MyTable',
			schema = message_table_schema,
			read_units = 10,
			write_units = 10
		)
		print 'Table created successful!'
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
		scheduler.receiveTasks()
		scheduler.createDynamoDB()
		scheduler.sendTaskToSQS()
		scheduler.getResultFromSQS()
