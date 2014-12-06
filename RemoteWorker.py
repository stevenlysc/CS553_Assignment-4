# -*- coding: utf-8 -*-

import boto.ec2
import boto.sqs
from boto.sqs.message import Message
import boto.dynamodb
import time
import datetime
import socket
import time
class RemoteWorker(object):
	def __init__(self):
		return

	def start(self):
		sqsConn = boto.sqs.connect_to_region('us-west-2')
		dynamodbConn = boto.dynamodb.connect_to_region('us-west-2')
		ec2Conn = boto.ec2.connect_to_region('us-west-2')

		taskQueue = sqsConn.get_queue('taskQueue')
		resultQueue = sqsConn.get_queue('resultQueue')

		myTable = dynamodbConn.get_table('MyTable')
		
		flag = 0
		while 1:
			rs = taskQueue.get_messages()
			print len(rs)
			if not flag:
				startTime = datetime.datetime.now()
			# No task get from taskQueue
			if len(rs) == 0:
				flag = 1
				endTime = datetime.datetime.now()
				idleTime = (endTime - startTime).seconds
				if idleTime > 600:
					reservations = ec2Conn.get_all_reservations()
					for reservation in reservations:
						for instance in reservation.instances:
							localIP = socket.gethostbyname(socket.gethostname())
							if instance.private_ip_address == localIP:
								ec2Conn.terminate_instances(instance_ids=[instance.id])
					break
			# Get task from taskQueue
			else:
				task = rs[0].get_body()
				print task
				taskId = str(task).split(':')[0]
				taskContent = str(task).split(':')[1]
				if myTable.has_item(hash_key=taskId):
					taskQueue.delete_message(rs[0])
				else:
					# Store into DynamoDB
					print 'Storing task ({}) into DynamoDB...' .format(task)
					item_data = {'taskContent': taskContent}
					item = myTable.new_item(hash_key=taskId, attrs = item_data)
					item.put()
					# Execute task
					print 'Executing task ({})...' .format(task)
					milliSleepTime = taskContent.strip().split(' ')[1]
					sleepTime = float(milliSleepTime) / 1000.0
					time.sleep(sleepTime)
					# Delete task from SQS
					print 'Deleting task ({}) from taskQueue...' .format(task)
					taskQueue.delete_message(rs[0])
					# Sent result to SQS
					result = task + ' is done.'
					msg = Message()
					msg.set_body(result)
					resultQueue.write(msg)
					print 'Sending result ({}) to resultQueue...\n\n' .format(result)
		return


if __name__ == '__main__':
	
	remoteWorker = RemoteWorker()
	remoteWorker.start()
