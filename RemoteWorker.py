# -*- coding: utf-8 -*-

import boto.ec2
import boto.sqs
from boto.sqs.message import Message
import boto.dynamodb
import time
import datetime
import socket

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
			if not flag:
				startTime = datetime.datetime.now()
			if not len(rs):
				flag = 1
				endTime = datetime.datetime.now()
				idleTime = (endTime - startTime).seconds
				if idleTime > 20:
					reservations = ec2Conn.get_all_reservations()
					instances = reservations[0].instances
					localIP = socket.gethostbyname(socket.gethostname())
					for instance in instances:
						if instance.private_ip_address == localIP:
							ec2Conn.terminate_instances(instance_ids=[instance.id])
					break
			else:
				task = rs[0].get_body()
				taskId = str(task).split(':')[0]
				taskContent = str(task).split(':')[1]
				if myTable.get_item(hash_key=taskId):
					pass
				else:
					# Store into DynamoDB
					item = table.new_item(hash_key=taskId, range_key=taskContent)
					item.put()
					# Execute task
					milliSleepTime = taskContent.split(' ')[1]
					sleepTime = float(milliSleepTime) / 1000.0
					time.sleep(sleepTime)
					# Delete task from SQS
					taskQueue.delete_message(rs[0])
					# Sent result to SQS
					result = task + 'is done.'
					msg = Message()
					msg.set_body(result)
					resultQueue.write(msg)
		return


if __name__ == '__main__':
	
	remoteWorker = RemoteWorker()
	remoteWorker.start()
