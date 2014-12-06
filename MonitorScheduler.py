# -*- coding: utf-8 -*-

import math
import boto.ec2
import boto.sqs
import boto.dynamodb

class MonitorScheduler(object):
	def __init__(self):
		return
	
	# Initialization SQS and DynamoDB and create EC2 instances
	def createDynamoDB(self):
		print 'Creating a table with DynamoDB...'
		dynamodbConn = boto.dynamodb.connect_to_region('us-west-2')

		myTable = dynamodbConn.get_table('MyTable')
		if myTable:
			print 'Table already exists.\n'
			return
		
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
		print 'Table created successful!\n'
		return

	def createSQS(self):
		print 'Creating queue with SQS...'
		sqsConn = boto.sqs.connect_to_region('us-west-2')
		taskQueue = sqsConn.create_queue('taskQueue')
		resultQueue = sqsConn.create_queue('resultQueue')
		print 'SQS created successful.\n'
		return

	def createEC2(self, count):
		print 'Creating instance...'
		ec2Conn = boto.ec2.connect_to_region('us-west-2')
		for i in range(count):
			ec2Conn.run_instances(
				'ami-df2076ef',
				key_name = 'PA4',
				instance_type = 't2.micro',
				security_groups = ['default']
			)
		print 'Instances created successful.\n'
		return

	def getQueueLength(self):
		sqsConn = boto.sqs.connect_to_region('us-west-2')
		taskQueue = sqsConn.get_queue('taskQueue')
		return taskQueue.count()

	def dynamicProvisioning(self):
		ec2Conn = boto.ec2.connect_to_region('us-west-2')
		while 1:
			instances = len(ec2Conn.get_all_reservations())
			queueLen = self.getQueueLength()
			aim_instances = int(math.log(queueLen, 2)) + 1
			print instances, aim_instances
			if instances < aim_instances:
				self.createEC2(aim_instances - instances)


if __name__ == '__main__':

	# Always have an eye on SQS and implement dynamic provisioning
	monitorScheduler = MonitorScheduler()
	# Create SQS and DynamoDB
	monitorScheduler.createSQS()
	monitorScheduler.createDynamoDB()
	
	monitorScheduler.dynamicProvisioning()
	#monitorScheduler.createEC2(2)










