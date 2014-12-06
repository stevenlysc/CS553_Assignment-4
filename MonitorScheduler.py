# -*- coding: utf-8 -*-

import boto.ec2
import boto.sqs
import boto.dynamodb

class MonitorScheduler(object):
	def __init__(self):
		return
	
	# Initialization SQS and DynamoDB
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
				'ami-af26709f',
				key_name = 'Li',
				instance_type = 't2.micro',
				security_groups = ['default']
			)
		print 'Instance created successful.\n'
		return


if __name__ == '__main__':

	# Always have an eye on SQS and implement dynamic provisioning
	monitorScheduler = MonitorScheduler()
	# Create SQS and DynamoDB
	monitorScheduler.createSQS()
	monitorScheduler.createDynamoDB()
	monitorScheduler.createEC2(2)
