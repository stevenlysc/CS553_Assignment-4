# -*- coding: utf-8 -*-

import math
import boto.ec2
import boto.sqs
import boto.dynamodb
import argparse

class MonitorScheduler(object):
	def __init__(self):
		self.ami = 'ami-73510743'
		return
	
	# Initialization SQS and DynamoDB and create EC2 instances
	def createDynamoDB(self):
		print 'Creating a table with DynamoDB...'
		dynamodbConn = boto.dynamodb.connect_to_region('us-west-2')
		message_table_schema = dynamodbConn.create_schema(
			hash_key_name = 'task_id',
			hash_key_proto_value = str
		)
		try:
			myTable = dynamodbConn.create_table(
				name = 'MyTable',
				schema = message_table_schema,
				read_units = 10,
				write_units = 10
			)
			print '\tTable created successful!\n'
		except:
			print '\tMyTable already exists.\n'
		return

	def createSQS(self):
		print 'Creating queue with SQS...'
		sqsConn = boto.sqs.connect_to_region('us-west-2')
		taskQueue = sqsConn.create_queue('taskQueue')
		resultQueue = sqsConn.create_queue('resultQueue')
		print '\tSQS queues created successful.\n'
		return

	def createEC2(self, count):
		print 'Creating instance...'
		ec2Conn = boto.ec2.connect_to_region('us-west-2')
		for i in range(count):
			ec2Conn.run_instances(
				self.ami,
				key_name = 'PA4',
				instance_type = 't2.small',
				security_groups = ['swift_security_group1']
			)
		print '\t{} Instances are created successful.\n' .format(count)
		return

	def getQueueLength(self):
		sqsConn = boto.sqs.connect_to_region('us-west-2')
		taskQueue = sqsConn.get_queue('taskQueue')
		return taskQueue.count()

	def dynamicProvisioning(self):
		ec2Conn = boto.ec2.connect_to_region('us-west-2')
		while 1:
			instances = 0
			reservations = ec2Conn.get_all_reservations()
			for res in reservations:
				for inst in res.instances:
					if inst.image_id == self.ami:
						if inst.state_code == 0 or inst.state_code == 16:
							instances += 1
			queueLen = self.getQueueLength()
			if not queueLen:
				aim_instances = 0
			else:
				aim_instances = int(math.log(queueLen, 2)) + 1
			print instances, aim_instances
			if instances < aim_instances:
				self.createEC2(aim_instances - instances)
		return

	def staticProvisioning(self, nWorkers):
		ec2Conn = boto.ec2.connect_to_region('us-west-2')
		self.createEC2(nWorkers)
		return

if __name__ == '__main__':
	
	# monitor --static/--dynamic
	parser = argparse.ArgumentParser()
	group = parser.add_mutually_exclusive_group()
	group.add_argument('-sp', '--static', type=int, help='static provisioning')
	group.add_argument('-dp', '--dynamic', help='dynamic provisioning', action='store_true')

	args = parser.parse_args()

	# Always have an eye on SQS and implement dynamic provisioning
	monitorScheduler = MonitorScheduler()
	# Create SQS and DynamoDB
	monitorScheduler.createSQS()
	monitorScheduler.createDynamoDB()
	
	if args.dynamic:
		monitorScheduler.dynamicProvisioning()
	else:
		monitorScheduler.staticProvisioning(args.static)
	#monitorScheduler.createEC2(2)










