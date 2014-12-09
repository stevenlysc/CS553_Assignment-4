# -*- coding: utf-8 -*-

import math
import boto.ec2
import boto.sqs
import boto.dynamodb
import argparse

class MonitorScheduler(object):
	def __init__(self, animoto):
		if not animoto:
			# Sleep
			self.ami = 'ami-9b7127ab'
		else:
			# Animoto
			self.ami = 'ami-91baeca1'
		return
	
	# Initialization SQS and DynamoDB and create EC2 instances
	def createDynamoDB(self):
		print 'Creating a table with DynamoDB...'
		#Set region
		dynamodbConn = boto.dynamodb.connect_to_region('us-west-2')
		#Create schema
		message_table_schema = dynamodbConn.create_schema(
			hash_key_name = 'task_id',
			hash_key_proto_value = str
		)
		try:
			#Create table using the schema
			myTable = dynamodbConn.create_table(
				name = 'MyTable',
				schema = message_table_schema,
				read_units = 100,
				write_units = 100
			)
			print '\tTable created successful!\n'
		except:
			print '\tMyTable already exists.\n'
		return

	#Method of creating SQS
	def createSQS(self):
		print 'Creating queue with SQS...'
		sqsConn = boto.sqs.connect_to_region('us-west-2')
		taskQueue = sqsConn.create_queue('taskQueue')
		resultQueue = sqsConn.create_queue('resultQueue')
		print '\tSQS queues created successful.\n'
		return
    
    #Method of creating EC2 instance
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

	#Method of geting length of the queue
	def getQueueLength(self):
		sqsConn = boto.sqs.connect_to_region('us-west-2')
		taskQueue = sqsConn.get_queue('taskQueue')
		return taskQueue.count()

	#According to the current length of the queue, launch instances dynamically
	def dynamicProvisioning(self):
		print 'Dynamic Provisioning...'
		ec2Conn = boto.ec2.connect_to_region('us-west-2')
		while 1:
			instances = 0
			reservations = ec2Conn.get_all_reservations()

			#Obtain current running or pending instances
			for res in reservations:
				for inst in res.instances:
					if inst.image_id == self.ami:
						if inst.state_code == 0 or inst.state_code == 16:
							instances += 1
			queueLen = self.getQueueLength()

			#Using log function, calculate the aim number of instances
			if not queueLen:
				aim_instances = 0
			else:
				aim_instances = int(math.log(queueLen, 2)) + 1

			#Print the current running (pending) instances and aim number of instances
			print instances, aim_instances
			if instances < aim_instances:
				self.createEC2(aim_instances - instances)
		return

	#Method of static provision, create given number of workers
	def staticProvisioning(self, nWorkers):
		print 'Static Provisioning...'
		ec2Conn = boto.ec2.connect_to_region('us-west-2')
		self.createEC2(nWorkers)
		return

if __name__ == '__main__':
	
	# monitor --static/--dynamic
	parser = argparse.ArgumentParser()
	group = parser.add_mutually_exclusive_group()
	group.add_argument('-sp', '--static', type=int, help='static provisioning')
	group.add_argument('-dp', '--dynamic', help='dynamic provisioning', action='store_true')

	parser.add_argument('-a', '--animoto', help='Animoto', action='store_true')

	args = parser.parse_args()

	# Always have an eye on SQS and implement dynamic provisioning
	monitorScheduler = MonitorScheduler(args.animoto)
	# Create SQS and DynamoDB
	monitorScheduler.createSQS()
	monitorScheduler.createDynamoDB()
	
	if args.dynamic:
		monitorScheduler.dynamicProvisioning()
	else:
		monitorScheduler.staticProvisioning(args.static)
	#monitorScheduler.createEC2(2)










