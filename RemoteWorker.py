# -*- coding: utf-8 -*-

import commands
import boto.ec2
import boto.sqs
from boto.sqs.message import Message
import boto.dynamodb
import time
import datetime
import socket
import argparse
import math, os
import boto.s3
from boto.s3.connection import S3Connection, Location
from filechunkio import FileChunkIO
from subprocess import call

class RemoteWorker(object):
	def __init__(self):
		return

	def startSleep(self):
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

	def uploadVideo(self):
		print 'Uploading video to S3...'
		s3Conn = S3Connection()
		sqsConn = boto.sqs.connect_to_region('us-west-2')
		resultQueue = sqsConn.get_queue('resultQueue')
		# Try to create a new bucket if it does not exist
		try:
			s3Conn.create_bucket('boyangli', location=Location.USWest2)
		except:
			print 'bucket already exists.'
		finally:
			bucket = s3Conn.get_bucket('boyangli')

		# Get file info
		source_paths = list()
		source_path_prefix = '/home/ubuntu/CS553_Assignment4/pic/'
		for item in os.listdir(source_path_prefix):
			if item.split('.')[1] == 'mkv' or item.split('.')[1] == 'MKV':
				source_paths.append(source_path_prefix + os.path.basename(item))

		for source_path in source_paths:
			print source_path
			source_size = os.stat(source_path).st_size
			# Create a multipart upload request, file name as the key_name
			mp = bucket.initiate_multipart_upload(os.path.basename(source_path))
			# Use a chunk size of 50 MiB
			chunk_size = 5242880
			chunk_count = int(math.ceil(source_size / chunk_size))
			# Send the file parts
			for i in range(chunk_count + 1):
				f_offset = chunk_size * i
				print f_offset
				f_bytes = min(chunk_size, source_size - f_offset)
				print f_bytes
				with FileChunkIO(source_path, 'r', offset = f_offset, bytes = f_bytes) as fp:
					print 'waiting...'
					mp.upload_part_from_file(fp, part_num=i+1)
					print 'partially finished...'
			# Complete uploading
			mp.complete_upload()
			file_key = bucket.get_key(os.path.basename(source_path))
			# url is valid for 5 minutes
			url = file_key.generate_url(300)
			msg = Message()
			msg.set_body(url)
			resultQueue.write(msg)
			print '\tSending result ({}) to resultQueue.\n' .format(url)
		return

	def generateVideo(self):
		sqsConn = boto.sqs.connect_to_region('us-west-2')
		dynamodbConn = boto.dynamodb.connect_to_region('us-west-2')
		
		taskQueue = sqsConn.get_queue('taskQueue')
		myTable = dynamodbConn.get_table('MyTable')

		i = 0
		while 1:
			rs = taskQueue.get_messages()
			if len(rs) == 0:
				pass
			else:
				task = rs[0].get_body()
				taskId = str(task).split(':')[0]
				taskContent = str(task).split(':')[1]
				if myTable.has_item(hash_key=taskId):
					taskQueue.delete_message(rs[0])
				else:
					# Store into DynamoDB
					taskQueue.delete_message(rs[0])
					#command_line = '''#! /bin/sh
					#cd /home/ubuntu/CS553_Assignment4/pic
					#wget -i pic.txt >> ~/Log{}.txt
					#x=1; for i in *jpg; do counter=$(printf %d $x); ln -s '$i' /home/ubuntu/CS553_Assignment4/pic/pic'$counter'.jpg; x=$(($x+1)); done
					#ffmpeg -i 'pic%d.jpg' -c:v libx264 -preset ultrafast  -ap 0 -filter:v 'setpts=25.5*PTS' out{}.mkv >> ~/Log{}.txt
					#''' .format(str(i).zfill(3), str(i).zfill(3), str(i).zfill(3))
					call('sh /home/ubuntu/CS553_Assignment4/pic/list.sh {} >> ~/Log{}.txt' .format(str(i).zfill(3), str(i).zfill(3)), shell=True)
					i += 1
					self.uploadVideo()
		return

	def startAnimoto(self):
		self.generateVideo()



if __name__ == '__main__':

	# remoteWorker [--animoto]
	parser = argparse.ArgumentParser()
	parser.add_argument('-a', '--animoto', help='Animoto', action='store_true')

	args = parser.parse_args()
	
	remoteWorker = RemoteWorker()
	if args.animoto:
		remoteWorker.startAnimoto()
	else:
		remoteWorker.startSleep()
