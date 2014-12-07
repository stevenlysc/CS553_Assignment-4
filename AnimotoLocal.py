# -*- coding: utf-8 -*-

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

class Animoto(object):
	def __init__(self):
		return

	def UploadVideo(self):
		print 'Uploading video to S3...'
		s3Conn = S3Connection()
		sqsConn = boto.s3.connect_to_region('us-west-2')
		resultQueue = sqsConn.get_queue('resultQueue')
		# Try to create a new bucket if it does not exist
		try:
			s3Conn.create_bucket('xingtanhu', location=Location.USWest2)
		except:
			print 'bucket already exists.'
		finally:
			bucket = s3Conn.get_bucket('xingtanhu')

		# Get file info
		source_paths = list()
		source_path_prefix = '/Users/WayneHu/Desktop/pic/'
		for item in os.listdir(source_path_prefix):
			if item.split('.')[1] == 'mkv' or item.split('.')[1] == 'MKV':
				source_paths.append(os.path.realpath(item))
				source_sizes.append(os.stat(os.path.realpath(item)).st_size)

		for source_path in source_paths:
			source_size = os.stat(os.path.realpath(source_path).st_size)
			# Create a multipart upload request, file name as the key_name
			mp = bucket.initiate_multipart_upload(os.path.basename(source_path))
			# Use a chunk size of 50 MiB
			chunk_size = 52428800
			chunk_count = int(math.ceil(source_size / chunk_size))
			# Send the file parts
			for i in range(chunk_count + 1):
				f_offset = chunk_size * i
				f_bytes = min(chunk_size, source_size - f_offset)
				with FileChunkIO(source_path, 'r', offset = f_offset, bytes = f_bytes) as fp:
					mp.upload_part_from_file(fp, part_num=i+1)
			# Finish the upload
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
					item_data = {'taskContent': taskContent}
					item = myTable.new_item(hash_key=taskId, attrs=item_data)
					item.put()
					# Execute task
					urls = task.split(' ')
					# Download image files
					for index in range(len(urls)):
						call('wget {} -O /Users/WayneHu/Desktop/pic/pic{}.jpg' .format(urls[index].strip(), str(index).zfill(3)), shell=True)
					# create animotp
					call('ffmpeg -i "/Users/WayneHu/Desktop/pic/pic%d.jpg" -c:v libx264 -preset ultrafast -qp 0 -filter:v "setpts=25.5*PTS" /Users/WayneHu/Desktop/pic/out{}.mkv' .format(str(index).zfill(3)))
		return

	def startAnimoto(self):
		self.generateVideo()
		self.uploadVideo()


if __name__ == '__main__':
	
	animoto = Animoto()
	animoto.startAnimoto()