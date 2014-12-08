# -*- coding: utf-8 -*-

import time
import datetime
import math, os
import boto.s3
from boto.s3.connection import S3Connection, Location
from filechunkio import FileChunkIO
from subprocess import call

class Animoto(object):
	def __init__(self):
		return

	def uploadVideo(self):
		print 'Uploading video to S3...'
		s3Conn = S3Connection()
		sqsConn = boto.s3.connect_to_region('us-west-2')
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
		print 'prefix: {}' .format(source_path_prefix)
		for item in os.listdir(source_path_prefix):
			if item.split('.')[1] == 'txt' or item.split('.')[1] == 'TXT':
				source_paths.append(source_path_prefix + os.path.basename(item))
		for source_path in source_paths:
			print 'Path: {}' .format(source_path)
			source_size = os.stat(source_path).st_size
			print 'Size: {}' .format(source_size)
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
			print '\tURL: {}.\n' .format(url)
		return

	def generateVideo(self):
		call('sh /Users/WayneHu/Desktop/pic/list.sh', shell=True)
		# create animotp
		return

	def startAnimoto(self):
		self.generateVideo()
		self.uploadVideo()


if __name__ == '__main__':
	
	animoto = Animoto()
	animoto.startAnimoto()
