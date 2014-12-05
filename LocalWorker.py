# -*- coding: utf-8 -*-

import time
from threading import Thread

class LocalWorker(Thread):
	def __init__(self, tasks):
		Thread.__init__(self)
		self.tasks = tasks
		self.results = list()
		return

	def run(self):
		for task in self.tasks:
			milliSleepTime = task.split(':')[1].strip().split(' ')[1]
			sleepTime = int(milliSleepTime) / 1000.0
			time.sleep(sleepTime)

			result = task + 'is done.'
			self.results.append(result)
		return
