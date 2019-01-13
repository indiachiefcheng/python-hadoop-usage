#!/use/bin/env python
# -*- encoding: UTF-8 -*-

#author：chenglinguang
#date 20180708


import json
import time

from snakebite.client import HAClient
from snakebite.namenode import Namenode

n1 = Namenode("namenode-1", 8022)
n2 = Namenode("namenode-2", 8022)

#get the timestamp of now
now=time.time()
#get the timestamp of 30 days ago
thirty_day_ago=now - 30 * 24 * 60 * 60

#get the time stamp of 30 days ago with ms timestamp
millis_new=int(round(thirty_day_ago * 1000))
#print millis_new

#connect to the HA client of HDFS namenodes
client = HAClient([n1, n2], use_trash=True, sock_connect_timeout=50000, sock_request_timeout=50000)

for file in client.ls(["/user/spark/applicationHistory/"]):
	file_timestamp = file['access_time']
	file_path = file['path']
	print file_path
	if file_timestamp < millis_new:
		for p in client.delete([file_path], recurse=True):
			print p

