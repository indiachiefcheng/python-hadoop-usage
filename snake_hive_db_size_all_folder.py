#!/use/bin/env python

import urllib,time
import urllib2
import string, datetime
import xml.sax
import xml.sax.handler
import sys,time,os,datetime
from subprocess import *
import threading
import socket
import requests
import json


from snakebite.client import HAClient
from snakebite.namenode import Namenode

n1 = Namenode("namenode-1", 8022)
n2 = Namenode("namenode-2", 8022)




ENDPOINT = "hive-db-monitor"    # Unique identifier
STEP = 600
FALCON_AGENT_URL = "http://192.168.17.13:1988/v1/push"

this_timestamp=int(time.time())
d = datetime.datetime.now()
item = {}
item['endpoint'] = 'hive-db-monitor-folder'
item['metric'] = ''
item['timestamp'] = this_timestamp
item['step'] = STEP
item['counterType'] = 'GAUGE'
item['tags'] = ''

folders = ['/user/spark','/yarn','/user/yarn','/user/airflow']

db_monitor_result=[]

#the function is to monitor the hive db size and push the request to falcon agent
def monitor_db_size():
    try:
        #connect to namenodeHA service with connect timeout setting and request timeout setting
        client = HAClient([n1, n2], use_trash=True, effective_user='hdfs', sock_connect_timeout=50000, sock_request_timeout=50000)
    except Exception,ex:
        pass
    for folder in folders:
        for db_contents in client.du([folder]):
            item['metric'] = db_contents['path']
            item['value'] = db_contents['length']
            #item['tags'] = db_contents['path'].split('/')[-1]
            db_monitor_result.append(item)
            requests.post(FALCON_AGENT_URL, data=json.dumps(db_monitor_result), timeout=10)

monitor_db_size()

