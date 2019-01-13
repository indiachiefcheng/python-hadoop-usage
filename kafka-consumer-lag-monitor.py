#!/usr/bin/python
#!-*- coding:utf8 -*-

#Author：chenglinguang
#date 20170407



import os, sys, time, json, requests
from kazoo.client import KazooClient
from kazoo.exceptions import NoNodeError
from kafka import KafkaClient
from kafka.protocol.offset import OffsetRequest
from kafka.common import OffsetRequestPayload

TOPIC_LIST = ["ch_aaa","ch_adeviewlog","ch_carousels","ch_edu","ch_edus","ch_epgviewlog","ch_jxg","ch_mapp","ch_mapr","ch_mobilelog","ch_mter","ch_pay","ch_pf_messageviewlog","ch_saircon","ch_saircons","ch_sdpsviewlog","ch_stvchannel","ch_tdedus","ch_terminationlog","ch_vidaa5laucher","ch_videocall","ch_vod","ch_vods","ch_wx","ch_wxter","json.vod","json.exception.vod","json.exception.ad","json.ter.unifiedapp"]  # Topics for MirrorMaker to consume
CONSUMER_GROUPS = ["channel-stc-watcher", "channel-stc-watcher-redisaaa"]  # MirrorMaker's consumer-group-id
KAFKA_HOSTS = 'storage-3:9092,storage-4:9092,storage-5:9092,storage-6:9092,storage-7:9092,storage-8:9092,storage-9:9092'  # Kafka brokers
ZOO_HOSTS = 'storage-15:2181,storage-13:2181,storage-14:2181'
ENDPOINT = "kafka-hdfsparser-kafka"    # Unique identifier 
STEP = 60
FALCON_AGENT_URL = "http://storage-12:1988/v1/push"

class ConsumerLagMoniter(object):
    def __init__(self, zookeeper_url='/', timeout=10):        
        self.timeout = timeout        
        self.kafka_logsize = {}
        self.result = []        
        self.timestamp = int(time.time())        

        if zookeeper_url == '/':
            self.zookeeper_url = zookeeper_url
        else:
            self.zookeeper_url = zookeeper_url + '/'

    def monitor(self):
        try:
            kafka_client = KafkaClient(KAFKA_HOSTS, timeout=self.timeout)
        except Exception as e:
            print "Error, cannot connect kafka broker."
            sys.exit(1)        

        try:
            zookeeper_client = KazooClient(hosts=ZOO_HOSTS, read_only=True, timeout=self.timeout)
            zookeeper_client.start()
        except Exception as e:
            print "Error, cannot connect zookeeper server."
            sys.exit(1)
       
        
        for group in CONSUMER_GROUPS:
            for topic in TOPIC_LIST:     
                try:           
                    partition_path = 'consumers/%s/offsets/%s' % (group, topic)
                    partitions = map(int, zookeeper_client.get_children(self.zookeeper_url + partition_path))

                    for partition in partitions:
                        offset_path = 'consumers/%s/offsets/%s/%s' % (group, topic, partition)
                        offset = zookeeper_client.get(self.zookeeper_url + offset_path)[0]
                        if offset is None:
                            continue

                        obj = {'timestamp': self.timestamp,
                            'group': group,
                            'topic': topic,
                            'partition':int(partition),
                            'metric': 'consumerlag:%s' % group,
                            'tags': 'topic=%s,partition=%s' % (topic, partition),
                            'offset':int(offset)}

                        self.result.append(obj)
                except NoNodeError as e:
                    print "Error, fail to get offset for group[%s], topic[%s]" % (group, topic)
                    continue
            
        zookeeper_client.stop()

        for kafka_topic in TOPIC_LIST:
            self.kafka_logsize[kafka_topic] = {}
            try:
                partitions = kafka_client.topic_partitions[kafka_topic]
                logsize_requests = [OffsetRequestPayload(kafka_topic, p, -1, 1) for p in partitions.keys()]

                logsize_responses = kafka_client.send_offset_request(logsize_requests)

                for r in logsize_responses:
                    self.kafka_logsize[kafka_topic][r.partition] = r.offsets[0]
            except Exception as e:
                print "error to get logsize for topic: %s" %  kafka_topic

        kafka_client.close()

        
        payload = []
        for obj in self.result:
            try:
                logsize = self.kafka_logsize[obj['topic']][obj['partition']]
                lag = int(logsize) - int(obj['offset'])
                item = {}
                item['endpoint'] = ENDPOINT
                item['metric'] = obj['metric']
                item['tags'] = obj['tags']
                item['timestamp'] = obj['timestamp']
                item['step'] = STEP
                item['value'] = lag
                item['counterType'] = 'GAUGE'

                payload.append(item)
                    
            except Exception as e:
                print "error to compute (%s/%s/%s) lag-value" % (obj['group'], obj['topic'], obj['partition'])
	    
        # 1. Print
        print "log-lag details:"
        print payload 

        # 2. report to falcon-agent
        if len(payload) > 0:
            requests.post(FALCON_AGENT_URL, data=json.dumps(payload), timeout=10)

if __name__ == '__main__':
    mon = ConsumerLagMoniter()
    mon.monitor()
