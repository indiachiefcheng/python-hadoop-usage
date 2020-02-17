#!/use/bin/env python
#!-*- coding:utf8 -*-
#Author :chenglinguang
#Date:20180402

import requests
import time
import json
import logging

from cm_api.api_client import ApiResource
from cm_api.endpoints.cms import ClouderaManager

# ---------------------------------------------------------------------------------------------------------------------
CM_HOST = 'namenode-1'
CM_PORT = '7180'
CM_USER = 'hitv'
CM_PASSWD = 'hadoop'
CM_USE_TLS = False
TOPICS_list = ['json.appstore', 'json.gamecenter', 'json.hiaccount', 'json.hipay', 'json.jhx', 'json.jhxmb', 'json.jxg', 'json.ter.ad',
               'json.ter.appstore', 'json.ter.carousel', 'json.ter.gamecenter', 'json.ter.hiaccount', 'json.ter.himsg',
               'json.ter.hipay', 'json.ter.smartimage', 'json.ter.jhx', 'json.ter.jxg', 'json.ter.livesdk',
               'json.ter.unitsearch', 'json.ter.videochat', 'json.ter.vod', 'json.ter.wx', 'json.ter.wxhelper',
               'json.ter.wxsdk', 'json.unitsearch', 'json.vod', 'json.wx', 'json.wxhelper',
               'json.bin.ter.ad', 'json.bin.ter.carousel', 'json.bin.ter.juass', 'json.bin.ter.unifiedapp', 'json.bin.ter.vod',
	       'json.bin.exception.unifiedapp','json.bin.exception.vod','json.bin.exception.himsg']

send_data = []
ts = int(time.time())
post_to_url = 'http://storage-13:1988/v1/push'
log_level = logging.DEBUG
logging.basicConfig(level=log_level,
                    format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                    datefmt='%a, %d %b %Y %H:%M:%S',
                    filename='cloudera_manager_kafka_monitor.log',
                    filemode='w')
tsquery = ApiResource(CM_HOST, server_port=CM_PORT, username=CM_USER, password=CM_PASSWD, use_tls=CM_USE_TLS)


# ---------------------------------------------------------------------------------------------------------------------


def set_push_format(metric, value, tags):
    data_format = {
        "endpoint": "cloudera_manager_kafka",
        "metric": "metric",
        "timestamp": ts,
        "step": 60,
        "value": 0,
        "counterType": "GAUGE",
        "tags": "",
    }
    data_format = data_format.copy()
    data_format['metric'] = metric
    data_format['value'] = value
    data_format['tags'] = tags
    send_data.append(data_format)


def post_to_agent(data):
    print data
    logging.info(data)
    try:
        r = requests.post(post_to_url, data=json.dumps(data))
        logging.info(": " + r.text)
    except Exception, e:
        logging.error("post_to_agent: " + e.message)


def construct_query():
    object_name = 'total_kafka_messages_received_rate_across_kafka_broker_topics'
    category = 'KAFKA_TOPIC'
    for topic in TOPICS_list:
        entity_name = 'kafka:' + topic
        metric_name = 'ls_topic_' + topic
        do_query(object_name, entity_name, category, metric_name)
    do_query("total_kafka_messages_received_rate_across_kafka_brokers", "kafka", "SERVICE", "Kafka-O_total_received_messages")
    do_query("total_kafka_messages_received_rate_across_kafka_brokers", "kafka3", "SERVICE", "Kafka-S_totla_received_messages")

def do_query(object_name, entity_name, category, metric_name):
    sql = "SELECT " + object_name + " WHERE entityName =" + entity_name + " AND category = " + category + ""
    for response in tsquery.query_timeseries(sql, from_time, to_time):
        if response.timeSeries:
            for ts in response.timeSeries:
                try:
                    i = len(ts.data)
                    data = ts.data[i - 1]
                    #set_push_format(metric_name, data.value, "entityName=" + entity_name)
                    set_push_format(metric_name, data.value, "entytyName=kafka")
                except Exception, e:
                    logging.error("query data value:" + e.message)
            else:
                logging.info("There is no data value!")


if __name__ == '__main__':
    send_data = []
    from_time = None
    to_time = None
    construct_query()
    post_to_agent(send_data)

