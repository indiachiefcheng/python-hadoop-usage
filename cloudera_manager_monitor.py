#!-*- coding:utf8 -*-
#Author: chenglinguang
#date:20180305


import requests
import time
import json
import logging

from cm_api.api_client import ApiResource
from cm_api.endpoints.cms import ClouderaManager

CM_HOST = 'namenode-1'
CM_PORT= '7180'
CM_USER = 'hitv'
CM_PASSWD = 'hadoop'
CM_USE_TLS = False
QUERY="SELECT allocated_memory_mb_cumulative where category=YARN_POOL and serviceName=\"yarn\" and queueName=root.hive"
YARN_QUERY = "SELECT allocated_memory_mb_cumulative,allocated_vcores_cumulative where category=YARN_POOL and serviceName=\"yarn\" and queueName = "
BLOCKS_QUERY = "SELECT blocks_total_across_hdfss"
DFS_CAPACITY_USED_QUERY = "select  dfs_capacity_used where entityName=\"hdfs:nameservice2\""
DFS_CAPACITY_USED_NON_HDFS_QUERY = "select  dfs_capacity_used_non_hdfs where entityName=\"hdfs:nameservice2\""
entityName=['kafka3-KAFKA_BROKER-17824beeebea3469176f9c5b7aea4e60','kafka3-KAFKA_BROKER-62ae9d6c80085e6fcf59450a1091ff39',
            'kafka3-KAFKA_BROKER-66da889c1c5041e00de75c69af768d8b','kafka3-KAFKA_BROKER-9fda5ad7bdfc34a3f578c7787312c444',
            'kafka3-KAFKA_BROKER-be04f594f50c63c9cc41ee4536f1ee6c','kafka3-KAFKA_BROKER-c6d71e0b207bebd54ef6171019e5a484',
            'kafka3-KAFKA_BROKER-e25b5487c46845a8ae6bd626a0273be8']
#metric_names = ["root", "root.hive", "root.spark", "root.impala", "root.resys", "root.default", "root.jhl", "root.kylin"]
metric_names = ["root", "root.hive", "root.spark", "root.impala", "root.resys", "root.default", "root.jhl", "root.kylin","root.etluser","root.adspark","root.stream"]
tsquery =ApiResource(CM_HOST, server_port=CM_PORT,username=CM_USER, password=CM_PASSWD, use_tls=CM_USE_TLS)
send_data=[]
ts = int(time.time())
log_level = logging.DEBUG
logging.basicConfig(level=log_level,
                    format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                    datefmt='%a, %d %b %Y %H:%M:%S',
                    filename='parser_new_result.log',
                    filemode='w')

def set_push_format(time,  metric, value, tags):
    data_format = {
        "endpoint": "clouderamanager",
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

#push
def post_to_agent(data):
    logging.info(data)
    try:
        r = requests.post("http://storage-13:1988/v1/push", data=json.dumps(data))
        logging.info(": " + r.text)
    except Exception, e:
        logging.error("post_to_agent: " + e.message)



def do_query(query, from_time, to_time):
    for response in tsquery.query_timeseries(QUERY,from_time,to_time):
      if response.timeSeries:
        for ts in response.timeSeries:
          try:
            i=len(ts.data)
            data=ts.data[i-1]
            set_push_format(data.timestamp, "hive raw", data.value, "")
          except Exception, e:
              logging.error("query data value:"+e.message)

      else:
          logging.info("There is no datavalue!")

def do_queries():
    for sq in entityName:
      sql = "SELECT kafka_messages_received_rate WHERE entityName ="+ sq +" AND category = ROLE"
      for response in tsquery.query_timeseries(sql,from_time,to_time):
        if response.timeSeries:
          for ts in response.timeSeries:
            try:
              i=len(ts.data)
              data=ts.data[i-1]
              set_push_format(data.timestamp,"Kafka-S_recieve",data.value,"entityName="+sq)
            except Exception, e:
              logging.error("query data value:"+e.message)
        else:
           logging.info("There is no datavalue!")
                    
def status():
  for s in tsquery.get_cluster('Cluster 1').get_all_services():
    
    if s.healthSummary=="GOOD" or s.healthSummary=="CONCERNING":
        set_push_format(ts,s.name,0,"status")
    else:
        set_push_format(ts,s.name,1,"status")

def test():
  test=" select health_good_rate * 100 as 'good health', health_concerning_rate * 100 as 'concerning health', health_bad_rate * 100 as 'bad health', health_disabled_rate * 100 as         'disabled health', health_unknown_rate * 100 as 'unknown health' where entityName='c11a337c-5f5f-4dca-8bf3-f801f18015ef'"
  for response in tsquery.query_timeseries(test,from_time,to_time):
    if response.timeSeries:
      for ts in response.timeSeries:
        try:
          i=len(ts.data)
          data=ts.data[i-1]
          print int(data.value)
        except Exception, e:
          print "111"
    else:
      print "erro"
def query_yarn():
    for metric_name in metric_names:
        try:
            response = tsquery.query_timeseries(YARN_QUERY + metric_name, from_time, to_time)[0]
            if response.timeSeries:
                mem_value = response.timeSeries[0].data[len(response.timeSeries[0].data)-1].value
                core_value = response.timeSeries[1].data[len(response.timeSeries[0].data)-1].value
            else:
                mem_value = 0
                core_value = 0
            set_push_format("","mem_"+metric_name, mem_value, "")
            set_push_format("","core_"+metric_name, core_value, "")
        except Exception, e:
            logging.error("query data value:" + e.message)


def query_blocks():
    try:
        response = tsquery.query_timeseries(BLOCKS_QUERY, from_time, to_time)[0]
        if response.timeSeries:
            blocks_value = response.timeSeries[0].data[len(response.timeSeries[0].data) - 1].value
        else:
            blocks_value = 0
        set_push_format("","hdfs_blocks_num", blocks_value, "")
    except Exception, e:
        logging.error("query data value:" + e.message)
 

def query_dfs_capacity():
    total_dfs_capatity_used = 0
    try:
        response = tsquery.query_timeseries(DFS_CAPACITY_USED_QUERY, from_time, to_time)[0]
        response1 = tsquery.query_timeseries(DFS_CAPACITY_USED_NON_HDFS_QUERY, from_time, to_time)[0]
        if response.timeSeries:
            dfs_capacity_used = response.timeSeries[0].data[len(response.timeSeries[0].data) - 1].value
            dfs_capacity_used_non_hdfs = response1.timeSeries[0].data[len(response.timeSeries[0].data) - 1].value
            total_dfs_capatity_used = dfs_capacity_used+dfs_capacity_used_non_hdfs
        set_push_format("", "total_dfs_capatity_used", total_dfs_capatity_used, "")
    except Exception, e:
        logging.error("query data value:" + e.message)


if __name__ == '__main__':
  send_data = []
  from_time = None
  to_time = None
  do_query(QUERY, from_time, to_time)
  #do_queries() zyf
  status()
  query_yarn()
  query_blocks()
  query_dfs_capacity()
  post_to_agent(send_data)

