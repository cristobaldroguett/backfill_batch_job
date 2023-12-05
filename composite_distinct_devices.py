#!/usr/bin/env python3
import os
import time
import json
from flask import Flask, jsonify, request, render_template
from datetime import datetime, timezone, timedelta
#from elasticsearch import Elasticsearch
#from elasticsearch.helpers import bulk
from opensearchpy import OpenSearch, Search, RequestsHttpConnection, helpers
from opensearchpy.helpers import bulk, scan, search

# Environmental Variables
#host = os.getenv("ESHOST")
osdsn = os.getenv("OPENSEARCHDSN")
osuser = os.getenv("OPENSEARCHUSER")
ospass = os.getenv("OPENSEARCHPASS")
osindex = os.getenv("INDEXNAME")
interval = os.getenv("INTERVAL_LEN")
timeframe = os.getenv("FREQUENCY")
intervalname = os.getenv("INTERVAL_QUERY")

#Functions
def opensearch_connection():
    if osdsn is not None:
        elements = osdsn.replace('/', '').split(':')
        if elements[0] == "https":
            use_ssl = True
            port = 443
        else:
            use_ssl = False
            port = 80
        host = elements[1]
        if len(elements) > 2:
            port = elements[2]
        # Hopefully it's a drop in replacement for Elastic API, we shall see...
        os_conn = OpenSearch(hosts=[{'host': host, 'port': port}],
                             use_ssl=use_ssl,
                             http_compress=True,
                             http_auth=(osuser, ospass),
                             verify_certs=False,
                             ssl_show_warn=False,
                             ssl_assert_hostname=False,
                             connection_class=RequestsHttpConnection,
                             timeout=120
                             )
        return os_conn

if __name__ == '__main__':
    #Track the time when entering program
    entrytime = datetime.now()

    # Connect to OpenSearch
    try:
        opensearch = opensearch_connection()
    except Exception as e:
        print(f"error: Failed to connect to OpenSearch: {str(e)}")

    #Get the start and end time
    startTime = datetime.now() - timedelta(minutes=int(timeframe))#Uncomment when this task is running in ECS on scheduler
    #startTime = datetime(2023,12,4,15,00)
    endTime = datetime.now()
    #endTime = datetime(2023,12,4,15,15)
    print("Start and End Times are " + str(startTime) + " and " + str(endTime))

    # First Query - Find the amount of distinct devices in this time span
    query = {
        "bool": {
            "must": [
                {
                    "match_all": {}
                }
            ],
            "filter": [
                {
                    "range": {
                        "@reporttime": {
                            "lte": endTime,
                            "gte": startTime,
                            "format": "strict_date_optional_time"
                        }
                    }
                }
            ],
            "should": [],
            "must_not": []
        }
    }
    aggs = {
        "distinct_devices": {
            "cardinality": {
                "field": "name.value.keyword"
            }
        }
    }
    querybody = {
        "from": 0,
        "sort": [{"@timestamp": {"order": "asc"}}],
        "query": query,
        "size": 0,
        "aggs": aggs
    }
    #indexpattern = "/devicehistory-fromsnapshot-backfill-" + datepattern + "*/_search"
    indexpattern = "/devicehistory-fromsnapshot-backfill-*/_search"
    snap_result = opensearch.transport.perform_request("GET", indexpattern, body=querybody)

    #If there is no aggregations bucket, then there were no results
    try:
        snap_result['aggregations']
    except Exception as e:
        print(f"There were no results for those parameters: {str(e)}")

    #Get and print the amount of devices
    amount = snap_result['aggregations']['distinct_devices']['value']
    print("The amount of distinct devices between " + str(startTime) + " and " + str(endTime) + " is " + str(amount))

    #Query 2 - Do an aggregate query to get a list of all the distinct devices and their oldest index
    aggs =  {
        "distinct_devices": {
            "composite": {
                "size": 10000,
                "sources": [
                    {
                        "device": {
                            "terms": {
                                "field": "name.value.keyword"
                            }
                        }
                    },
                    {
                        "index": {
                            "terms": {
                                "field": "_index"
                            }
                        }
                    }
                ]
            }
        }
    }
    querybody = {
        "from": 0,
        "sort": [{"@timestamp": {"order": "asc"}}],
        "query": query,
        "size": 0,
        "aggs": aggs
    }
    snap_result = opensearch.transport.perform_request("GET", indexpattern, body=querybody)

    # If there is no aggregations bucket, then there were no results
    try:
        snap_result['aggregations']
    except Exception as e:
        print(f"There were no results for those parameters: {str(e)}")

    #We save the first 10,000 device names into the devices list and index list
    deviceslist = []
    indexlist = []
    for devices in snap_result['aggregations']['distinct_devices']['buckets']:
        deviceslist.append(devices['key']['device'])
        if devices['key']['index'] not in indexlist:
            indexlist.append(devices['key']['index'])

    #If there is an after_key we paginate until there isnt
    while 'after_key' in snap_result['aggregations']['distinct_devices']:
        aggs = {
            "distinct_devices": {
                "composite": {
                    "size": 10000,
                    "sources": [
                        {
                            "device": {
                                "terms": {
                                    "field": "name.value.keyword"
                                }
                            }
                        },
                        {
                            "index":{
                                "terms": {
                                    "field": "_index"
                                }
                            }
                        }
                    ],
                    "after": {"device": snap_result['aggregations']['distinct_devices']['after_key']['device'],
                              "index": snap_result['aggregations']['distinct_devices']['after_key']['index']}
                }
            }
        }
        querybody = {
            "from": 0,
            "sort": [{"@timestamp": {"order": "asc"}}],
            "query": query,
            "size": 0,
            "aggs": aggs
        }
        snap_result = opensearch.transport.perform_request("GET", indexpattern, body=querybody)

        # If there is no aggregations bucket, then there were no results
        try:
            snap_result['aggregations']
        except Exception as e:
            print(f"There were no results for those parameters: {str(e)}")

        for devices in snap_result['aggregations']['distinct_devices']['buckets']:
            deviceslist.append(devices['key']['device'])
            if devices['key']['index'] not in indexlist:
                indexlist.append(devices['key']['index'])

    #Code for tracking record and device totals
    total_devices = len(deviceslist)
    total_records = 0

    #Creating big string for all indexes
    indexes = ""
    for index in indexlist:
        if indexes != "":
            indexes += "," + index
        else:
            indexes += index

    # Do another loop to insert each 1000 batch until deviceslist is empty
    while len(deviceslist) > 1:
        # We have devices list and need to build and query with 1000 names
        triggered = False
        query_string = ""
        currentdeviceslist = deviceslist[:100]
        for names in deviceslist[:100]:
            if triggered != True:
                query_string = query_string + "name.value.keyword:" + str(names)
            else:
                query_string = query_string + " OR name.value.keyword:" + str(names)
            triggered = True

        #Now that we have the query for first 1000 we need to do the real aggregation
        must_string = [
            {
                "query_string": {
                    "query": query_string,
                    "analyze_wildcard": True
                }
            }
        ]
        # Making the query
        query = {
            "bool": {
                "must": must_string,
                "filter": [
                    {
                        "range": {
                            "@reporttime": {
                                "lte": endTime,
                                "gte": startTime,
                                "format": "strict_date_optional_time"
                            }
                        }
                    }
                ],
                "should": [],
                "must_not": []
            }
        }
        includeList = ["*"]

        #### Another way to get the indexes
        # querybody = {
        #     "from": 0,
        #     "query": query,
        #     "sort": [{"@timestamp": {"order": "asc"}}],
        #     "size": 1
        # }
        # snap_result = opensearch.transport.perform_request("GET", indexpattern, body=querybody)
        # oldestping = snap_result['hits']['hits'][0]['_source']['PingTime']
        # #startpattern = oldestping.strftime("%Y%m%d")
        # startpattern = oldestping[:4] + oldestping[5:7] + oldestping[8:10]
        # endpattern = datetime.now().strftime("%Y%m%d")
        # #time.sleep(0.01)
        # # Create datetime pattern for index query
        # x = len(startpattern)
        # y = len(endpattern)
        # z = ""
        # for x in range(0, len(startpattern)):
        #     if x > len(endpattern):
        #         break
        #     if startpattern[x] == endpattern[x]:
        #         z = z + startpattern[x]
        #     elif startpattern[x] != endpattern[x]:
        #         break
        # datepattern = z
        # if len(z) < len(startpattern):
        #     datepattern = datepattern + "*"
        #### END of date pattern

        aggs = {
            "group_by_category": {
                "terms": {
                    "field": "name.value.keyword",
                    "size": 10000
                },
                "aggs": {
                    intervalname: {
                        "date_histogram": {
                            "field": "PingTime",
                            "interval": interval
                        },
                        "aggs": {
                            "latest_record": {
                                "top_hits": {
                                    "_source": {
                                        "include": includeList
                                    },
                                    "sort": [
                                        {
                                            "PingTime": {
                                                "order": "desc"
                                            }
                                        }
                                    ],
                                    "size": 1
                                }
                            }
                        }
                    }
                }
            }
        }
        querybody = {
            "from": 0,
            "sort": [{"@timestamp": {"order": "asc"}}],
            "query": query,
            "size": 0,
            "aggs": aggs
        }

        #indexpattern = "/devicehistory-fromsnapshot-backfill-" + datepattern + "/_search"
        #indexpattern = "/devicehistory-fromsnapshot-backfill-*/_search"
        indexpattern = "/" + indexes + "/_search"
        snap_result = opensearch.transport.perform_request("GET", indexpattern, body=querybody)

        # Once we get the result we need to parse through it to just get the fields we want
        result = []
        for aggregation in snap_result['aggregations']["group_by_category"]['buckets']:
            for frame in aggregation[intervalname]['buckets']:
                if frame["latest_record"]["hits"]["hits"] != []:
                    result.append(frame["latest_record"]["hits"]["hits"])
        total_records += len(result)

        #We have the aggregated records at 3 min cadence so now we need to insert to snapshot index
        bulk_data = []
        batch_size = 1000
        for record in result:
            final_record = record

            if final_record:
                bulk_data.append(final_record[0])

            if len(bulk_data) >= batch_size or len(bulk_data) >= len(result):
                actions = []
                for entry in bulk_data:
                    index_name = entry['_index'][:26] + entry['_index'][35:]
                    #time.sleep(0.01)
                    action = {
                        "_op_type": "index",
                        "_index": index_name,  # index_name,
                        "_id": entry['_id'],
                        "_source": entry['_source'],
                    }
                    actions.append(action)
                    #print("Index name is" + str(index_name))

                success, failed = helpers.bulk(opensearch, actions)

                #print("We added a record into the index")
                if failed:
                    print(f"Failed to insert {len(failed)} documents")
                    for item in failed:
                        print(f"Error: {item['index']['error']}")
                bulk_data = []

        #Print after each batch was inserted
        print("An insertion was completed")

        #Before we go to next loop iteration we need to make devicesList have the next 1000 devices
        if len(deviceslist) > 100:
            deviceslist = deviceslist[100:]
        else:
            deviceslist = []

    #print("Length of device list at the end is " + str(deviceslist))
    totaltime = datetime.now()
    print("Time from start to end: " + str(totaltime - entrytime) + " seconds")
    print("In total we inserted " + str(total_records) + " records into the snapshot index")

    opensearch.close()

