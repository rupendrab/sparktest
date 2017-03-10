#!/usr/bin/env python

import sys
import os
from pykafka import KafkaClient

def getClient(host):
    try:
        client = KafkaClient(hosts="localhost:9092")
        return client
    except Exception as e:
        return None

def getTopic(client, topicStr):
    topic = client.topics[topicStr]
    return topic

def publish_file(topic, filename):
    filebasename = os.path.basename(filename)
    with topic.get_sync_producer() as producer:
        for l in open(filename, 'r'):
            if l:
              producer.produce(filebasename + ": " + l[:-1])

def test_publish(topic):
    with topic.get_sync_producer() as producer:
        for i in range(100):
            producer.produce('rupen nina ronnie')

if __name__ == '__main__':
    from sys import argv
    if (len(argv) != 4):
        print("Usage: produce_file.py <Host:Port of Kafka> <Topic> <File Name>")
        sys.exit(1)
    hostStr = argv[1]
    topicStr = argv[2]
    fileName = argv[3]

    client = getClient(hostStr)
    if not client:
        print("Cannot connect to %s" % hostStr)
        sys.exit(1)
    topic = getTopic(client, topicStr)
    if not topic:
        print("No such topic %s" % topicStr)
        sys.exit(1)

    publish_file(topic, fileName)

    exit()

