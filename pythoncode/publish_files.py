#!/usr/bin/env python

import re
import os
import sys
from pykafka import KafkaClient
import Queue
import time
import numpy as np

def parseargs(args):
    args_dict = {}
    curtag = None
    curvals = []
    for i, arg in enumerate(args):
      if (arg.startswith('--')):
        if (curtag):
           args_dict[curtag] = curvals
        curtag = arg[2:]
        curvals = []
      else:
        curvals.append(arg)
    if (curtag):
      args_dict[curtag] = curvals
    return args_dict

class FilePublisher(object):

    def __init__(self, hostPort, topicName, filedir, filepatternstr, stagingdir, donedir):
        self.hostPort = hostPort
        self.client = self.getClient()
        self.topicName = topicName
        self.topic = self.getTopic()
        self.filedir = filedir
        self.filepatternstr = filepatternstr.replace(".", "\.").replace("*", ".*")
        self.stagingdir = stagingdir
        self.donedir = donedir
        self.filepattern = re.compile(self.filepatternstr)
        if not self.topic:
            self.valid = False
        else:
            self.valid = True
        if self.topic:
            self.producer = self.topic.get_producer(delivery_reports=True)

    def isValid(self):
        return self.valid

    def getClient(self):
        try:
            client = KafkaClient(hosts=self.hostPort)
            return client
        except Exception as e:
            return None
    
    def getTopic(self):
        if self.client:
            topic = self.client.topics[self.topicName]
            return topic

    def listfiles(self, dirname):
        return set([f for f in os.listdir(dirname) if os.path.isfile(os.path.join(dirname, f)) and self.filepattern.match(f)])

    def newfiles(self):
        current = self.listfiles(self.filedir)
        staged = self.listfiles(self.stagingdir)
        done = self.listfiles(self.donedir)
        current.difference_update(staged)
        current.difference_update(done)
        return current

    def moveFile(self, filename, todir):
        newlocation = os.path.join(todir, os.path.basename(filename))
        try:
            # print("Moving %s to %s" % (filename, newlocation))
            os.rename(filename, newlocation)
            return newlocation
        except Exception as e:
            print("Unable to move file %s to directory %s" % (filename, todir))
            # print(e)
            return None

    def moveToStaging(self, filename):
        fileName = os.path.join(self.filedir, os.path.basename(filename))
        return self.moveFile(fileName, self.stagingdir)

    def moveToDone(self, filename):
        fileName = os.path.join(self.stagingdir, os.path.basename(filename))
        return self.moveFile(fileName, self.donedir)

    def publish_file(self, filename):
        newfilename = self.moveToStaging(os.path.join(self.filedir, filename))
        if newfilename is None:
            return False
        filebasename = os.path.basename(newfilename)
        print('Publishing file %s to kafka' % (filebasename))

        lno = 0
        for l in open(newfilename, 'r'):
            lno += 1
            if l:
              self.producer.produce(filebasename + ": " + l[:-1], partition_key='{}'.format(lno))
        while True:
            try: 
                msg, exc = self.producer.get_delivery_report(block=False)
                if exc is not None:
                    print('Failed for file %s line %d' % (filename, msg.partition_key))
            except Queue.Empty:
                break

        self.moveToDone(filename)
        print('Published file %s to kafka' % (filebasename))

    def getLastN(self, arr, pos, N):
        if pos >= N:
            return arr[pos-N:pos]
        else:
            arr1 = arr[len(arr) - (N - pos):]
            arr2 = arr[0:pos]
            return np.concatenate((arr1, arr2), axis=0)

    def lastNAllZeros(self, arr, pos, N):
        return np.count_nonzero(self.getLastN(arr, pos, N)) == 0

    def run(self):
        counts = 50
        countArray = np.zeros(50, dtype=int)
        cnt = 0
        while True:
            nfiles = self.newfiles()
            if nfiles and len(nfiles) > 0:
                for fl in nfiles:
                    self.publish_file(fl)
            if nfiles:
                countArray[cnt] = len(nfiles)
            else:
                countArray[cnt] = 0
            # If any files are found any any of last 10 tries, then sleep by .1 second
            # otherwise sleep for 5 seconds
            cnt = (cnt + 1) % 50
            if (self.lastNAllZeros(countArray, cnt, 10)):
                time.sleep(5)
            else:
                time.sleep(.1)

if __name__ == '__main__':
    from sys import argv
    args_dict = parseargs(argv)

    hostPort = args_dict.get('host')
    if (hostPort is None):
        print('Host must be passed as --host <hostname:portnumber>')
        sys.exit(1)

    topicName = args_dict.get('topic')
    if (topicName is None):
        print('Topic must be passed as --topic <Topic Name>')
        sys.exit(1)

    filedir = args_dict.get('input')
    if (filedir is None):
        print('Input directory must be passed as --input <Input Directory>')
        sys.exit(1)

    filepatternstr = args_dict.get('files')
    if (filepatternstr is None):
        print('Input files must be passed as --files <File Name of Pattern>')
        sys.exit(1)

    stagingdir = args_dict.get('staging')
    if (stagingdir is None):
        print('Staging directory must be passed as --staging <Staging Directory>')
        sys.exit(1)

    donedir = args_dict.get('processed')
    if (donedir is None):
        print('Processed directory must be passed as --processed <Processed Directory>')
        sys.exit(1)

    fp = FilePublisher(hostPort[0], topicName[0], filedir[0], filepatternstr[0], stagingdir[0], donedir[0])
    fp.run()

