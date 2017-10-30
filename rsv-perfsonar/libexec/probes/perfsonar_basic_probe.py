#!/usr/bin/python

import os
import re
import subprocess
import sys
import rsvprobe
import time
import random

import time
from time import strftime
from time import localtime

sys.path.insert(0, '.')
class PerfsonarSimpleProbe(rsvprobe.RSVProbe):
    """
    TODO - write description of the probe
    """
    def __init__(self):
        rsvprobe.RSVProbe.__init__(self)
        metric = rsvprobe.RSVMetric("Perfsonar-Monitor",
                                    "org.osg.general.perfsonar-simple", rsvprobe.RSVMetric.STATUS)
        self.supported_metrics = [metric]
        self.details = "---\n"
        # Add the config directory
        self.conf_dir = os.path.join("/", "etc", "rsv", "metrics")
        self.start = 900
        self.debug = False
        self.maxstart = 43200
        self.summaries = False
        self.allowedEvents = "packet-loss-rate, throughput, packet-trace, packet-retransmits, histogram-owdelay, packet-count-sent,packet-count-lost"
        self.soft_timeout = 1400
        self.tmpdirectory = '/tmp/rsv-perfsonar/'
        # Add the options so the parsing knows what to expect
        self.addopt("", "start=", "--start How back in history get data from perfsonar ndoe in secs (i.e 43200)")
        # sleep left here but not used anymore just for compatiblity 
        self.addopt("", "debug=", "--debug True or False. For if extra debugging is needed")
        self.addopt("", "timeout=", "--timeout Seconds. A softimeout for how long the probes are allowed to run")
        self.addopt("", "summary=", "--summary True or False. Read and upload data summaries or not")
        self.addopt("", "maxstart=", "--maxstart the max number in seconds it will go in the past to retrieve information")
        self.addopt("", "allowedEvents=", "--allowedEvents a list with the allowed event types")
        self.addopt("", 'tmpdirectory=', "--tmpdirectory a directory to store temporary timestamps must be read/writebl by rsv")

    def parseopt(self):
        """parse options specific to network monitroing probe and return options, optlist and reminder to
        allow further processing
        """
        options, optlist, remainder = rsvprobe.RSVProbe.parseopt(self)
        for opt, arg in options:
            if opt == '--start':
                self.start = arg
            elif opt == '--debug':
                self.debug = arg
            elif opt == '--timeout':
                self.soft_timeout = arg
            elif opt == '--summary':
                self.summaries = arg
            elif opt == '--maxstart':
                self.maxstart = arg
            elif opt == '--tmpdirectory':
                self.tmpdirectory = arg
            elif opt == '--allowedEvents':
                # Replacing white spaces with nothing
                self.allowedEvents = arg.replace(" ", '')
            if self.host == self.localhost:
                self.is_local = True
            else:
                self.is_local = False
        return options, optlist, remainder

    def ReadTimeStampFile(self, filename):
        if not os.path.isfile(filename):
            return 1
        else:
            nfile = open(filename, 'r')
            return nfile.readline()

    def createDir(self, directory):
        if not os.path.exists(directory):
            os.makedirs(directory)
        return directory
    
    def WriteNewTimestamp(self, filename, starttime):
         nfile = open(filename, 'w')
         time = strftime("%a, %d %b %Y %H:%M:%S", starttime)
         nfile.write(time)

    def computeStartTime(self, filename):
        timestamp = self.ReadTimeStampFile(filename)
        timeFormat = "%a, %d %b %Y %H:%M:%S"
        if not timestamp == 1:#The probe ran succesfully:
           try:
               oldTime = time.strptime(timestamp, timeFormat)
               self.add_message("Last succesfull run at %s" % timestamp)
               newStart = time.mktime(localtime())-time.mktime(oldTime)
           except ValueError:
               self.out_debug("Timestamp %s, in file %s not in correct format %s" % (timestamp, filename, timeFormat))
               newStart = self.start
           # If it the value is less than the one allowed use maxstart
           if newStart < int(self.maxstart):
               self.start = int(newStart)
           else:
               self.add_message("previous time_start %s too old. set to maxstart: %s" % (newStart, self.maxstart) )
               self.start = int(self.maxstart)
        else:
            self.add_message("No previous sucesfull run found")

    def runCallerScript(self):
        cmd = "source ./uploader.sh %s %d %s %s" % (self.host, int(self.start), self.soft_timeout, self.metricName)
        self.add_message("Command call %s" % cmd)                                                                                         
        ec, out = rsvprobe.run_command(cmd, workdir="/usr/libexec/rsv/probes/worker-scripts")
        return out

    def run(self):
       """Main routine for the probe"""
       self.parseopt()
       #In /var/log/rsv/metrics/ each probe creates a dir with the last time it ran succesfully if it is not yet there
       basedir = os.path.join("/var/log/rsv/metrics/", self.supported_metrics[0].name)
       basedir = self.createDir(basedir)
       directoryFile = self.createDir(os.path.join(basedir,self.host))
       timeFile = os.path.join(directoryFile, 'timeFile.out')
       # computeStartTime basically reads the file assumes the date there is the time_end in the filter probe of the last succesfull probe
       # and computes how long in the past it has to look back, basically now - time_end of last sucesfull run.
       self.computeStartTime(timeFile)
       startTime = localtime()
       # Some dictionaries where last ran actually happen will be created on the /tmps
       tmpdirectory = os.path.join(self.tmpdirectory, self.supported_metrics[0].name, self.host)
       self.tmpdirectory = self.createDir(tmpdirectory) 
       # Calling the caller script and parsing it is output
       out = self.runCallerScript()
       reverOut = out.split('\n')
       reverOut.reverse()
       for line in reverOut:
           self.add_message(line)
       if 'ERROR' in out:
            self.return_critical("Failed running the caller")
       self.WriteNewTimestamp(timeFile, startTime)
       if 'WARNING' in out:
           self.return_warning('WARNING: timeout ')
       self.return_ok("OK")

    
