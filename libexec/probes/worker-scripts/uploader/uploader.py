import os
import time
import inspect
import json
import warnings
import sys
import pika
from time import strftime
from time import localtime

# Using the esmond_client instead of the rpm
from esmond_client.perfsonar.query import ApiFilters
from esmond_client.perfsonar.query import ApiConnect
from esmond_client.perfsonar.post import MetadataPost, EventTypePost, EventTypeBulkPost
from esmond_client.perfsonar.post import EventTypeBulkPostWarning, EventTypePostWarning

# New module with socks5 OR SSL connection that inherits ApiConnect
from SocksSSLApiConnect import SocksSSLApiConnect
from SSLNodeInfo import EventTypeSSL
from SSLNodeInfo import SummarySSL

from optparse import OptionParser

from messaging.message import Message
from messaging.queue.dqs import DQS

# Set filter object
filters = ApiFilters()
gfilters = ApiFilters()

# Set command line options
parser = OptionParser()
parser.add_option('-d', '--disp', help='display metadata from specified url', dest='disp', default=False, action='store')
parser.add_option('-e', '--end', help='set end time for gathering data (default is now)', dest='end', default=0)
parser.add_option('-l', '--loop', help='include this option for looping process', dest='loop', default=False, action='store_true')
parser.add_option('-p', '--post',  help='begin get/post from specified url', dest='post', default=False, action='store_true')
parser.add_option('-r', '--error', help='run get/post without error handling (for debugging)', dest='err', default=False, action='store_true')
parser.add_option('-s', '--start', help='set start time for gathering data (default is -12 hours)', dest='start', default=960)
parser.add_option('-u', '--url', help='set url to gather data from (default is http://hcc-pki-ps02.unl.edu)', dest='url', default='http://hcc-pki-ps02.unl.edu')
parser.add_option('-w', '--user', help='the username to upload the information to the GOC', dest='username', default='afitz', action='store')
parser.add_option('-k', '--key', help='the key to upload the information to the goc', dest='key', default='fc077a6a133b22618172bbb50a1d3104a23b2050', action='store')
parser.add_option('-g', '--goc', help='the goc address to upload the information to', dest='goc', default='http://osgnetds.grid.iu.edu', action='store')
parser.add_option('-t', '--timeout', help='the maxtimeout that the probe is allowed to run in secs', dest='timeout', default=1000, action='store')
parser.add_option('-x', '--summaries', help='upload and read data summaries', dest='summary', default=True, action='store')
parser.add_option('-a', '--allowedEvents', help='The allowedEvents', dest='allowedEvents', default=False, action='store')
parser.add_option('-A', '--allowedMQEvents', help='The allowedMQEvents', dest='allowedMQEvents', default=False, action='store')
parser.add_option('-M', '--maxMQmessageSize', help='The max MQ message size allowed', dest='maxMQmessageSize', default=False, action='store')
#Added support for SSL cert and key connection to the remote hosts                                                                                                                
parser.add_option('-c', '--cert', help='Path to the certificate', dest='cert', default='/etc/grid-security/rsv/rsvcert.pem', action='store')
parser.add_option('-o', '--certkey', help='Path to the certificate key', dest='certkey', default='/etc/grid-security/rsv/rsvkey.pem', action='store')
# Add support for message queue                                                                                                                                                  
parser.add_option('-q', '--queue', help='Directory queue (path)', default=None, dest='dq', action='store')
parser.add_option('-m','--tmp', help='Tmp directory to use for timestamps', default='/tmp/rsv-perfsonar/', dest='tempr', action='store')
(opts, args) = parser.parse_args()


class Uploader(object):

    def add2log(self, log):
        print strftime("%a, %d %b %Y %H:%M:%S", localtime()), str(log)
    
    def __init__(self,verbose,start,end,connect,username=None,key=None, goc=None, allowedEvents='packet-loss-rate', cert=None, certkey=None, dq=None, tempr='/tmp/rsv-perfsonar/', allowedMQEvents='packet-loss-rate', maxMQmessageSize=10000):
        # Filter variables
        filters.verbose = verbose
        #filters.verbose = True 
        # this are the filters that later will be used for the data
        self.time_end = int(time.time())
        self.time_start = int(self.time_end - start)
        # Filter for metadata
        filters.time_start = int(self.time_end - 3*start)
        # Added time_end for bug that Andy found as sometime far in the future 24 hours
        filters.time_end = self.time_end + 24*60*60
        # For logging pourposes
        filterDates = (strftime("%a, %d %b %Y %H:%M:%S ", time.gmtime(self.time_start)), strftime("%a, %d %b %Y %H:%M:%S", time.gmtime(self.time_end)))
        #filterDates = (strftime("%a, %d %b %Y %H:%M:%S ", time.gmtime(filters.time_start)))
        self.add2log("Data interval is from %s to %s" %filterDates)
        self.add2log("Metada interval is from %s to now" % (filters.time_start))
        # gfiltesrs and in general g* means connecting to the cassandra db at the central place ie goc
        gfilters.verbose = False        
        gfilters.time_start = int(self.time_end - 5*start)
        gfilters.time_end = self.time_end
        gfilters.input_source = connect
        # Username/Key/Location/Delay
        self.connect = connect
        self.username = username
        self.key = key
        self.goc = goc
        self.conn = SocksSSLApiConnect("http://"+self.connect, filters)
        self.gconn = ApiConnect(self.goc, gfilters)
        self.cert = cert
        self.certkey = certkey
        self.tmpDir = tempr + '/'
        # Convert the allowedEvents into a list
        self.allowedEvents = allowedEvents.split(',')
        # List of events that are allwoed to send via the MQ
        # If not present should be the same as allowed events
        if allowedMQEvents != None:
            self.allowedMQEvents = allowedMQEvents.split(',')
        else:
            self.allowedMQEvents = allowedEvents
        self.maxMQmessageSize = maxMQmessageSize
        # In general not use SSL for contacting the perfosnar hosts
        self.useSSL = False
        #Code to allow publishing data to the mq                                                                                                                                   
        self.mq = None
        self.dq = dq
        if self.dq != None and self.dq!='None':
            try:
                self.mq = DQS(path=self.dq)
            except Exception as e:
                self.add2log("Unable to create dirq %s, exception was %s, " % (self.dq, e))
                
    # Get Data
    def getData(self, disp=False, summary=True):
        self.add2log("Only reading data for event types: %s" % (str(self.allowedEvents)))
        if summary:
            self.add2log("Reading Summaries")
        else:
            self.add2log("Omiting Sumaries")
        metadata = self.conn.get_metadata()
        try:
            #Test to see if https connection is succesfull
            md = metadata.next()
            self.readMetaData(md, disp, summary)
        except Exception as e:
            #Test to see if https connection is sucesful
            self.add2log("Unable to connect to %s, exception was %s, trying SSL" % ("http://"+self.connect, e))
            try:
                metadata = self.conn.get_metadata(cert=self.cert, key=self.certkey)
                md = metadata.next()
                self.useSSL = True
                self.readMetaData(md, disp, summary)
            except Exception as e:
                raise Exception("Unable to connect to %s, exception was %s, " % ("https://"+self.connect, e))
        for md in metadata:
            self.readMetaData(md, disp, summary)

    # Md is a metadata object of query
    def readMetaData(self, md, disp=False, summary=True):
        arguments = {}
        # Building the arguments for the post
        arguments = {
            "subject_type": md.subject_type,
            "source": md.source,
            "destination": md.destination,
            "tool_name": md.tool_name,
            "measurement_agent": md.measurement_agent,
            "input_source": md.input_source,
            "input_destination": md.input_destination,
            "tool_name": md.tool_name
        }
        if not md.time_duration is None:
            arguments["time_duration"] = md.time_duration
        if not md.ip_transport_protocol is None:
            arguments["ip_transport_protocol"] = md.ip_transport_protocol
        # Assigning each metadata object property to class variables
        event_types = md.event_types
        metadata_key = md.metadata_key
        # print extra debugging only if requested
        self.add2log("Reading New METADATA/DATA %s" % (md.metadata_key))
        if disp:
            self.add2log("Posting args: ")
            self.add2log(arguments)
        # Get Events and Data Payload
        summaries = {}
        summaries_data = {}
        # datapoints is a dict of lists
        # Each of its members are lists of datapoints of a given event_type
        datapoints = {}
        datapointSample = {}
        #load next start times
        self.time_starts = {}
        try:
            f = open(self.tmpDir+md.metadata_key, 'r')
            self.time_starts = json.loads(f.read())
            f.close()
        except IOError:
            self.add2log("first time for %s" % (md.metadata_key))
        except ValueError:
            # decoding failed
            self.add2log("first time for %s" % (md.metadata_key))
        for et in md.get_all_event_types():
            if self.useSSL:
                etSSL = EventTypeSSL(et, self.cert, self.certkey)
                et = etSSL
            # Adding the time.end filter for the data since it is not used for the metadata
            #use previously recorded end time if available
            et.filters.time_start = self.time_start
            if et.event_type in self.time_starts.keys():
                et.filters.time_start = self.time_starts[et.event_type]
                self.add2log("loaded previous time_start %s" % et.filters.time_start)
            et.filters.time_end = filters.time_end
            eventype = et.event_type
            datapoints[eventype] = {}
            #et = md.get_event_type(eventype)
            if summary:
                summaries[eventype] = et.summaries
            else:
                summaries[eventype] = []
            # Skip reading data points for certain event types to improv efficiency  
            if eventype not in self.allowedEvents:                                                                                                  
                continue
            # Read summary data 
            summaries_data[eventype] = []
            for summ in et.get_all_summaries():
                if self.useSSL:
                    summSSL = SummarySSL(summ, self.cert, self.certkey)
                    summ = summSSL
                summ_data = summ.get_data()
                summ_dp = [ (dp.ts_epoch, dp.val) for dp in summ_data.data ]
                if not summ_dp:
                    continue
                summaries_data[eventype].append({'event_type': eventype,
                                                   'summary_type' : summ.summary_type,
                                                   'summary_window' : summ.summary_window,
                                                   'summary_data' : summ_dp })
                # Read datapoints
            dpay = et.get_data()
            tup = ()
            for dp in dpay.data:
                tup = (dp.ts_epoch, dp.val)
                datapoints[eventype][dp.ts_epoch] = dp.val
                # print debugging data
            self.add2log("For event type %s, %d new data points"  %(eventype, len(datapoints[eventype])))
            if len(datapoints[eventype]) > 0 and not isinstance(tup[1], (dict,list)): 
                # picking the first one as the sample
                datapointSample[eventype] = tup[1]
        self.add2log("Sample of the data being posted %s" % datapointSample)
        try:
            self.postData(arguments, event_types, summaries, summaries_data, metadata_key, datapoints, summary, disp)
        except Exception as e:
            raise Exception("Unable to post to %s, because exception %s. Check postgresql and cassandra services are up. Then check user and key are ok "  %(self.goc, e))

    # Experimental function to try to recover from missing packet-count-sent or packet-count-lost data
    def getMissingData(self, timestamp, metadata_key, event_type, disp=False):
        filtersEsp = ApiFilters()
        filtersEsp.verbose = disp
        filtersEsp.metadata_key = metadata_key
        filtersEsp.time_start = timestamp - 30000
        filtersEsp.time_end  = timestamp + 30000 
        conn = SocksSSLApiConnect("http://"+self.connect, filtersEsp)
        if self.useSSL:
            metadata = conn.get_metadata(cert=self.cert, key=self.certkey)
        else:
            metadata = conn.get_metadata()
        datapoints = {}
        datapoints[event_type] = {}
        for md in metadata:
            if not md.metadata_key == metadata_key:
                continue
            et = md.get_event_type(event_type)
            if self.useSSL:
                etSSL = EventTypeSSL(et, self.cert, self.certkey)
                et = etSSL
            dpay = et.get_data()
            for dp in dpay.data:
                if dp.ts_epoch == timestamp:
                    self.add2log("point found")
                    datapoints[event_type][dp.ts_epoch] = dp.val
        return datapoints
                
    # Place holde for posting Data if it is to Esmond, ActiveMQ, RabbitMQ
    def postData(self, arguments, event_types, summaries, summaries_data, metadata_key, datapoints, summary = True, disp=False):
        pass
