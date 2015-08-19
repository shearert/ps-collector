import os
import time
import inspect
import json
from time import strftime
from time import localtime

from optparse import OptionParser
from esmond.api.client.perfsonar.query import ApiFilters
from esmond.api.client.perfsonar.query import ApiConnect
# New module with socks5 OR SSL connection that inherits ApiConnect
from SocksSSLApiConnect import SocksSSLApiConnect
from esmond.api.client.perfsonar.post import MetadataPost, EventTypePost, EventTypeBulkPost

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
#Added support for SSL cert and key connection to the remote hosts
parser.add_option('-c', '--cert', help='Path to the certificate', dest='cert', default='/etc/grid-security/rsv/rsvcert.pem', action='store')
parser.add_option('-o', '--certkey', help='Path to the certificate key', dest='certkey', default='/etc/grid-security/rsv/rsvkey.pem', action='store')
# Add support for message queue
parser.add_option('-q', '--queue', help='Directory queue (path)', default=None, dest='dq', action='store')
(opts, args) = parser.parse_args()

class EsmondUploader(object):

    def add2log(self, log):
        print strftime("%a, %d %b %Y %H:%M:%S", localtime()), str(log)
    
    def __init__(self,verbose,start,end,connect,username=None,key=None, goc=None, allowedEvents='packet-loss-rate', cert=None, certkey=None, dq=None):
        # Filter variables
        filters.verbose = verbose
        # this are the filters that later will be used for the data
        self.time_end = time.time()
        self.time_start = int(self.time_end - start)
        # Filter for metadata
        filters.time_start = int(self.time_end - 3*start)
        # Added time_end for bug that Andy found as sometime far in the future 24 hours
        filters.time_end = int(self.time_end + 24*60*60)
        # For logging pourposes
        filterDates = (strftime("%a, %d %b %Y %H:%M:%S ", time.gmtime(self.time_start)), strftime("%a, %d %b %Y %H:%M:%S", time.gmtime(self.time_end)))
        #filterDates = (strftime("%a, %d %b %Y %H:%M:%S ", time.gmtime(filters.time_start)))
        self.add2log("Data interval is from %s to %s" %filterDates)
        self.add2log("Metada interval is from %s to now" % (filters.time_start))
        # gfiltesrs and in general g* means connecting to the cassandra db at the central place ie goc
        gfilters.verbose = False        
        gfilters.time_start = int(self.time_end - 5*start)
        gfilters.time_end = self.time_end + 24*60*60
        gfilters.input_source = connect
        # Username/Key/Location/Delay
        self.connect = connect
        self.username = username
        self.key = key
        self.goc = goc
        self.conn = SocksSSLApiConnect(self.connect, filters)
        self.gconn = ApiConnect(self.goc, gfilters)
        self.cert = cert
        self.certkey = certkey

        # Convert the allowedEvents into a list
        self.allowedEvents = allowedEvents.split(',')
        
        #Code to allow publishing data to the mq
        self.mq = None
        self.dq = dq
        if self.dq != None and self.dq!='None':
            try:
                self.mq = DQS(path=self.dq)
            except Exception as e:
                self.add2log("Unable to create dirq %s, exception was %s, " % (self.dq, e))
    
    #Auxiliary function to detect data already added to the central data store
    # for a single metadata_key.
    # This function is too hard on the original server and should not be used
    def getExistingData(self):
        gmetadata = self.gconn.get_metadata()
        datapoints = {}
        for gmd in self.gconn.get_metadata():
             if "org_metadata_key" in gmd._data:
                 metadata_key = gmd._data["org_metadata_key"]
                 datapoints[metadata_key] = {}
                 for et in gmd.get_all_event_types():
                     eventype = et.event_type
                     datapoints[metadata_key][eventype] = {}
                     if eventype not in self.allowedEvents:
                             continue
                     dpay = et.get_data()
                     for dp in dpay.data:
                         datapoints[metadata_key][eventype][dp.ts_epoch] = dp.val
        return datapoints
    
    # Publish message to Mq
    def publishToMq(self, arguments, event_types, datapoints, summaries_data):
        for event in event_types:
            # filter events for mq (must be subset of the probe's filter)
            if event not in ('path-mtu', 'histogram-owdelay','packet-loss-rate','histogram-ttl','throughput','packet-retransmits','packet-trace'):
                continue
            # skip events that have no datapoints 
            if not datapoints[event]:
                continue
            # compose msg
            msg_head = { 'input-source' : arguments['input_source'],
                        'input-destination' : arguments['input_destination'],
                         'event-type' : event,
                         'rsv-timestamp' : "%s" % time.time(),
                         'summaries' : 0,
                         'destination' : '/topic/perfsonar.' + event}
            msg_body = { 'meta': arguments }
            if summaries_data[event]:
                msg_body['summaries'] = summaries_data[event]
                msg_head['summaries'] = 1
            if datapoints[event]:
                msg_body['datapoints'] = datapoints[event]
            msg = Message(body=json.dumps(msg_body), header=msg_head)
            # add to mq
            try:
                self.mq.add_message(msg)
            except Exception as e:
                self.add2log("Failed to add message to mq %s, exception was %s" % (self.dq, e))

                 
    # Expects an object of tipe SocksSSLApiConnect
    def getMetaDataConnection(self, conn):
        try:
            metadata = conn.get_metadata()
            return metadata
        except Exception as e:
            self.add2log("Unable to connect to %s, exception was %s, trying SSL" % (uri, e))
            try:
                metadata = conn.get_metadata(cert=self.cert, key=self.cert-key)
                return metadata
            except Exception as e:
                self.add2log("Unable to connect to %s, exception was %s, " % (uri, e))

    # Get Data
    def getData(self, disp=False, summary=True):
        self.add2log("Only reading data for event types: %s" % (str(self.allowedEvents)))
        if summary:
            self.add2log("Reading Summaries")
        else:
            self.add2log("Omiting Sumaries")
        metadata = self.getMetaDataConnection(self.conn)
        oldDataPoints = {}
        datapoints = {}
        #oldDataPoints = self.getExistingData()
        for md in metadata:
            # Building the arguments for the post
            arguments = {
                    "subject_type": md.subject_type,
                    "source": md.source,
                    "destination": md.destination,
                    "tool_name": md.tool_name,
                    "measurement_agent": md.measurement_agent,
                    "input_source": md.input_source,
                    "input_destination": md.input_destination
            }
            if not md.time_duration is None:
                arguments["time_duration"] = md.time_duration
            # Assigning each metadata object property to class variables
            event_types = md.event_types
            metadata_key = md.metadata_key
            if disp:
                self.add2log("event_types")
                self.add2log(event_types)
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
            for et in md.get_all_event_types():
                # Adding the time.end filter for the data since it is not used for the metadata
                et.filters.time_start = self.time_start
                et.filters.time_end = self.time_end
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
            self.postData(arguments, event_types, summaries, summaries_data, metadata_key, datapoints, summary, disp)

    
    # Experimental function to try to recover from missing packet-count-sent or packet-count-lost data
    def getMissingData(self, timestamp, metadata_key, event_type, disp=False):
        filtersEsp = ApiFilters()
        filtersEsp.verbose = True
        filtersEsp.metadata_key = metadata_key
        filtersEsp.time_start = timestamp - 30000
        filtersEsp.time_end  = timestamp + 30000 
        conn = SocksSSLApiConnect(self.connect, filtersEsp)
        metadata = self.getMetaDataConnection(conn)
        datapoints = {}
        datapoints[event_type] = {}
        for md in metadata:
            if not md.metadata_key == metadata_key:
                continue
            et = md.get_event_type(event_type)
            dpay = et.get_data()
            for dp in dpay.data:
                if dp.ts_epoch == timestamp:
                    self.add2log("point found")
                    datapoints[event_type][dp.ts_epoch] = dp.val
        return datapoints
                

    def postData(self, arguments, event_types, summaries, summaries_data, metadata_key, datapoints, summary = True, disp=False):
        mp = MetadataPost(self.goc, username=self.username, api_key=self.key, **arguments)
        for event_type in event_types:
            mp.add_event_type(event_type)
            if summary:
                summary_window_map = {}
                #organize summaries windows by type so that all windows of the same type are in an array
                for summy in summaries[event_type]:
                    if summy[0] not in summary_window_map:
                        summary_window_map[summy[0]] = []
                    summary_window_map[summy[0]].append(summy[1])
                #Add each summary type once and give the post object the array of windows
                for summary_type in summary_window_map:
                    mp.add_summary_type(event_type, summary_type, summary_window_map[summary_type])
        # Publish to MQ
        if self.mq:
           self.publishToMq(arguments, event_types, datapoints, summaries_data)
        # Added the old metadata key
        mp.add_freeform_key_value("org_metadata_key", metadata_key)
        new_meta = mp.post_metadata()
        # Catching bad posts                                                                                                                              
        if new_meta is None:
            raise Exception("Post metadata empty, possible problem with user and key")
        #Getting data already in the data store
        et = EventTypeBulkPost(self.goc, username=self.username, api_key=self.key, metadata_key=new_meta.metadata_key)
        for event_type in event_types:
            for epoch in datapoints[event_type]:
                # packet-loss-rate is read as a float but should be uploaded as a dict with denominator and numerator                                     
                if event_type in ['packet-loss-rate', 'packet-loss-rate-bidir']:
                    # Some extra protection incase the number of datapoints in packet-loss-setn and packet-loss-rate does not match
                    packetcountsent = 210
                    packetcountlost = 0
                    specialTypes = ['packet-count-sent', 'packet-count-lost']
                    if event_type == 'packet-loss-rate-bidir':
                        specialTypes = ['packet-count-sent', 'packet-count-lost-bidir']                    
                    for specialType in specialTypes:
                        if not epoch in datapoints[specialType].keys():
                            self.add2log("Something went wrong time epoch %s not found for %s fixing it" % (specialType, epoch))
                            datapoints_added = self.getMissingData(epoch, metadata_key, specialType)
                            # Try to get the data once more because we know it is there
                            try:
                                value = datapoints_added[specialType][epoch]
                            except Exception as err:
                                # Since we are trying to double check for the first time sleep some time before going all berserker again
                                time.sleep(5)
                                datapoints_added = self.getMissingData(epoch, metadata_key, specialType)
                            value = datapoints_added[specialType][epoch]
                            datapoints[specialType][epoch] = value
                            et.add_data_point(specialType, epoch, value)
                    packetcountsent = datapoints['packet-count-sent'][epoch]
                    if event_type == 'packet-loss-rate-bidir':
                        packetcountlost = datapoints['packet-count-lost-bidir'][epoch]
                    else:
                        packetcountlost = datapoints['packet-count-lost'][epoch]
                    et.add_data_point(event_type, epoch, {'denominator': packetcountsent, 'numerator': packetcountlost})
                    # For the rests the data points are uploaded as they are read                                                         
                else:
                    # datapoint are tuples the first field is epoc the second the value  
                    et.add_data_point(event_type, epoch, datapoints[event_type][epoch])
        if disp:
            self.add2log("Datapoints to upload:")
            self.add2log(et.json_payload())
        et.post_data()
        self.add2log("posting NEW METADATA/DATA %s" % new_meta.metadata_key)      



