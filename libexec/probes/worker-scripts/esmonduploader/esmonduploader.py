import os
import time
import inspect
from time import strftime
from time import localtime

from optparse import OptionParser
from fractions import Fraction

from esmond.api.client.perfsonar.query import ApiFilters
from esmond.api.client.perfsonar.query import ApiConnect
# New module with the socks5 that inherics ApiConnect
from SocksApiConnect import SocksApiConnect

from esmond.api.client.perfsonar.post import MetadataPost, EventTypePost, EventTypeBulkPost


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
parser.add_option('-s', '--start', help='set start time for gathering data (default is -12 hours)', dest='start', default=-43200)
parser.add_option('-u', '--url', help='set url to gather data from (default is http://hcc-pki-ps02.unl.edu)', dest='url', default='http://hcc-pki-ps02.unl.edu')
parser.add_option('-w', '--user', help='the username to upload the information to the GOC', dest='username', default='afitz', action='store')
parser.add_option('-k', '--key', help='the key to upload the information to the goc', dest='key', default='fc077a6a133b22618172bbb50a1d3104a23b2050', action='store')
parser.add_option('-g', '--goc', help='the goc address to upload the information to', dest='goc', default='http://osgnetds.grid.iu.edu', action='store')
parser.add_option('-t', '--timeout', help='the maxtimeout that the probe is allowed to run in secs', dest='timeout', default=1000, action='store')
parser.add_option('-x', '--summaries', help='upload and read data summaries', dest='summary', default=True, action='store')
parser.add_option('-a', '--allowedEvents', help='The allowedEvents', dest='allowedEvents', default=False, action='store')

(opts, args) = parser.parse_args()

class EsmondUploader(object):

    def add2log(self, log):
        print strftime("%a, %d %b %Y %H:%M:%S", localtime()), str(log)
    
    def __init__(self,verbose,start,end,connect,username='afitz',key='fc077a6a133b22618172bbb50a1d3104a23b2050', goc='http://osgnetds.grid.iu.edu', allowedEvents='packet-loss-rate'):
        # Filter variables
        filters.verbose = verbose
        filters.time_start = time.time() - 1*start
        filters.time_end = time.time()
        filterDates = (strftime("%a, %d %b %Y %H:%M:%S ", time.gmtime(filters.time_start)), strftime("%a, %d %b %Y %H:%M:%S", time.gmtime(filters.time_end)))
        self.add2log("Data interval is from %s, to %s " %filterDates)
        # gfiltesrs and in general g* means connecting to the cassandra db at the central place ie goc
        gfilters.verbose = False        
        gfilters.time_start = time.time() - 5*start
        gfilters.time_end = time.time()
        gfilters.input_source = connect

        # Username/Key/Location/Delay
        self.connect = connect
        self.username = username
        self.key = key
        self.goc = goc
        self.conn = SocksApiConnect(self.connect, filters)
        self.gconn = ApiConnect(self.goc, gfilters)
                
        # List to store the old metadata to be sure the same metadata is not upload twice
        self.old_list = []
        # Conver the allowedEvents into a list
        self.allowedEvents = allowedEvents.split(',')
   
    # Get Existing GOC Data
    def getGoc(self, disp=False):
        if disp:
            self.add2log("Getting old data...")
        for gmd in self.gconn.get_metadata():
            if "org_metadata_key" in gmd._data:
                self.old_list.append(gmd._data["org_metadata_key"])
   
    # Get Data
    def getData(self, disp=False, summary=True):
        #self.getGoc(disp)
        self.add2log("Only reading data for event types: %s" % (str(self.allowedEvents)))
        if summary:
            self.add2log("Reading Summaries")
        else:
            self.add2log("Omiting Sumaries")
        for md in self.conn.get_metadata():
            # Check for repeat data
            if md.metadata_key in self.old_list:
                continue
            else:
                # It is a new metadata
                # Building the arguments for the post
                arguments = {
                    "subject_type": md.subject_type,
                    "source": md.source,
                    "destination": md.destination,
                    "tool_name": md.tool_name,
                    "measurement_agent": md.measurement_agent,
                    "input_source": self.connect,
                    "input_destination": self.goc
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
                # datapoints is a dict of lists
                # Each of its members are lists of datapoints of a given event_type
                datapoints = {}
                datapointSample = {}
                # et = event type
                #for eventype in event_types: 
                for et in md.get_all_event_types():
                    eventype = et.event_type
                    datapoints[eventype] = []
                    #et = md.get_event_type(eventype)
                    if summary:
                        summaries[eventype] = et.summaries
                    else:
                        summaries[eventype] = []
                    # Skip readind data points for certain event types to improv efficiency  
                    if eventype not in self.allowedEvents:                                                                                                  
                       continue
                    dpay = et.get_data()
                    for dp in dpay.data:
                        tup = (dp.ts_epoch, dp.val)
                        datapoints[eventype].append(tup)
                    # print debugging data
                    self.add2log("For event type %s, %d new data points"  %(eventype, len(datapoints[eventype])))
                    if len(datapoints[eventype]) > 0 and not isinstance(datapoints[eventype][0][1], (dict,list)): 
                        # picking the first one as the sample
                        datapointSample[eventype] = datapoints[eventype][0]
                self.add2log("Sample of the data being posted %s" % datapointSample)
            self.postData2(arguments, event_types, summaries, metadata_key, datapoints, summary, disp)


    def postData2(self, arguments, event_types, summaries, metadata_key, datapoints, summary = True, disp=False):
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
        # Added the old metadata key
        mp.add_freeform_key_value("org_metadata_key", metadata_key)
        new_meta = mp.post_metadata()
        # Catching bad posts                                                                                                                              
        if new_meta is None:
            raise Exception("Post metada empty, possible problem with user and key")
        self.add2log("posting NEW METADATA/DATA %s" % new_meta.metadata_key)
        et = EventTypeBulkPost(self.goc, username=self.username, api_key=self.key, metadata_key=new_meta.metadata_key)
        for event_type in event_types:
            # datapoint are tuples the first field is epoc the second the value                                                                          
            for datapoint in datapoints[event_type]:
                # packet-loss-rate is read as a float but should be uploaded as a dict with denominator and numerator                                     
                if 'packet-loss-rate' in event_type:
                    datapointIndex = datapoints[event_type].index(datapoint)
                    packetcountsent = datapoints['packet-count-sent'][datapointIndex][1]
                    packetcountlost = datapoints['packet-count-lost'][datapointIndex][1]
                    et.add_data_point(event_type, datapoint[0], {'denominator': packetcountsent, 'numerator': packetcountlost})
                    # For the rests the data points are uploaded as they are read                                                         
                else:
                    et.add_data_point(event_type, datapoint[0], datapoint[1])
        if disp:
            self.add2log("Datapoints to upload:")
            self.add2log(et.json_payload())
        et.post_data()
        self.add2log("Finish posting data for metadata")

