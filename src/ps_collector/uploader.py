import os
import time
import inspect
import json
import warnings
import sys
import configparser
from time import strftime
from time import localtime
import traceback

# Using the esmond_client instead of the rpm
from .esmond.api.client.perfsonar.query import ApiFilters
from .esmond.api.client.perfsonar.query import ApiConnect, ApiConnectWarning

# New module with socks5 OR SSL connection that inherits ApiConnect
from .SocksSSLApiConnect import SocksSSLApiConnect
from .SSLNodeInfo import EventTypeSSL
from .SSLNodeInfo import SummarySSL
from requests.exceptions import ConnectionError

# Set filter object
filters = ApiFilters()

class Uploader(object):
    
    def __init__(self, start = 1600,
                 connect = 'iut2-net3.iu.edu',
                 metricName='org.osg.general-perfsonar-simple.conf', 
                 config = None, log = None, backprocess_start = None, backprocess_end = None):
        self.log = log
        self.config = config
        ########################################
        ### New Section to read directly the configuration file                                                                     
        self.metricName =  metricName
        #conf_dir = os.path.join("/", "etc", "rsv", "metrics")
        ########################################        
        self.debug = self.str2bool(self.readConfigFile('debug', "false"))
        verbose = self.debug
        # Filter variables
        filters.verbose = True
        filters.ssl_verify = False
        #filters.verbose = True 
        # this are the filters that later will be used for the data
        if backprocess_start and backprocess_end:
            self.time_end = int(time.mktime(backprocess_end.timetuple()))
            self.time_start = int(time.mktime(backprocess_start.timetuple()))
            log.info("Inside uploader, starting backprocess")
        else:
            self.time_end = int(time.time())
            self.time_start = int(self.time_end - start)
        # Filter for metadata

        # For logging purposes
        filterDates = (strftime("%a, %d %b %Y %H:%M:%S ", time.gmtime(self.time_start)), strftime("%a, %d %b %Y %H:%M:%S", time.gmtime(self.time_end)))
        #filterDates = (strftime("%a, %d %b %Y %H:%M:%S ", time.gmtime(filters.time_start)))
        self.log.info("Data interval is from %s to %s" %filterDates)
        #self.log.info("Metada interval is from %s to now" % (filters.time_start))
        # Connection info to the perfsonar remote host that the data is gathered from.
        self.connect = connect
        self.cert = self.readConfigFile('usercert')
        self.certkey = self.readConfigFile('userkey')
        # The tmp dir structure is: tmp/metricName/host/metadatafiles
        self.tmpDir = os.path.join(self.readConfigFile('tmpdirectory'), self.metricName, self.connect + '/')
        # Convert the allowedEvents into a list
        allowedEvents = self.readConfigFile('allowedEvents')
        self.allowedEvents = allowedEvents.split(',')
        self.useSSL = False

        self.summary = self.str2bool(self.readConfigFile('summary'))
                
    # Get Data
    def getData(self):
        disp = self.debug
        self.log.info("Only reading data for event types: %s" % (str(self.allowedEvents)))
        summary = self.summary
        disp = self.debug
        if summary:
            self.log.info("Reading Summaries")
        else:
            self.log.info("Omitting Summaries")
        period = 3600
        for new_time_start in range(self.time_start, self.time_end, period):
             self.getDataHourChunks(new_time_start, new_time_start + period)

    def getDataHourChunks(self, time_start, time_end):
        filters.time_start = time_start
        filters.time_end = time_end
        self.log.debug("Querying between times {} and {}".format(time_start, time_end))

        # Do we have to use Socks?
        if self.useSSL == True:
            self.conn = SocksSSLApiConnect("https://"+self.connect, filters)
        else:
            self.conn = SocksSSLApiConnect("http://"+self.connect, filters)
        metadata = self.conn.get_metadata()
        #self.conn = None
        #metadata = None
        #try:
        #    self.conn = ApiConnect("http://" + self.connect, filters)
        #    metadata = self.conn.get_metadata()
        #except ApiConnectWarning as apiwarning:
        #    self.log.exception("Unable to connect to host")
        #    return
        
        try:
            #Test to see if https connection is succesfull  
            md = next(metadata)
            self.readMetaData(md)
        except  StopIteration:
            self.log.warning("There is no metadata in this time range")
            return
        except ConnectionError as e:
            #Test to see if https connection is sucesful                                                                                               
            self.log.exception("Unable to connect to %s, exception was %s, trying SSL" % ("http://"+self.connect, type(e)))
            try:
                metadata = self.conn.get_metadata(cert=self.cert, key=self.certkey)
                md = next(metadata)
                self.useSSL = True
                self.readMetaData(md)
            except  StopIteration:
                self.log.warning("There is no metadata in this time range")
            except  ConnectionError as e:
                raise Exception("Unable to connect to %s, exception was %s, " % ("https://"+self.connect, type(e)))
        except Exception as e:
            self.log.exception("Failed to read metadata")
            return

        for md in metadata:
            self.readMetaData(md)

    # Md is a metadata object of query
    def readMetaData(self, md):
        disp = self.debug
        disp = True
        summary = self.summary
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
        self.log.info("Reading New METADATA/DATA %s" % (md.metadata_key))
        if disp:
            self.log.debug("Posting args: ")
            self.log.debug(arguments)
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
            self.log.exception("first time for %s" % (md.metadata_key))
        except ValueError:
            # decoding failed
            self.log.exception("first time for %s" % (md.metadata_key))
        for et in md.get_all_event_types():
            if self.useSSL:
                etSSL = EventTypeSSL(et, self.cert, self.certkey)
                et = etSSL
            # Adding the time.end filter for the data since it is not used for the metadata
            #use previously recorded end time if available
            et.filters.time_start = filters.time_start
            if et.event_type in list(self.time_starts.keys()):
                et.filters.time_start = self.time_starts[et.event_type]
                self.log.info("loaded previous time_start %s" % et.filters.time_start)
            et.filters.time_end = filters.time_end
            if et.filters.time_end <  et.filters.time_start:
                continue
            eventype = et.event_type
            datapoints[eventype] = {}
            if summary:
                summaries[eventype] = et.summaries
            else:
                summaries[eventype] = []
            # Skip reading data points for certain event types to improv efficiency  
            if eventype not in self.allowedEvents:
                continue
            # Read summary data 
            if summary:
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
            warnings.filterwarnings('error')
            try:
                dpay = et.get_data()
            except Warning:
                raise Exception("Unable to read data  exception: %s" )
            tup = ()
            for dp in dpay.data:
                tup = (dp.ts_epoch, dp.val)
                datapoints[eventype][dp.ts_epoch] = dp.val
                # print debugging data
            self.log.info("For event type %s, %d new data points"  %(eventype, len(datapoints[eventype])))
            if len(datapoints[eventype]) > 0 and not isinstance(tup[1], (dict,list)): 
                # picking the first one as the sample
                datapointSample[eventype] = tup[1]
        self.log.debug("Sample of the data being posted %s" % datapointSample)
        try:
            self.postData(arguments, event_types, summaries, summaries_data, metadata_key, datapoints)
        except Exception as e:
            import traceback
            traceback.print_exc()
            raise e
            #raise Exception("Unable to post because exception: %s"  % repr(e))
 
    # Experimental function to try to recover from missing packet-count-sent or packet-count-lost data
    def getMissingData(self, timestamp, metadata_key, event_type):
        self.log.info("Trying to get missing data")
        disp = self.debug
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
                    self.log.debug("point found")
                    datapoints[event_type][dp.ts_epoch] = dp.val
        return datapoints
                
    # Place holde for posting Data if it is to Esmond, ActiveMQ, RabbitMQ
    def postData(self, arguments, event_types, summaries, summaries_data, metadata_key, datapoints):
        pass

    def readConfigFile(self, key, default = ""):
        section = self.metricName + " args"
        try:
            return self.config.get(section, key)
        except configparser.NoOptionError:
            self.log.warning("Error getting key: %s from config file", key)
            return default
            

    def str2bool(self,word):
        return word.lower() in ("true")

    def total_size(o, handlers={}, verbose=False):
        """ Returns the approximate memory footprint an object and all of its contents.

        Automatically finds the contents of the following builtin containers and
        their subclasses:  tuple, list, deque, dict, set and frozenset.
        To search other containers, add handlers to iterate over their contents:

            handlers = {SomeContainerClass: iter,
                        OtherContainerClass: OtherContainerClass.get_elements}
    
        """
        dict_handler = lambda d: chain.from_iterable(list(d.items()))
        all_handlers = {tuple: iter,
                        list: iter,
                        dict: dict_handler,
                        set: iter,
                        frozenset: iter,
                    }
        #all_handlers.update(handlers)     # user handlers take precedence
        seen = set()                      # track which object id's have already been seen
        default_size = sys.getsizeof(0)       # estimate sizeof object without __sizeof__

        def sizeof(o):
            if id(o) in seen:       # do not double count the same object
                return 0
            seen.add(id(o))
            s = sys.getsizeof(o, default_size)

            for typ, handler in list(all_handlers.items()):
                if isinstance(o, typ):
                    s += sum(map(sizeof, handler(o)))
                    break
            return s
