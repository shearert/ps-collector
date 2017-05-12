from uploader import *
# Esmond libraries to post back to the data store
from esmond_client.perfsonar.post import MetadataPost, EventTypePost, EventTypeBulkPost
from esmond_client.perfsonar.post import EventTypeBulkPostWarning, EventTypePostWarning

class EsmondUploader(Uploader):

    def __init__(self, start = 1600, connect = 'iut2-net3.iu.edu', metricName='org.osg.general-perfsonar-simple.conf'):
        Uploader.__init__(self, start = start, connect = connect, metricName = metricName)
        self.username = self.readConfigFile('username')
        self.key = self.readConfigFile('key')
        self.goc = self.readConfigFile('goc')
        
        gfilters = ApiFilters()
        # gfiltesrs and in general g* means connecting to the cassandra db at the central place ie goc                                                          
        gfilters.verbose = False
        gfilters.time_start = int(self.time_end - 5*start)
        gfilters.time_end = self.time_end
        gfilters.input_source = connect
        self.gconn = ApiConnect(self.goc, gfilters)


    def postMetaData(self, arguments, event_types, summaries, summaries_data, metadata_key, datapoints, summary = True, disp=False):
         mp = MetadataPost(self.goc, username=self.username, api_key=self.key, **arguments)
         for event_type in summaries.keys():
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
         return new_meta
    
    # Post data points from a metadata
    def postBulkData(self, new_meta, old_metadata_key, datapoints, disp=False):
        et = EventTypeBulkPost(self.goc, username=self.username, api_key=self.key, metadata_key=new_meta.metadata_key)
        for event_type in datapoints.keys():
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
                            time.sleep(5)
                            datapoints_added = self.getMissingData(epoch, old_metadata_key, specialType)
                            # Try to get the data once more because we know it is there                                                                     
  
                            try:
                                value = datapoints_added[specialType][epoch]
                            except Exception as err:
                                datapoints_added[specialType][epoch] = 0
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
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter('error',  EventTypePostWarning)
            try:
                et.post_data()
            # Some EventTypePostWarning went wrong:                                                                                                          
            except Exception as err:
                self.add2log("Probably this data already existed")
                #self.postDataSlow(json.loads(et.json_payload()), new_meta.metadata_key, datapoints, disp)                                                   
            for event_type in datapoints.keys():
                if len(datapoints[event_type].keys()) > 0:
                    if event_type not in self.time_starts:
                        self.time_starts[event_type] = 0
                    next_time_start = max(datapoints[event_type].keys())+1
                    if next_time_start > self.time_starts[event_type]:
                        self.time_starts[event_type] = int(next_time_start)
            f = open(self.tmpDir + old_metadata_key, 'w')
            f.write(json.dumps(self.time_starts))
            f.close()
        self.add2log("posting NEW METADATA/DATA %s" % new_meta.metadata_key)

    def postData(self, arguments, event_types, summaries, summaries_data, metadata_key, datapoints, summary = True, disp=False):
        lenght_post = -1
        for event_type in datapoints.keys():
            if len(datapoints[event_type])>lenght_post:
                lenght_post = len(datapoints[event_type])
        new_meta = self.postMetaData(arguments, event_types, summaries, summaries_data, metadata_key, datapoints, summary, disp)
        # Catching bad posts                                                                                                                                 
        if new_meta is None:
                raise Exception("Post metadata empty, possible problem with user and key")
        if lenght_post == 0:
            self.add2log("No new datapoints skipping posting for efficiency")
            return
        step_size = 100
        for step in range(0, lenght_post, step_size):
            chunk_datapoints = {}
            for event_type in datapoints.keys():
                chunk_datapoints[event_type] = {}
                if len(datapoints[event_type].keys())>0:
                    pointsconsider = sorted(datapoints[event_type].keys())[step:step+step_size]
                    for point in pointsconsider:
                        chunk_datapoints[event_type][point] = datapoints[event_type][point]
            self.postBulkData(new_meta, metadata_key, chunk_datapoints, disp=False)

            
