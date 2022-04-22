from .uploader import Uploader
# # Need to push to Rabbit mq
# import pika
# from ps_collector.sharedrabbitmq import SharedRabbitMQ
import ps_collector
import time
import json
import tempfile
import os
import errno

import requests

class HTTPArchiverUploader(Uploader):
    def __init__(self, start = 1600,
                 connect = 'iut2-net3.iu.edu',
                 metricName='org.osg.general-perfsonar-simple.conf', 
                 config = None, log = None, backprocess_start = None, backprocess_end = None):
        Uploader.__init__(self,start,connect,config,log,backprocess_start,backprocess_end)

    def postarchiver(self,msg_body,event):
        result = None
        try:
            requests.post('https://ps-collection.atlas-ml.org',data=msg_body)
            break
        except Exception as e:
            self.log.exception("Failed to Upload to archiver,, exception was %s, " % (repr(e)))
    
    def createbody(self,arguments,event_types,datapoints):
         for event in list(datapoints.keys()):
#             # filter events for mq (must be subset of the probe's filter)
             if event not in self.allowedEvents:
                 continue
#             # skip events that have no datapoints 
            if not datapoints[event]:
                continue
#             # compose msg
            arguments['rsv-timestamp'] = "%s" % time.time()
            arguments['event-type'] =  event
            arguments['summaries'] = 0
#             # including the timestart of the smalles measureement
            ts_start = min(datapoints[event].keys())
            arguments['ts_start'] = ts_start
            msg_body = {"archiver":"http","data":{"_url":"https://ps-collection.atlas-ml.org",
                "op":"post","retry_policy":[{"attempts":5,"wait":"PT1M"},
                {"attempts":11,"wait":"PT5M"},{"attempts":23,"wait":"PT1H"},
                {"attempts":6,"wait":"PT4H"}]},
                "ttl":"P2DT1H"}
            msg_body['data']['version'] = 2
            msg_body['data']['datapoints'] = datapoints[event]
            self.postarchiver(msg_body, event)

    def postData(self, arguments, event_types, summaries, summaries_data, metadata_key, datapoints):
        summary= self.summary
        disp = self.debug
        lenght_post = -1
        arguments['org_metadata_key'] = metadata_key
        for event_type in list(datapoints.keys()):
            if len(datapoints[event_type])>lenght_post:
                lenght_post = len(datapoints[event_type])
        if lenght_post == 0:
            self.log.info("No new datapoints skipping posting for efficiency")
            return
        step_size = 200
        self.log.info("Length of the post: %s " % lenght_post)
        for step in range(0, lenght_post, step_size):
            chunk_datapoints = {}
            for event_type in list(datapoints.keys()):
                chunk_datapoints[event_type] = {}
                if len(list(datapoints[event_type].keys()))>0:
                    pointsconsider = sorted(datapoints[event_type].keys())[step:step+step_size]
                    for point in pointsconsider:
                        chunk_datapoints[event_type][point] = datapoints[event_type][point]
            self.createbody(arguments, event_types, chunk_datapoints)
            # Updating the checkpoint files for each host/metric and metadata
            for event_type in list(chunk_datapoints.keys()):
                if len(list(chunk_datapoints[event_type].keys())) > 0:
                    if event_type not in self.time_starts:
                        self.time_starts[event_type] = 0
                    next_time_start = max(chunk_datapoints[event_type].keys())+1
                    if next_time_start > self.time_starts[event_type]:
                        self.time_starts[event_type] = int(next_time_start)

            self.writeCheckpoint(metadata_key, self.time_starts)
            self.log.info("posting NEW METADATA/DATA to rabbitMQ %s" % metadata_key)

    def writeCheckpoint(self, metadata_key, times):
        """
        Perform an atomic write to the checkpoint file
        """
        # Make sure that the directory exists
        try:
            os.makedirs(self.tmpDir)
        except OSError as e:
            if e.errno != errno.EEXIST:
                raise

        # Open a temporary file
        (file_handle, tmp_filename) = tempfile.mkstemp(dir=self.tmpDir)
        wrapped_file = os.fdopen(file_handle, 'w')
        wrapped_file.write(json.dumps(times))
        wrapped_file.close()
        os.rename(tmp_filename, self.tmpDir + metadata_key)

# class RabbitMQUploader(Uploader):
    
#     def __init__(self, start = 1600, connect = 'iut2-net3.iu.edu',
#                  metricName='org.osg.general.perfsonar-rabbitmq-simple',
#                  config = None, log = None, backprocess_start = None, backprocess_end = None):
#         Uploader.__init__(self, start, connect, metricName, config, log, backprocess_start, backprocess_end)
        
#         self.channel = ps_collector.get_rabbitmq_connection(config).createChannel()
#         self.maxMQmessageSize =  self.readConfigFile('mq-max-message-size')

#     def __del__(self):
#         #self.log.exception("Del odd")
#         if self.channel and self.channel.is_open:
#             self.channel.close()

#     # Publish summaries to Mq
#     def publishSToMq(self, arguments, event_types, summaries, summaries_data):
#         for event in list(summaries_data.keys()):
#             if not summaries_data[event]:
#                 continue
#             arguments['rsv-timestamp'] = "%s" % time.time()
#             arguments['event-type'] =  event
#             arguments['summaries'] = 1
#             msg_body = { 'meta': arguments }
#             msg_body['summaries'] = summaries_data[event]
#             self.SendMessagetoMQ(msg_body, event)

#     def SendMessagetoMQ(self, msg_body, event):
#         # the max size limit in KB but python expects it in bytes                                                                           
#         size_limit = self.maxMQmessageSize * 1000
#         #json_str = msg_body.dumps(msg_body)
#         # if size of the message is larger than 10MB discarrd                                                                             
#         #if size_msg > size_limit:
#         #    self.log.warning("Size of message body bigger than limit, discarding")
#         #    return
#         # add to mq
#         result = None
#         for tries in range(5):
#             try:
#                 self.channel.basic_publish(exchange = self.config.get('rabbitmq', 'exchange'),
#                                                 routing_key = 'perfsonar.raw.' + event,
#                                                 body = json.dumps(msg_body), 
#                                                 properties = pika.BasicProperties(delivery_mode = 2))
#                 break
#             except Exception as e:
#                 self.log.exception("Restarting pika connection,, exception was %s, " % (repr(e)))
#                 self.channel = ps_collector.get_rabbitmq_connection(self.config).createChannel()


#     # Publish message to Mq
#     def publishRToMq(self, arguments, event_types, datapoints):
#         for event in list(datapoints.keys()):
#             # filter events for mq (must be subset of the probe's filter)
#             if event not in self.allowedEvents:
#                 continue
#             # skip events that have no datapoints 
#             if not datapoints[event]:
#                 continue
#             # compose msg
#             arguments['rsv-timestamp'] = "%s" % time.time()
#             arguments['event-type'] =  event
#             arguments['summaries'] = 0
#             # including the timestart of the smalles measureement
#             ts_start = min(datapoints[event].keys())
#             arguments['ts_start'] = ts_start
#             msg_body = { 'meta': arguments }
#             msg_body['version'] = 2
#             msg_body['datapoints'] = datapoints[event]
#             self.SendMessagetoMQ(msg_body, event)

#     def postData(self, arguments, event_types, summaries, summaries_data, metadata_key, datapoints):
#         summary= self.summary
#         disp = self.debug
#         lenght_post = -1
#         arguments['org_metadata_key'] = metadata_key
#         for event_type in list(datapoints.keys()):
#             if len(datapoints[event_type])>lenght_post:
#                 lenght_post = len(datapoints[event_type])
#         if lenght_post == 0:
#             self.log.info("No new datapoints skipping posting for efficiency")
#             return

#         # Now that we know we have data to send, actually connect upstream.
#         if self.channel is None:
#             self.channel = ps_collector.get_rabbitmq_connection(self.config).createChannel()

#         if summaries_data:
#             self.log.info("posting new summaries")
#             self.publishSToMq(arguments, event_types, summaries, summaries_data)
#         step_size = 200
#         self.log.info("Length of the post: %s " % lenght_post)
#         for step in range(0, lenght_post, step_size):
#             chunk_datapoints = {}
#             for event_type in list(datapoints.keys()):
#                 chunk_datapoints[event_type] = {}
#                 if len(list(datapoints[event_type].keys()))>0:
#                     pointsconsider = sorted(datapoints[event_type].keys())[step:step+step_size]
#                     for point in pointsconsider:
#                         chunk_datapoints[event_type][point] = datapoints[event_type][point]
#             self.publishRToMq(arguments, event_types, chunk_datapoints)
#             # Updating the checkpoint files for each host/metric and metadata
#             for event_type in list(chunk_datapoints.keys()):
#                 if len(list(chunk_datapoints[event_type].keys())) > 0:
#                     if event_type not in self.time_starts:
#                         self.time_starts[event_type] = 0
#                     next_time_start = max(chunk_datapoints[event_type].keys())+1
#                     if next_time_start > self.time_starts[event_type]:
#                         self.time_starts[event_type] = int(next_time_start)

#             self.writeCheckpoint(metadata_key, self.time_starts)
#             self.log.info("posting NEW METADATA/DATA to rabbitMQ %s" % metadata_key)

#     def writeCheckpoint(self, metadata_key, times):
#         """
#         Perform an atomic write to the checkpoint file
#         """
#         # Make sure that the directory exists
#         try:
#             os.makedirs(self.tmpDir)
#         except OSError as e:
#             if e.errno != errno.EEXIST:
#                 raise

#         # Open a temporary file
#         (file_handle, tmp_filename) = tempfile.mkstemp(dir=self.tmpDir)
#         wrapped_file = os.fdopen(file_handle, 'w')
#         wrapped_file.write(json.dumps(times))
#         wrapped_file.close()
#         os.rename(tmp_filename, self.tmpDir + metadata_key)



