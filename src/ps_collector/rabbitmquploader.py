from uploader import Uploader
# Need to push to Rabbit mq
import pika
from ps_collector.sharedrabbitmq import SharedRabbitMQ
import ps_collector
import time
import json


class RabbitMQUploader(Uploader):
    
    def __init__(self, start = 1600, connect = 'iut2-net3.iu.edu',
                 metricName='org.osg.general.perfsonar-rabbitmq-simple',
                 config = None):
        Uploader.__init__(self, start, connect, metricName, config)
        
        self.channel = ps_collector.get_rabbitmq_connection(config).createChannel()
        self.maxMQmessageSize =  self.readConfigFile('mq-max-message-size')

    def __del__(self):
        if self.channel and self.channel.is_open:
            self.channel.close()
        

    # Publish summaries to Mq
    def publishSToMq(self, arguments, event_types, summaries, summaries_data):
        for event in summaries_data.keys():
            if not summaries_data[event]:
                continue
            arguments['rsv-timestamp'] = "%s" % time.time()
            arguments['event-type'] =  event
            arguments['summaries'] = 1
            msg_body = { 'meta': arguments }
            msg_body['summaries'] = summaries_data[event]
            self.SendMessagetoMQ(msg_body, event)

    def SendMessagetoMQ(self, msg_body, event):
        # the max size limit in KB but python expects it in bytes                                                                           
        size_limit = self.maxMQmessageSize * 1000
        size_msg = self.total_size(msg_body)
        # if size of the message is larger than 10MB discarrd                                                                             
        if size_msg > size_limit:
            self.add2log("Size of message body bigger than limit, discarding")
            return
        # add to mq
        result = None
        for tries in range(5):
            try:
                result = self.channel.basic_publish(exchange = self.config.get('rabbitmq', 'exchange'),
                                                routing_key = 'perfsonar.raw.' + event,
                                                body = json.dumps(msg_body), 
                                                properties = pika.BasicProperties(delivery_mode = 2))
                break
                if not result:
                    raise Exception('ERROR: Exception publishing to rabbit MQ', 'Problem publishing to mq')
            except Exception as e:
                import traceback
                traceback.print_exc()
                self.add2log("Restarting pika connection,, exception was %s, " % (repr(e)))
                #self.restartPikaConnection()
        if result == None:
                self.add2log("ERROR: Failed to send message to mq, exception was %s" % (repr(e)))

    # Publish message to Mq
    def publishRToMq(self, arguments, event_types, datapoints):
        for event in datapoints.keys():
            # filter events for mq (must be subset of the probe's filter)
            if event not in self.allowedEvents:
                continue
            # skip events that have no datapoints 
            if not datapoints[event]:
                continue
            # compose msg
            arguments['rsv-timestamp'] = "%s" % time.time()
            arguments['event-type'] =  event
            arguments['summaries'] = 0
            # including the timestart of the smalles measureement
            ts_start = min(datapoints[event].keys())
            arguments['ts_start'] = ts_start
            msg_body = { 'meta': arguments }
            msg_body['datapoints'] = datapoints[event]
            self.SendMessagetoMQ(msg_body, event)

    def postData(self, arguments, event_types, summaries, summaries_data, metadata_key, datapoints):
        summary= self.summary
        disp = self.debug
        lenght_post = -1
        arguments['org_metadata_key'] = metadata_key
        for event_type in datapoints.keys():
            if len(datapoints[event_type])>lenght_post:
                lenght_post = len(datapoints[event_type])
        if lenght_post == 0:
            self.add2log("No new datapoints skipping posting for efficiency")
            return

        # Now that we know we have data to send, actually connect upstream.
        if self.channel is None:
            self.channel = ps_collector.get_rabbitmq_connection(self.config).createChannel()

        if summaries_data:
            self.add2log("posting new summaries")
            self.publishSToMq(arguments, event_types, summaries, summaries_data)
        step_size = 200
        self.add2log("Length of the post: %s " % lenght_post)
        for step in range(0, lenght_post, step_size):
            chunk_datapoints = {}
            for event_type in datapoints.keys():
                chunk_datapoints[event_type] = {}
                if len(datapoints[event_type].keys())>0:
                    pointsconsider = sorted(datapoints[event_type].keys())[step:step+step_size]
                    for point in pointsconsider:
                        chunk_datapoints[event_type][point] = datapoints[event_type][point]
            self.publishRToMq(arguments, event_types, chunk_datapoints)
            # Updating the checkpoint files for each host/metric and metadata
            for event_type in chunk_datapoints.keys():
                if len(chunk_datapoints[event_type].keys()) > 0:
                    if event_type not in self.time_starts:
                        self.time_starts[event_type] = 0
                    next_time_start = max(chunk_datapoints[event_type].keys())+1
                    if next_time_start > self.time_starts[event_type]:
                        self.time_starts[event_type] = int(next_time_start)
            f = open(self.tmpDir + metadata_key, 'w')
            f.write(json.dumps(self.time_starts))
            f.close()
            self.add2log("posting NEW METADATA/DATA to rabbitMQ %s" % metadata_key)
