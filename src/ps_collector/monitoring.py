
import multiprocessing
import queue
import time
from prometheus_client import Summary, Gauge, Counter

IN_PROGRESS = Gauge("ps_inprogress_requests", "Number of requests queued or running")
request_summary = Summary('ps_request_latency_seconds', 'How long the request to the remote perfsonar took', ['endpoint'])
NUM_ENDPOINTS = Gauge("ps_num_endpoint", "How many endpoints are being queried")
ENDPOINT_FAILURE = Counter('ps_endpoint_failures_num', 'How many failures have occured on an endpoint', ['endpoint'])

# Create this in the primary thread
communication_manager = multiprocessing.Manager()
communication_queue = communication_manager.Queue()

class Monitoring(object):

    QUERY_ELASPED_TYPE = "QUERY_ELAPSED"
    ENDPOINT_FAILURE_TYPE = "ENDPOINT_FAIL"

    def __init__(self):
        pass
    
    def process_messages(self):
        while True:
            try:
                item = communication_queue.get(False)        
                print(("Got from queue: {0}".format(item)))
                if item[0] == Monitoring.QUERY_ELASPED_TYPE:
                    request_summary.labels(item[1]).observe(item[2])
                elif item[0] == Monitoring.ENDPOINT_FAILURE_TYPE:
                    ENDPOINT_FAILURE.labels(item[1]).inc()
            except queue.Empty:
                break


    # Static methods for monitoring
    @staticmethod
    def SendQueryTime(endpoint, elapsed):
        """
        Send the time it took to query an enpoint to a interprocess queue.
        This queue will later be processed in the "process_messages" function.
        """
        # Interprocess communciation
        # Message format: [ type, endpoint, elapsed ]
        communication_queue.put([Monitoring.QUERY_ELASPED_TYPE, endpoint, elapsed])

    @staticmethod
    def SendEndpointFailure(endpoint):
        """
        Send if the endpoint has experienced a failure
        This queue will later be processed in the "process_messages" function.
        """
        # Interprocess communciation
        # Message format: [ type, endpoint ]
        communication_queue.put([Monitoring.ENDPOINT_FAILURE_TYPE, endpoint])


    @staticmethod
    def IncRequestsPending():
        IN_PROGRESS.inc()
    
    @staticmethod
    def DecRequestsPending():
        IN_PROGRESS.dec()
    
    @staticmethod
    def IncNumEndpoints():
        NUM_ENDPOINTS.inc()
    
    @staticmethod
    def DecNumEndpoints():
        NUM_ENDPOINTS.dec()


class timed_execution:

    def __init__(self, endpoint):
        self.start_time = 0
        self.endpoint = endpoint

    def __enter__(self):
        self.start_time = time.time()

    def __exit__(self, type, value, traceback):
        Monitoring.SendQueryTime(self.endpoint, time.time() - self.start_time)
