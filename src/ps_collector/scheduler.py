from __future__ import print_function

import functools
import logging
import multiprocessing
from multiprocessing import Queue
from multiprocessing.dummy import Pool as ThreadPool
import Queue
import time

import schedule
from prometheus_client import Summary, Gauge
from prometheus_client import start_http_server


import ps_collector.config
import ps_collector.sharedrabbitmq
from ps_collector.rabbitmquploader import RabbitMQUploader
from ps_collector.mesh import Mesh
import ps_collector
from ps_collector.monitoring import timed_execution

# The conversion factor from minutes to seconds:
MINUTE = 60

log = None

communication_manager = multiprocessing.Manager()
communication_queue = communication_manager.Queue()

class SchedulerState(object):

    def __init__(self, cp, pool, log):
        self.pool = pool
        self.cp = cp
        self.probes = set()
        self.futures = {}
        self.log = log


IN_PROGRESS = Gauge("ps_inprogress_requests", "Number of requests queued or running")
request_summary = Summary('ps_request_latency_seconds', 'How long the request to the remote perfsonar took', ['endpoint'])
NUM_ENDPOINTS = Gauge("ps_num_endpoint", "How many endpoints are being queried")

def query_ps_child(cp, endpoint):
    reverse_dns = endpoint.split(".")
    reverse_dns = ".".join(reverse_dns[::-1])
    log = logging.getLogger("perfSonar.{}".format(reverse_dns))
    log.info("I query endpoint {}.".format(endpoint))
    with timed_execution(endpoint, communication_queue):
        uploader = RabbitMQUploader(connect=endpoint, config=cp, log = log)
        # Use a threading timeout
        # https://stackoverflow.com/questions/29494001/how-can-i-abort-a-task-in-a-multiprocessing-pool-after-a-timeout
        timeout = cp.getint("Scheduler", "query_timeout") * MINUTE
        p = ThreadPool(1)
        res = p.apply_async(uploader.getData)
        try:
            out = res.get(timeout)  # Wait timeout seconds for func to complete.
            p.close()
            p.join()
            del p
            return out
        except multiprocessing.TimeoutError:
            print("Aborting due to timeout")
            p.terminate()
            p.join()
            del p
            raise
        except Exception as e:
            log.exception("Failed to get data last time")
            p.close()
            p.join()
            del p
            raise

def query_ps(state, endpoint):
    old_future = state.futures.get(endpoint)
    if old_future:
        if not old_future.ready():
            state.log.info("Prior probe {} is still running; skipping query.".format(endpoint))
            return
        # For now, ignore the result.
        try:
            old_future.get()
        except Exception as e:
            state.log.exception("Failed to get data last time:")

    result = state.pool.apply_async(query_ps_child, (state.cp, endpoint))
    IN_PROGRESS.inc()
    state.futures[endpoint] = result


def query_ps_mesh(state):
    state.log.info("Querying PS mesh")
    # TODO: get a list of endpoints from the configured mesh config.

    mesh_endpoint = state.cp.get("Mesh", "endpoint")

    mesh = Mesh(state.cp.get("Mesh", "endpoint"))
    endpoints = mesh.get_nodes()
    state.log.info("Nodes: %s", endpoints)

    running_probes = set(state.probes)
    probes_to_stop = running_probes.difference(endpoints)
    probes_to_start = endpoints.difference(running_probes)

    for probe in probes_to_stop:
        state.log.debug("Stopping probe: %s", probe)
        NUM_ENDPOINTS.dec()
        state.probes.remove(probe)
        schedule.clear(probe)
        future = state.futures.get(probe)
        if not future:
            continue
        future.wait()

    default_probe_interval = state.cp.getint("Scheduler", "probe_interval") * MINUTE

    for probe in probes_to_start:
        state.log.debug("Adding probe: %s", probe)
        NUM_ENDPOINTS.inc()
        state.probes.add(probe)
        probe_interval = default_probe_interval
        if state.cp.has_section(probe) and state.cp.has_option("interval"):
            probe_interval = state.cp.getint(probe, "interval") * MINUTE

        query_ps_job = functools.partial(query_ps, state, probe)
        # Run the probe the first time
        query_ps_job()
        schedule.every(probe_interval).to(probe_interval + MINUTE).seconds.do(query_ps_job).tag(probe)

    time.sleep(5)
    logging.debug("Finished querying mesh")


def cleanup_futures(state):
    # Loop through the futures, cleaning up those that are done, and dec the gauge
    for endpoint in state.futures:
        cur_future = state.futures[endpoint]
        if cur_future:
            if cur_future.ready():
                try:
                    cur_future.get()
                except Exception as e:
                    state.log.exception("Failed to get data last time for endpoint {0}:".format(endpoint))
                state.futures[endpoint] = None
                IN_PROGRESS.dec()

def main():
    global MINUTE
    cp = ps_collector.config.get_config()
    if cp.has_option("Scheduler", "debug"):
        if cp.get("Scheduler", "debug").lower() == "true":
            MINUTE = 1
    ps_collector.config.setup_logging(cp)
    global log
    log = logging.getLogger("scheduler")

    pool_size = 5
    if cp.has_option("Scheduler", "pool_size"):
        pool_size = cp.getint("Scheduler", "pool_size")
    pool = multiprocessing.Pool(pool_size)

    state = SchedulerState(cp, pool, log)

    # Query the mesh the first time
    query_ps_mesh(state)

    query_ps_mesh_job = functools.partial(query_ps_mesh, state)
    cleanup_futures_job = functools.partial(cleanup_futures, state)

    mesh_interval_s = cp.getint("Scheduler", "mesh_interval") * MINUTE
    log.info("Will update the mesh config every %d seconds.", mesh_interval_s)
    schedule.every(mesh_interval_s).to(mesh_interval_s + MINUTE).seconds.do(query_ps_mesh_job)

    schedule.every(10).seconds.do(cleanup_futures_job)

    # Start the prometheus webserver
    start_http_server(8000)
    try:
        while True:
            schedule.run_pending()
            while True:
                try: 
                    item = communication_queue.get(True, 1)
                    print("Got from queue: {0}".format(item))
                    request_summary.labels(item[0]).observe(item[1])
                except Queue.Empty:
                    break
    except:
        pool.terminate()
        pool.join()
        raise
