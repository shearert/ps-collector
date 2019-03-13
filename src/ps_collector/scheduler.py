from __future__ import print_function

import functools
import logging
import multiprocessing
from multiprocessing.dummy import Pool as ThreadPool

import time

import schedule
from prometheus_client import start_http_server

# Use pebble because it has timeouts for the schedule
import pebble


import ps_collector.config
import ps_collector.sharedrabbitmq
from ps_collector.rabbitmquploader import RabbitMQUploader
from ps_collector.mesh import Mesh
import ps_collector
from ps_collector.monitoring import timed_execution, Monitoring

# The conversion factor from minutes to seconds:
MINUTE = 60

log = None



class SchedulerState(object):

    def __init__(self, cp, pool, log):
        self.pool = pool
        self.cp = cp
        self.probes = set()
        self.futures = {}
        self.log = log



def query_ps_child(cp, endpoint):
    reverse_dns = endpoint.split(".")
    reverse_dns = ".".join(reverse_dns[::-1])
    log = logging.getLogger("perfSonar.{}".format(reverse_dns))
    log.info("I query endpoint {}.".format(endpoint))
    with timed_execution(endpoint):
        uploader = RabbitMQUploader(connect=endpoint, config=cp, log = log)
        return uploader.getData()


def query_ps(state, endpoint):
    old_future = state.futures.get(endpoint)
    if old_future:
        if not old_future.done():
            state.log.info("Prior probe {} is still running; skipping query.".format(endpoint))
            return
        # For now, ignore the result.
        try:
            old_future.result()
        except Exception as e:
            state.log.exception("Failed to get data last time for endpoint {0}: ".format(endpoint))
            Monitoring.SendEndpointFailure(endpoint)
        finally:
            Monitoring.DecRequestsPending()

    timeout = state.cp.getint("Scheduler", "query_timeout") * MINUTE
    result = state.pool.schedule(query_ps_child, args=(state.cp, endpoint), timeout=timeout)
    Monitoring.IncRequestsPending()
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
        Monitoring.DecNumEndpoints()
        state.probes.remove(probe)
        schedule.clear(probe)
        future = state.futures.get(probe)
        if not future:
            continue
        future.wait()

    default_probe_interval = state.cp.getint("Scheduler", "probe_interval") * MINUTE

    for probe in probes_to_start:
        state.log.debug("Adding probe: %s", probe)
        Monitoring.IncNumEndpoints()
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
            if cur_future.done():
                try:
                    cur_future.result()
                except Exception as e:
                    state.log.exception("Failed to get data last time for endpoint {0}:".format(endpoint))
                    Monitoring.SendEndpointFailure(endpoint)
                state.futures[endpoint] = None
                Monitoring.DecRequestsPending()

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
    pool = pebble.ProcessPool(max_workers = pool_size, max_tasks=5)

    state = SchedulerState(cp, pool, log)

    # Query the mesh the first time
    query_ps_mesh(state)

    query_ps_mesh_job = functools.partial(query_ps_mesh, state)
    cleanup_futures_job = functools.partial(cleanup_futures, state)

    mesh_interval_s = cp.getint("Scheduler", "mesh_interval") * MINUTE
    log.info("Will update the mesh config every %d seconds.", mesh_interval_s)
    schedule.every(mesh_interval_s).to(mesh_interval_s + MINUTE).seconds.do(query_ps_mesh_job)

    schedule.every(10).seconds.do(cleanup_futures_job)

    monitor = Monitoring()
    # Start the prometheus webserver
    start_http_server(8000)
    try:
        while True:
            schedule.run_pending()
            monitor.process_messages()
            time.sleep(1)

    except:
        pool.stop()
        pool.join()
        raise
