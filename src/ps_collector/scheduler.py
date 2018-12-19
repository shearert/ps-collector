from __future__ import print_function

import functools
import logging
import multiprocessing
import time

import schedule

import ps_collector.config
import ps_collector.sharedrabbitmq
from ps_collector.rabbitmquploader import RabbitMQUploader
from ps_collector.mesh import Mesh
import ps_collector

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
    RabbitMQUploader(connect=endpoint, config=cp, log = log).getData()


def query_ps(state, endpoint):
    old_future = state.futures.get(endpoint)
    if old_future:
        if not old_future.ready():
            state.log.info("Prior probe {} is still running; skipping query.".format(endpoint))
            return
        # For now, ignore the result.
        old_future.get()

    result = state.pool.apply_async(query_ps_child, (state.cp, endpoint))
    state.futures[endpoint] = result


def query_ps_mesh(state):
    state.log.info("Querying PS mesh")
    # TODO: get a list of endpoints from the configured mesh config.

    mesh_endpoint = state.cp.get("Mesh", "endpoint")

    mesh = Mesh(state.cp.get("Mesh", "endpoint"))
    endpoints = mesh.get_nodes()
    #endpoints = set(["http://hcc-ps01.unl.edu", "http://hcc-ps02.unl.edu"])

    running_probes = set(state.probes)
    probes_to_stop = running_probes.difference(endpoints)
    probes_to_start = endpoints.difference(running_probes)

    for probe in probes_to_stop:
        state.probes.remote(probe)
        scheduler.clear(probe)
        future = self.futures.get(probe)
        if not future:
            continue
        future.wait()

    default_probe_interval = state.cp.getint("Scheduler", "probe_interval") * MINUTE

    for probe in probes_to_start:
        state.probes.add(probe)
        probe_interval = default_probe_interval
        if state.cp.has_section(probe) and state.cp.has_option("interval"):
            probe_interval = state.cp.get(probe, "interval")
        probe_interval *= MINUTE

        query_ps_job = functools.partial(query_ps, state, probe)
        schedule.every(probe_interval).to(probe_interval + MINUTE).seconds.do(query_ps_job).tag(probe)

    time.sleep(5)


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
        pool_size = cp.get("Scheduler", "pool_size")
    pool = multiprocessing.Pool(pool_size)

    state = SchedulerState(cp, pool, log)

    query_ps_mesh_job = functools.partial(query_ps_mesh, state)

    mesh_interval_s = cp.getint("Scheduler", "mesh_interval") * MINUTE
    log.info("Will update the mesh config every %d seconds.", mesh_interval_s)
    schedule.every(mesh_interval_s).to(mesh_interval_s + MINUTE).seconds.do(query_ps_mesh_job)

    try:
        while True:
            schedule.run_pending()
            time.sleep(1)
    except:
        pool.terminate()
        pool.join()
        raise
