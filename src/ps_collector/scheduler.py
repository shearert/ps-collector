from __future__ import print_function

import functools
import time
import multiprocessing

import schedule

import ps_collector.config
import ps_collector.sharedrabbitmq

# The conversion factor from minutes to seconds:
# Temporarily change to 1 to make the query cycles faster when debugging.
MINUTE = 1

# Global shared RabbitMQ connection
shared_rabbitmq = None


class SchedulerState(object):

    def __init__(self, cp, pool):
        self.pool = pool
        self.cp = cp
        self.probes = set()
        self.futures = {}


def with_logging(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        print('LOG: Running job "%s"' % func.__name__)
        try:
            result = func(*args, **kwargs)
        finally:
            print('LOG: Job "%s" completed' % func.__name__)
        return result
    return wrapper

def query_ps_child(cp, endpoint):
    print("I query endpoint {}.".format(endpoint))
    time.sleep(5)


@with_logging
def query_ps(state, endpoint):
    old_future = state.futures.get(endpoint)
    if old_future:
        if not old_future.ready():
            print("Prior probe {} is still running; skipping query.".format(endpoint))
            return
        # For now, ignore the result.
        old_future.get()

    result = state.pool.apply_async(query_ps_child, (state.cp, endpoint))
    state.futures[endpoint] = result


@with_logging
def query_ps_mesh(state):
    print("Querying PS mesh")
    # TODO: get a list of endpoints from the configured mesh config.

    mesh_endpoint = state.cp.get("Mesh", "endpoint")

    endpoints = set(["hcc-ps01.unl.edu", "hcc-ps02.unl.edu"])

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
    global shared_rabbitmq
    cp = ps_collector.config.get_config()

    pool_size = 5
    if cp.has_option("Scheduler", "pool_size"):
        pool_size = cp.get("Scheduler", "pool_size")
    pool = multiprocessing.Pool(pool_size)

    state = SchedulerState(cp, pool)

    query_ps_mesh_job = functools.partial(query_ps_mesh, state)

    mesh_interval_s = cp.getint("Scheduler", "mesh_interval") * MINUTE
    schedule.every(mesh_interval_s).to(mesh_interval_s + MINUTE).seconds.do(query_ps_mesh_job)

    # Initialize the shared RabbitMQ
    shared_rabbitmq = ps_collector.sharedrabbitmq.SharedRabbitMQ(cp)

    try:
        while True:
            schedule.run_pending()
            time.sleep(1)
    except:
        pool.terminate()
        pool.join()
        raise

