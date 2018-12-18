from __future__ import print_function

import functools
import time

import schedule

import ps_collector.config
import ps_collector.sharedrabbitmq

# The conversion factor from minutes to seconds:
# Temporarily change to 1 to make the query cycles faster when debugging.
MINUTE = 1

# Global shared RabbitMQ connection
shared_rabbitmq = None


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


@with_logging
def query_ps_mesh(cp):
    print("Querying PS mesh")
    # TODO: get a list of endpoints from the configured mesh config.

    mesh_endpoint = cp.get("Mesh", "endpoint")

    endpoints = ["hcc-ps01.unl.edu", "hcc-ps02.unl.edu"]

    time.sleep(10)


def main():
    global shared_rabbitmq
    cp = ps_collector.config.get_config()
    query_ps_mesh_job = functools.partial(query_ps_mesh, cp)

    mesh_interval_s = cp.getint("Scheduler", "mesh_interval") * MINUTE
    schedule.every(mesh_interval_s).to(mesh_interval_s + MINUTE).seconds.do(query_ps_mesh_job)

    # Initialize the shared RabbitMQ
    shared_rabbitmq = ps_collector.sharedrabbitmq.SharedRabbitMQ(cp)

    while True:
        schedule.run_pending()
        time.sleep(1)

