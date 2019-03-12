
import multiprocessing
import time

class timed_execution:

    def __init__(self, endpoint, q):
        self.start_time = 0
        self.q = q
        self.endpoint = endpoint

    def __enter__(self):
        self.start_time = time.time()

    def __exit__(self, type, value, traceback):
        self.q.put([self.endpoint, time.time() - self.start_time])

