"""
A shared manager for the shared list of MAs that are being pushed.
This manager is written by the push parser, and read by the mesh config.
"""
from multiprocessing import Manager

push_manager = Manager()
pushlist = push_manager.list()
