from time import sleep
from time import strftime
from time import localtime
import sys
import signal
import os

try:
  from esmonduploader import *
except Exception as err:
  print "ERROR:! Importing esmond libraries! Exception: \"%s\" of type: \"%s\" was thrown! Quitting out." % (err,type(err)) 
  sys.exit(1)

### File that would call EsmondUploader() with specified parameters to get and post the data ###
caller = EsmondUploader(verbose=False,start=int(opts.start),end=int(opts.end),
                        connect=opts.url, username=opts.username, key=opts.key, 
                        goc=opts.goc, allowedEvents=opts.allowedEvents,
                        cert=opts.cert, certkey=opts.certkey, dq=opts.dq)

def str2bool(word):
  return word.lower() in ("true")

def get_post():
    try:
        caller.getData(str2bool(opts.disp), str2bool(opts.summary))
    except Exception as err:
        print "ERROR:! Unsuccessful! Exception: \"%s\" of type: \"%s\" was thrown! Quitting out." % (err,type(err))
        sys.exit(1)
    else:
        caller.add2log("Finished getting and posting data succesfully!")
        sys.exit(0)


def handler(signum, frame):
    caller.add2log('WARNING: Running time took more than %d seconds' % int(opts.timeout))
    sys.exit(0)

# Option: Get and Post Metadata
if opts.post:
   # Implementing some timeout
    signal.signal(signal.SIGALRM, handler)
    signal.alarm(int(opts.timeout))
    get_post()
    signal.alarm(0)

