from time import sleep
from time import strftime
from time import localtime
import sys
import signal
import os
from optparse import OptionParser

try:
  from activemquploader import *
  from esmonduploader import *
  from rabbitmquploader import *
except Exception as err:
  print "ERROR:! Importing libraries! Exception: \"%s\" of type: \"%s\" was thrown! Quitting out." % (err,type(err)) 
  sys.exit(1)

# Set command line options                                                                                                                                     
parser = OptionParser()
parser.add_option('-s', '--start', help='set start time for gathering data (default is -12 hours)', dest='start', default=960)
parser.add_option('-u', '--url', help='set url to gather data from (default is http://hcc-pki-ps02.unl.edu)', dest='url', default='http://hcc-pki-ps02.unl.edu')
parser.add_option('-t', '--timeout', help='the maxtimeout that the probe is allowed to run in secs', dest='timeout', default=1000, action='store')
# Name of the metric                                                                                                                                      
parser.add_option('-C','--metric', help='Metric name', default='org.osg.general-perfsonar-simple', dest='metricName', action='store')
(opts, args) = parser.parse_args()

caller = None
metricName = opts.metricName
if metricName == 'org.osg.general.perfsonar-activemq-simple':
  caller = ActiveMQUploader(start=int(opts.start), connect=opts.url, metricName=metricName)
elif metricName == 'org.osg.general.perfsonar-simple':
  caller = EsmondUploader(start=int(opts.start), connect=opts.url, metricName=metricName)
elif metricName == 'org.osg.general.perfsonar-rabbitmq-simple':
  caller = RabbitMQUploader(start=int(opts.start), connect=opts.url, metricName=metricName)
else:
  print "ERROR: metric not supporred"
  

def get_post():
    try:
        caller.getData()
    except Exception, err:
        print Exception, err
        print "ERROR:! Unsuccessful! Exception: \"%s\" of type: \"%s\" was thrown! Quitting out." % (err,type(err))
        sys.exit(1)
    else:
        caller.add2log("Finished getting and posting data succesfully!")
        sys.exit(0)


def handler(signum, frame):
    caller.add2log('WARNING: Running time took more than %d seconds' % int(opts.timeout))
    sys.exit(0)

# Option: Get and Post Metadata
if True:
   # Implementing some timeout
    signal.signal(signal.SIGALRM, handler)
    signal.alarm(int(opts.timeout))
    get_post()
    signal.alarm(0)

