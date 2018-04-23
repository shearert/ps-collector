#!/bin/bash
host=$1 start=$2 timeout=$3 metricName=$4
# enabling the python2.7 enviroment with esmond libraries which was installed by the rpm
source /opt/rh/python27/enable
. /var/rsv/localenv/bin/activate
python uploader/caller.py -s $start -u $host -t $timeout -C $metricName
deactivate
