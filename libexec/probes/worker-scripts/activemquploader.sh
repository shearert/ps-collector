#!/bin/bash
host=$1 start=$2 username=$3 key=$4 goc=$5 debug=$6 timeout=$7 summary=$8 allowedEvents=$9 cert=${10} certkey=${11} directoryqueue=${12} tmpdir=${13} allowedMQEvents=${14} maxMQmessageSize=${15}
# enabling the python2.7 enviroment with esmond libraries which was installed by the rpm
#echo "python esmonduploader/caller.py -d $debug -s $start -u $host -p -w $username -k $key -g $goc -t $timeout -x $summary -a $allowedEvents -c $cert -o $certkey --queue $directoryqueue --tmp $tmpdir"
source /opt/rh/python27/enable
. /var/rsv/localenv/bin/activate
python uploader/callerActiveMQ.py -d $debug -s $start -u $host -p -w $username -k $key -g $goc -t $timeout -x $summary -a $allowedEvents -c $cert -o $certkey --queue $directoryqueue --tmp $tmpdir -A $allowedMQEvents -M $maxMQmessageSize
deactivate
