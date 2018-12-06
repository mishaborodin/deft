#!/bin/sh
# PRODSYS_BASE_DIR=/data/prodsys
# */10 * * * * /bin/nice -n 0 $PRODSYS_BASE_DIR/deftcore/process_requests.sh $PRODSYS_BASE_DIR MC > /tmp/$USER.process_requests.sh.log 2>&1
# vim /etc/security/limits.conf
# atlswing - priority -20

base_path=$1
log_path="/tmp/${USER}.process_requests.log"

cd ${base_path}/deftcore
#/usr/local/bin/python2.7 manage.py runworker -n process_requests -t $2 > ${log_path} 2>&1
/usr/local/bin/python2.7 manage.py debug -t $2 > ${log_path} 2>&1
