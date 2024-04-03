#!/bin/sh

SCRIPT_PATH=${BASH_SOURCE[0]}
PROJECT_ROOT=$(cd $(dirname $(readlink ${SCRIPT_PATH} || echo ${SCRIPT_PATH}))/;/bin/pwd)

FILE_LIST=$PROJECT_ROOT/file_list
JOB_LIST=$PROJECT_ROOT/job_list

hadoop fs -ls /var/log/spark/apps/ | grep "application" > $FILE_LIST
yarn app -list | awk '{print $1}' | grep "application" > $JOB_LIST

for job in `cat $JOB_LIST`
do
        cat $FILE_LIST | grep -v $job > $PROJECT_ROOT/temp
        mv $PROJECT_ROOT/temp $FILE_LIST
done

for file in `cat $FILE_LIST | awk '{print $NF}'`
do
        hadoop fs -rm -r -skipTrash $file
done

rm $FILE_LIST $JOB_LIST