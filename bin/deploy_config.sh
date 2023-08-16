#!/bin/sh

if [ -z $1 ] ; then
	echo "usage: bin/deploy_config.sh config_file_path"
	exit -1
fi

config_file=$1

upload_file=`echo $config_file | awk -F'src/main/scala/' '{print $2}'`
upload_path="s3://klaytn-spark-job/config/$upload_file"

aws s3 cp $config_file $upload_path
