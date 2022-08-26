#!/usr/bin/env bash
###
# This script is used to spin up minio server for source/destination buckets
# using input yaml file configuration from CFG_YAML env variable path.
# Usage:
# Spin up for source bucket:
#   ./start-minio.sh src
# Spin up for destination bucket:
#   ./start-minio.sh dest
###

if [[ -z $CFG_YAML ]]; then
    echo "Invalid YAML path. Aborting!"
    exit 0
fi

src_username=`cat $CFG_YAML | grep -i source_token | cut -d':' -f 2 `
src_username=`echo $src_username | sed -e 's/^"//' -e 's/"$//'`

src_password=`cat $CFG_YAML | grep -i source_secret | cut -d':' -f 2 `
src_password=`echo $src_password| sed -e 's/^"//' -e 's/"$//'`

dest_username=`cat $CFG_YAML | grep -i dest_token | cut -d':' -f 2 `
dest_username=`echo $dest_username| sed -e 's/^"//' -e 's/"$//'`

dest_password=`cat $CFG_YAML | grep -i dest_secret | cut -d':' -f 2 `
dest_password=`echo $dest_password | sed -e 's/^"//' -e 's/"$//'`


src_port=`cat $CFG_YAML | grep -i source_s3_endpoint | cut -d':' -f 4 `
src_port=`echo $src_port| sed -e 's/^"//' -e 's/"$//'`

dest_port=`cat $CFG_YAML | grep -i dest_s3_endpoint | cut -d':' -f 4 `
dest_port=`echo $dest_port| sed -e 's/^"//' -e 's/"$//'`

minio_bin=${MINIO_BIN:-~/bin/minio}
echo "Using minio binary from $minio_bin"
if [ "$1" == "src" ]; then
    echo "Spinning up minio server for source bucket at port $src_port"
    MINIO_ROOT_USER=$src_username MINIO_ROOT_PASSWORD=$src_password $minio_bin server /tmp/transfer-eval --address :$src_port

elif [ "$1" == "dest" ]; then
    echo "Spinning up minio server for destination bucket at port $dest_port"
    MINIO_ROOT_USER=$dest_username MINIO_ROOT_PASSWORD=$dest_password $minio_bin server /tmp/transfer-eval --address :$dest_port
else
    echo "Expected argument to ths script: src or dest"
    echo "'start-minio.sh src' starts source server"
    echo "'start-minio.sh dest starts destination server"
fi
