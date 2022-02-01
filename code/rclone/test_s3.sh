#/usr/bin/env bash

# globals
CONCURRENCY="${RCLONE_CONCURRENCY:+16}"


# list
# rclone ls  remote-test-s3:hls-sentinel-validation-scenes

# copy from source to target
rclone -v --config="./test_conf.conf" copy --no-traverse s3-source-1:hls-sentinel-validation-scenes/ondaDIAS_2015_2016/ s3-target-1:nishuah-test-bucket --transfers=$CONCURRENCY
