#/usr/bin/env bash

CONCURRENCY="${RCLONE_CONCURRENCY:-16}"
BUFFER_SIZE="${RCLONE_BSIZE:-512M}"
LOG2FILE="${RCLONE_LOG2FILE:-0}"
LOGDIR="${RCLONE_LOGDIR:-logs/}"
LOGFILE=$(date "+%Y-%m-%d.%H:%M").log
LOGFILE="$LOGDIR/$LOGFILE"
CONFIG="${RCLONE_CFG:-./test_conf.conf}"
SRC="${RCLONE_SRC:-s3-source-1}"
DEST="${RCLONE_DEST:-tmp/}"

mkdir -p $DEST

# code

params="--config=$CONFIG --log-file=$LOGFILE --transfers=$CONCURRENCY --buffer-size=$BUFFER_SIZE"

# list remote
# cmd="rclone -v ls  $SRC:hls-sentinel-validation-scenes"

# copy from source $SRC to current
cmd="rclone -v copy --no-traverse $SRC:hls-sentinel-validation-scenes/ondaDIAS_2015_2016 $DEST"

cmd="$cmd $params"


echo "Logfile -> $LOGFILE"
echo "CFG -> $CONFIG"
echo "Command -> $cmd"

echo "-------" >> $LOGFILE
echo "CONCURRENCY=$CONCURRENCY" >> $LOGFILE
echo "BUFFER_SIZE=$BUFFER_SIZE" >> $LOGFILE
echo "Executing :: $cmd" >>$LOGFILE
echo "-------" >> $LOGFILE 

cat $LOGFILE

echo "======" >> $LOGFILE

eval $cmd

echo "======" >> $LOGFILE

cat $LOGFILE
