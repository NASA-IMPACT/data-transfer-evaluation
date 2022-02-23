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
BUCKET="${RCLONE_BUCKET:-tmp/}"
MULTI_THREAD_CUTOFF="${RCLONE_MTC:-100M}"
MULTI_THREAD_STREAMS="${RCLONE_MTS:-5}"

mkdir -p $DEST

# code

params="--config=$CONFIG --log-file=$LOGFILE --transfers=$CONCURRENCY --buffer-size=$BUFFER_SIZE --order-by size,desc  --multi-thread-cutoff=$MULTI_THREAD_CUTOFF --multi-thread-streams=$MULTI_THREAD_STREAMS  --progress"

# list remote
# cmd="rclone -v ls  $SRC:hls-sentinel-validation-scenes"

# copy from source $SRC to current
# cmd="rclone -v copy --no-traverse $SRC:$BUCKET $DEST"
cmd="rclone -v copy  $SRC:$BUCKET $DEST"

cmd="$cmd $params"


echo "Logfile -> $LOGFILE"
echo "CFG -> $CONFIG"
echo "Command -> $cmd"

echo "-------" >> $LOGFILE
echo "CONCURRENCY=$CONCURRENCY" >> $LOGFILE
echo "BUFFER_SIZE=$BUFFER_SIZE" >> $LOGFILE
echo "MULTI_THREAD_CUTOFF=$MULTI_THREAD_CUTOFF" >> $LOGFILE
echo "MULTI_THREAD_STREAMS=$MULTI_THREAD_STREAMS" >> $LOGFILE
echo "Executing :: $cmd" >>$LOGFILE
echo "-------" >> $LOGFILE 

cat $LOGFILE

echo "======" >> $LOGFILE

{
eval $cmd
}  | tee -a $LOGFILE

echo "======" >> $LOGFILE

cat $LOGFILE
