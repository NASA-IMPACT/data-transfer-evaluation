#!/usr/bin/env bash

###
# This script is used to auto-create source/desitnation buckets with dummies
###

echo "setting up test buckets"
BASE_PATH="/tmp/transfer-eval"
SRC="src"
DEST="dest"

src_path="$BASE_PATH/$SRC"
dest_path="$BASE_PATH/$DEST"
echo "Creating destination bucket at $src_path"
mkdir -p $src_path

echo "Creating destination bucket at $src_path"
mkdir -p $src_path
mkdir -p $dest_path

echo "Creating dummy files at $src_path"

# change N as per need
for i in {1..5}
do
    outfile="$src_path/dummy.$i"
    echo "Creating $outfile"
    dd if=/dev/zero of=$outfile bs=1024 count=1000000
done
