#!/bin/sh

LOGS_MOUNT="/logs"

# create the logs directory
mkdir -p $LOGS_MOUNT
# make a symbolic link
rm -rf logs
ln -s $LOGS_MOUNT ./logs
