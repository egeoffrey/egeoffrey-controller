#!/bin/sh

CONFIG_MOUNT="/config"

# create the config directory
mkdir -p $CONFIG_MOUNT
# if the directory is empty copy there the default configuration
if [ ! "$(ls -A $CONFIG_MOUNT)" ]; then
    echo -e "[\e[33mmyHouse\e[0m] Copying default configuration into $CONFIG_MOUNT..."
    cp -Rf config/* $CONFIG_MOUNT
fi
# make a symbolic link so to use the user's configuration
rm -rf config
ln -s $CONFIG_MOUNT $CONFIG
