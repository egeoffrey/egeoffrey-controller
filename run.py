#!/usr/bin/python
import time
from sdk.module.watchdog import Watchdog

# run the watchdog module which will load and start all the modules listed in MYHOUSE_MODULES
watchdog = Watchdog()
watchdog.daemon = True
watchdog.start()

# keep running forever
while True:
    time.sleep(1)