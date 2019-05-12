### service/earthquake: retrieve earthquake information
## HOW IT WORKS: 
## DEPENDENCIES:
# OS: 
# Python: 
## CONFIGURATION:
# required: 
# optional: 
## COMMUNICATION:
# INBOUND: 
# - IN: 
#   required: domain, query
#   optional: 
# OUTBOUND: 

import datetime
import time
import json

from sdk.module.service import Service

import sdk.utils.web
import sdk.utils.datetimeutils

class Earthquake(Service):
    # What to do when initializing
    def on_init(self):
        # constants
        self.limit = 10000
        self.query = "format=text&limit="+str(self.limit)+"&orderby=time-asc"
        # helpers
        self.date = None
        
    # What to do when running
    def on_start(self):
        pass
    
    # What to do when shutting down
    def on_stop(self):
        pass

    # What to do when receiving a request for this module
    def on_message(self, message):
        if message.command == "IN":
            sensor_id = message.args
            # ensure configuration is valid
            if not self.is_valid_configuration(["domain", "query"], message.get_data()): return
            domain = message.get("domain")
            query = message.get("query")
            # if the raw data is cached, take it from there, otherwise request the data and cache it
            cache_key = "/".join([domain])
            if self.cache.find(cache_key): 
                data = self.cache.get(cache_key)
            else:
                # retrieve the data
                try:
                    url = "http://"+domain+"/fdsnws/event/1/query?"+query+"&"+str(query)
                    data = json.dumps(sdk.utils.web.get(url))
                except Exception,e: 
                    self.log_error("unable to connect to "+url+": "+exception.get(e))
                    return
                self.cache.add(cache_key, data)
            message.reply()
            # load the file
            data = json.loads(data)
            # for each line
            for line in data.split('\n'):
                message.clear()
                #EventID|Time|Latitude|Longitude|Depth/Km|Author|Catalog|Contributor|ContributorID|MagType|Magnitude|MagAuthor|EventLocationName
                #    0    1      2          3       4       5     6           7            8           9     10         11           12
                if line.startswith('#'): continue
                # split the entries
                entry = line.split('|')
                if len(entry) != 13: continue
                # set the timestamp to the event's date
                date_format = "%Y-%m-%dT%H:%M:%S.%f"
                date = datetime.datetime.strptime(entry[1], date_format)
                message.set("timestamp", self.date.timezone(self.date.timezone(int(time.mktime(date.timetuple())))))
                # prepare the position value
                position = {}
                position["latitude"] = float(entry[2])
                position["longitude"] = float(entry[3])
                position["label"] = str(entry[10])
                date_string = sdk.utils.datetimeutils.timestamp2date(int(message.get("timestamp")))
                position["text"] = str(entry[12])
                # prepare the measure
                message.set("statistics", "day/avg")
                message.set("value", position)
                # send the response back
                self.send(message)

    # What to do when receiving a new/updated configuration for this module    
    def on_configuration(self,message):
        # we need house timezone
        if message.args == "house":
            if not self.is_valid_module_configuration(["timezone"], message.get_data()): return False
            self.date = DateTimeUtils(message.get("timezone"))
