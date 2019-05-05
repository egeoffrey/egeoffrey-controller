### service/rss: parse a rss feed
## HOW IT WORKS: 
## DEPENDENCIES:
# OS: python-feedparser
# Python: 
## CONFIGURATION:
# required: 
# optional: 
## COMMUNICATION:
# INBOUND: 
# - IN: 
#   required: url
#   optional: 
# OUTBOUND: 

import json
import feedparser

from sdk.module.service import Service

import sdk.utils.web

class Rss(Service):
    # What to do when initializing
    def on_init(self):
        pass
        
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
            if not self.is_valid_configuration(["url"], message.get_data()): return
            url = message.get("url")
            # if the raw data is cached, take it from there, otherwise request the data and cache it
            cache_key = "/".join([url])
            if self.cache.find(cache_key): 
                data = self.cache.get(cache_key)
            else:
                try:
                    data = json.dumps(sdk.utils.web.get(url))
                except Exception,e: 
                    self.log_error("unable to connect to "+csv_file+": "+exception.get(e))
                    return
                self.cache.add(cache_key, data)
            message.reply()
            # load the file
            data = json.loads(data)
            # parse the feed
            feed = feedparser.parse(data)
            result = ""
            for i in range(len(feed["entries"])):
                entry = feed["entries"][i]
                # return a single string containing all the entries
                result = result + entry["title"].encode('ascii','ignore')+"\n"
            message.set("value", result)
            # send the response back
            self.send(message)

    # What to do when receiving a new/updated configuration for this module    
    def on_configuration(self,message):
        pass
