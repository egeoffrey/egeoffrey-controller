### service/icloud: retrieve position of a device from Apple icloud service
## HOW IT WORKS: 
## DEPENDENCIES:
# OS: 
# Python: pyicloud
## CONFIGURATION:
# required: 
# optional: 
## COMMUNICATION:
# INBOUND: 
# - IN: 
#   required: username, device_name
#   optional: 
# OUTBOUND: 

import sys  
reload(sys)  
sys.setdefaultencoding('utf8')
import json
from pyicloud import PyiCloudmodule

from sdk.module.service import Service

import sdk.utils.datetimeutils
import sdk.utils.exceptions as exception

# 
# configuration:
#   required:
#   optional:
# request:
#   required: 
#   optional:
class Icloud(Service):
    # What to do when initializing
    def on_init(self):
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
            if not self.is_valid_configuration(["username", "device_name"], message.get_data()): return
            username = message.get("username")
            device_name = message.get("device_name")
            # if the raw data is cached, take it from there, otherwise request the data and cache it
            cache_key = "/".join([username])
            if self.cache.find(cache_key): 
                data = self.cache.get(cache_key)
            else:
                # authenticate against icloud
                try:
                    icloud = PyiCloudmodule(username)
                except Exception,e:
                    self.log_warning("unable to access icloud: "+exception.get(e))
                    return
                # retrieve the devices
                devices = icloud.devices
                locations = {}
                # for each device
                for i, device in enumerate(devices):
                    device = devices[i]
                    # retrieve the location
                    location = device.location()
                    if location is None: continue
                    # save the raw location
                    locations[device["name"]] = location
                data = json.dumps(locations)
                self.cache.add(cache_key, data)
            data = json.loads(data)
            device = {}
            # for each device
            for entry in data:
                # identify the device
                if entry != device_name: continue
                # normalize the data for a map
                date = sdk.utils.datetimeutils.timestamp2date(self.date.timezone(int(data[device_name]["timeStamp"]/1000)))
                device["label"] = str(device_name)
                device["text"] = str("<p><b>"+device_name+":</b></p><p>"+date+" ("+data[device_name]["positionType"]+") </p>")
                device["latitude"] = data[device_name]["latitude"]
                device["longitude"] = data[device_name]["longitude"]
                device["accuracy"] = data[device_name]["horizontalAccuracy"]
                # send the response back
                message.reply()
                message.set("value", device)
                self.send(message)

    # What to do when receiving a new/updated configuration for this module    
    def on_configuration(self,message):
        # we need house timezone
        if message.args == "house":
            if not self.is_valid_module_configuration(["timezone", "units", "language"], message.get_data()): return
            self.date = DateTimeUtils(message.get("timezone"))
