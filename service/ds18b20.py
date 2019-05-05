### service/ds18b20: retrieve temperature from a ds18b20 sensor
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
#   required: device
#   optional: 
# OUTBOUND: 

import os

from sdk.module.service import Service

import sdk.utils.exceptions as exception

class Ds18b20(Service):
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
            if not self.is_valid_configuration(["device"], message.get_data()): return
            device = message.get("device")
            # if the raw data is cached, take it from there, otherwise request the data and cache it
            cache_key = "/".join([device])
            if self.cache.find(cache_key): 
                data = self.cache.get(cache_key)
            else:
                # ensure the device exists
                filename = "/sys/bus/w1/devices/"+device+"/w1_slave"
                if not os.path.isfile(filename):
                    self.log_error(filename+" does not exist")
                    return
                # read the file
                self.log_debug("Reading temperature from "+filename)
                with open(filename, 'r') as file:
                    data = file.read()
                file.close()
                self.cache.add(cache_key, data)
            # find t=
            pos = data.find('t=')
            if pos != -1:
                temp_string = data[pos+2:]
                temp = float(temp_string) / 1000.0
                # send the response back
                message.reply()
                message.set("value", temp)
                self.send(message)

    # What to do when receiving a new/updated configuration for this module    
    def on_configuration(self,message):
        pass