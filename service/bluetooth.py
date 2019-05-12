### service/bluetooth: retrieve values from a bluetooth/BLE device
## HOW IT WORKS: 
## DEPENDENCIES:
# OS: bluetooth, bluez, bluez-tools
# Python: 
## CONFIGURATION:
# required: adapter
# optional: 
## COMMUNICATION:
# INBOUND: 
# - IN: 
#   required: handle, handle_type (value|notification), mac
#   optional: format (number|string)
# OUTBOUND: 

import re

from sdk.module.service import Service

import sdk.utils.command
import sdk.utils.numbers
import sdk.utils.strings

class Bluetooth(Service):
    # What to do when initializing
    def on_init(self):
        # constants
        self.scan_timeout = 10
        self.notification_timeout = 10
        # configuration
        self.config = {}
        # require configuration before starting up
        self.add_configuration_listener(self.fullname, True)
    
    # What to do when running
    def on_start(self):
        pass
    
    # What to do when shutting down
    def on_stop(self):
        pass

    # read a value from the device handle and return its hex
    def get_value(self, device, handle):
        # use char read
        output = sdk.utils.command.run("gatttool -i "+self.config["adapter"]+" -b "+device+" -t random --char-read -a "+handle)
        # clean up the output
        return output.replace("Characteristic value/descriptor: ","")

    # read a value from the device notification handle and return its hex
    def get_notification(self, device, handle):
        # enable notification on the provided handle
        output = sdk.utils.command.run(["gatttool","-i",self.config["adapter"], "-b", device, "-t", "random", "--char-write-req", "-a", handle,"-n", "0100", "--listen"], hell=False, timeout=self.notification_timeout)
        # disable notifications
        sdk.utils.command.run("gatttool -i "+hci+" -b "+device+" -t random --char-write-req -a "+handle+" -n 0000")
        # find all the values
        values = re.findall("value: (.+)\n", output)
        # return the first match
        if len(values) > 0: return values[0]
        return ""
        
    # What to do when receiving a request for this module
    def on_message(self, message):
        if message.command == "IN":
            sensor_id = message.args
            # ensure configuration is valid
            if not self.is_valid_configuration(["handle", "handle_type", "mac"], message.get_data()): return
            handle = message.get("handle")
            handle_type = message.get("handle_type")
            mac = message.get("handle_type")
            format = message.get("format") if message.has("format") else None
            if handle_type not in ["value", "notification"]:
                self.log_error("invalid handle type "+handle_type)
                return
            if format is not None and format not in ["number", "string"]:
                self.log_error("invalid format "+format)
                return
            # if the raw data is cached, take it from there, otherwise request the data and cache it
            cache_key = "/".join([mac, handle])
            if self.cache.find(cache_key): 
                data = self.cache.get(cache_key)
            else:
                # read the raw value 
                if handle_type == "value": data = self.get_value(mac, handle)
                elif handle_type == "notification": data = self.get_notification(mac, handle)
                self.log_debug("polled: "+str(data))
                self.cache.add(cache_key, data)
            # format the hex data into the expected format
            if format is not None:
                if format == "number": data = sdk.utils.numbers.hex2int(data)
                elif format == "string": sdk.utils.strings.hex2string(data)
            message.reply()
            message.set("value", data)
            # send the response back
            self.send(message)

    # What to do when receiving a new/updated configuration for this module    
    def on_configuration(self,message):
        # module's configuration
        if message.args == self.fullname:
            if not self.is_valid_module_configuration(["adapter"], message.get_data()): return False
            self.config = message.get_data()
