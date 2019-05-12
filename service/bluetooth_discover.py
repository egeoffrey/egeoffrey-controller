### service/bluetooth_discover: discover available bluetooth/BLE devices
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
# OUTBOUND: 

import re

from sdk.module.service import Service

import sdk.utils.command
import sdk.utils.numbers
import sdk.utils.strings

class Bluetooth_discover(Service):
    # What to do when initializing
    def on_init(self):
        # constants
        self.scan_timeout = 10
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
            data = "Scanning for BLE devices...\n"
            scan = sdk.utils.command.run("hcitool -i "+hci+" lescan",timeout=scan_timeout)
            # search for MAC addresses
            devices = set(re.findall("(\w\w:\w\w:\w\w:\w\w:\w\w:\w\w)",scan))
            data = data + "Found "+str(len(devices))+" device(s):\n"
            # for each device
            for device in devices:
                data = data+"\t- Device "+device+":\n"
                # for value handles read the characteristics
                characteristics = sdk.utils.command.run("gatttool -i "+hci+" -b "+device+" -t random --characteristics")
                # filter by char properties (02 and 12 is READ)
                value_handles = re.findall("char properties = 0x(12|02), char value handle = (.+), uuid =", characteristics)
                for value_handle in value_handles:
                    # for each handle
                    handle = value_handle[1]
                    # read the value
                    value = get_value(device,handle)
                    data = data+"\t\t - Value handle "+handle+", value: "+str(value)+", int="+str(sdk.utils.numbers.hex2int(value))+", string="+str(sdk.sdk.utils.strings.hex2string(value)+"\n")
                # for notification handles, find all the handles with 2902 UUID
                notifications = sdk.utils.command.run("gatttool -i "+hci+" -b "+device+" -t random --char-read -u 2902")
                notification_handles = re.findall("handle: (\S+) ", notifications)
                for notification_handle in notification_handles:
                    # for each handle
                    handle = notification_handle
                    # get the value by enabling notifications
                    value = get_notification(device,handle)
                    data = data+"\t\t - Notification handle "+handle+", value: "+str(value)+", int="+str(sdk.utils.numbers.hex2int(value))+", string="+str(sdk.utils.strings.hex2string(value)+"\n")	
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
