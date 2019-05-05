### service/rtl_433: interact with an attached RTL-SDR device
## HOW IT WORKS: 
## DEPENDENCIES:
# OS: raspi-gpio, python-rpi.gpio
# Python: 
## CONFIGURATION:
# required: command
# optional: 
## COMMUNICATION:
# INBOUND: 
# OUTBOUND:
# - controller/hub IN: 
#   required: search, measure
#   optional: 

import datetime
import json
import time
import json
import subprocess
import shlex

from sdk.module.service import Service
from sdk.module.helpers.message import Message

import sdk.utils.datetimeutils
import sdk.utils.command
import sdk.utils.exceptions as exception

class Rtl_433(Service):
    # What to do when initializing
    def on_init(self):
        # constants
        self.command_arguments = "-F json -U"
        # map sensor_id with service's configuration
        self.sensors = {}
        # helpers
        self.date = None
        # require configuration before starting up
        self.add_configuration_listener(self.fullname, True)
        self.add_configuration_listener("sensors/#")
        
    # What to do when running
    def on_start(self):
        # kill rtl_433 if running
        sdk.utils.command.run("killall rtl_433")
        # run rtl_433 and handle the output
        command = self.config['command']+" "+command_arguments
        self.log_debug("running command "+command)
        process = subprocess.Popen(shlex.split(command), stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        prev_output = ""
        while True:
            # read a line from the output
            output = process.stdout.readline()
            if output == '' and process.poll() is not None:
                # process ended, break
                self.log_info("rtl_433 has ended")
                break
            if output:
                # output available
                try:
                    # avoid handling the same exact output, skipping
                    if prev_output == output: continue
                    # parse the json output
                    json_output = json.loads(output)
                except ValueError, e:
                    # not a valid json, ignoring
                    continue
                # for each registered sensor
                for sensor_id in self.sensors:
                    sensor = self.sensors[sensor_id]
                    # check if the output matches the search string
                    search_json = json.loads(sensor["search"])
                    found = True
                    for key, value in search_json.iteritems():
                        # check every key/value pair
                        if key not in json_output: found = False
                        if str(value) != str(json_output[key]): found = False
                    if not found: continue
                    # prepare the message
                    message = Message(self)
                    message.recipient = "controller/hub"
                    message.command = "IN"
                    message.args = sensor_id
                    if "time" in json_output:
                        date = datetime.datetime.strptime(json_output["time"],"%Y-%m-%d %H:%M:%S")
                        message.set("timestamp", self.date.timezone(self.date.timezone(int(time.mktime(date.timetuple())))))
                    value = json_output[sensor["measure"]] if sensor["measure"] in json_output else 1
                    message.set("value", value)
                    # send the measure to the controller
                    self.send(message)
                # keep track of the last line of output
                prev_output = output

    
    # What to do when shutting down
    def on_stop(self):
        pass

    # What to do when receiving a request for this module
    def on_message(self, message):
        pass

    # What to do when receiving a new/updated configuration for this module    
    def on_configuration(self,message):
        # we need house timezone
        if message.args == "house":
            if not self.is_valid_module_configuration(["timezone"], message.get_data()): return
            self.date = DateTimeUtils(message.get("timezone"))
        # module's configuration
        if message.args == self.fullname:
            if not self.is_valid_module_configuration(["command"], message.get_data()): return
            self.config = message.get_data()
        # sensors to register
        elif message.args.startswith("sensors/"):
            sensor_id = message.args.replace("sensors/","")
            sensor = message.get_data()
            # a sensor has been deleted
            if message.is_null:
                if sensor_id in self.sensors: del self.sensors[sensor_id]
            # a sensor has been added/updated
            else: 
                # filter in only relevant sensors
                if "service" not in sensor or sensor["service"]["name"] != self.name or sensor["service"]["mode"] != "passive": return
                configuration = sensor["service"]["configuration"]
                if not self.is_valid_configuration(["measure", "search"], configuration): return
                # keep track of the sensor's configuration
                self.sensors[sensor_id] = configuration
                self.log_info("registered sensor "+sensor_id)