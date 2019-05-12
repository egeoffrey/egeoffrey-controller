### service/messagebridge: interact with Ciseco/WirelessThings devices
## HOW IT WORKS: 
## DEPENDENCIES:
# OS: 
# Python: 
## CONFIGURATION:
# required: port_listen, port_send
# optional: 
## COMMUNICATION:
# INBOUND: 
# - IN: 
#   required: node_id, measure
#   optional: cycle_sleep_min
# - OUT: 
#   required: node_id, measure
#   optional: cycle_sleep_min
# OUTBOUND: 
# - controller/hub IN: 
#   required: node_id, measure
#   optional: cycle_sleep_min

import datetime
import json
import sys
import time
import os
import socket
import json

from sdk.module.service import Service
from sdk.module.helpers.message import Message

import sdk.utils.exceptions as exception

class Messagebridge(Service):
    # What to do when initializing
    def on_init(self):
        # configuration
        self.config = {}
        # map sensor_id with service configuration
        self.sensors = {}
        # queue messages when the sensor is sleeping
        self.queue = {}
        # helpers
        self.date = None
        # require configuration before starting up
        self.add_configuration_listener(self.fullname, True)
        
    # initialize a sensor when just started or when in an unknown status
    def init(self, sensor):
        # turn all the output off
        self.tx(sensor["node_id"], ["OUTA0", "OUTB0", "OUTC0", "OUTD0"], True)
        # put it to sleep
        self.sleep(sensor)
        
    # put a sensor to sleep
    def sleep(self, sensor):
        sleep_min = sensor["cycle_sleep_min"]*60
        time.sleep(1)
        self.tx(sensor, "SLEEP"+str(sleep_min).zfill(3)+"S", False)
        
    # transmit a message to a sensor
    def tx(self, sensor, data, service_message=False):
        if not module_message: log_info("sending message to "+sensor["node_id"]+": "+str(data))
        # create a socket
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        # prepare the message
        message = {'type':"WirelessMessage",'network':"Serial"}
        message["id"] = sensor["node_id"]
        message["data"] = data if isinstance(data,list) else [data]
        json_message = json.dumps(message)
        # send the message
        self.log_debug("sending message: "+json_message)
        sock.sendto(json_message, ('<broadcast>', self.config["port_send"]))
        sock.close()
        
    # What to do when running
    def on_start(self):
        # request all sensors' configuration so to filter sensors of interest
        self.add_configuration_listener("sensors/#")
        self.log_debug("listening for UDP datagrams on port "+str(self.config["port_listen"]))
        # bind to the network
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        sock.bind(("", self.config["port_listen"]))
        while True:
            try:
                # new data arrives	
                data, addr = sock.recvfrom(1024)
                self.log_debug("received "+data)
                # the message is expected to be in JSON format
                data = json.loads(data)
                if data["type"] != "WirelessMessage": continue
                # check if this node is associated to a registered sensor
                for sensor_id in self.sensor:
                    if data["id"] == self.sensor[sensor_id]["node_id"]:
                        sensor = self.sensor[sensor_id]
                        break
                # not registered sensor, skip it
                if sensor is None: continue
                # for each message
                for content in data["data"]:
                    if content == "STARTED":
                        self.log_info(data["id"]+" has just started")
                        # ACK a started message
                        self.tx(sensor, "ACK", True)
                        # initialize
                        self.init(sensor)
                    elif content == "AWAKE":
                        # send a message if there is something in the queue
                        if data["id"] in self.queue and len(self.queue[data["id"]]) > 0:
                            self.tx(sensor, queue[data["id"]])
                            self.queue[data["id"]] = []
                        # put it to sleep again
                        self.sleep(sensor)
                    else:
                        for sensor_id in self.sensor:
                            if data["id"] == self.sensor[sensor_id]["node_id"] and content.startswith(self.sensor[sensor_id]["measure"]):
                                sensor = self.sensor[sensor_id]
                                break
                        # not registered measure, skip it
                        if sensor is None: continue
                        # prepare the message
                        message = Message(self)
                        message.recipient = "controller/hub"
                        message.command = "IN"
                        message.args = sensor_id
                        # generate the timestamp
                        date = datetime.datetime.strptime(data["timestamp"],"%d %b %Y %H:%M:%S +0000")
                        measure.set("timestamp", self.date.timezone(self.date.timezone(int(time.mktime(date.timetuple())))))
                        # strip out the measure from the value
                        message.set("value", content.replace(self.sensor[sensor_id]["measure"],""))
                        # send the measure to the controller
                        self.send(message)
            except Exception,e:
                self.log_warning("unable to parse "+str(data)+": "+exception.get(e))
            
    # What to do when shutting down
    def on_stop(self):
        pass
        
    # What to do when receiving a request for this module
    def on_message(self, message):
        sensor_id = message.args
        if message.command == "OUT":
            sensor = message.get_data()
            data = message.get("value")
            if not self.is_valid_configuration(["node_id", "measure"], sensor): return
            if "cycle_sleep_min" not in sensor:
                # send the message directly
                self.tx(sensor, data)
            else:
                # may be sleeping, queue it
                self.log_info("queuing message for "+sensor["node_id"]+": "+data)
                if node_id not in queue: queue[node_id] = []
                queue[node_id] = [data]

    # What to do when receiving a new/updated configuration for this module
    def on_configuration(self,message):
        # we need house timezone
        if message.args == "house":
            if not self.is_valid_module_configuration(["timezone"], message.get_data()): return False
            self.date = DateTimeUtils(message.get("timezone"))
        # module's configuration
        if message.args == self.fullname:
            if not self.is_valid_module_configuration(["port_listen", "port_send"], message.get_data()): return False
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
                if not self.is_valid_configuration(["node_id", "measure"], configuration): return
                # keep track of the sensor's configuration
                self.sensors[sensor_id] = configuration
                self.log_info("registered sensor "+sensor_id)