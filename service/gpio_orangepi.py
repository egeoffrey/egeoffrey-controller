### service/gpio_orangepi: read/write from an orangepi GPIO
## HOW IT WORKS: 
## DEPENDENCIES:
# OS: 
# Python: OPI.GPIO
## CONFIGURATION:
# required: mode (board|bcm)
# optional: 
## COMMUNICATION:
# INBOUND: 
# - IN: 
#   required: pin
#   optional: 
# - OUT: 
#   required: pin, value
#   optional: 
# OUTBOUND: 
# - controller/hub IN:
#   required: pin
#   optional: edge_detect (rising|falling|both) , pull_up_down (up|down)

import OPi.GPIO as GPIO
import datetime
import json
import time
import json

from sdk.module.service import Service
from sdk.module.helpers.message import Message

import sdk.utils.exceptions as exception

class Gpio_raspi(Service):
    # What to do when initializing
    def on_init(self):
        # map pin with sensor_id
        self.pins = {}
        # require configuration before starting up
        self.add_configuration_listener(self.fullname, True)
        self.add_configuration_listener("sensors/#")
        
    # What to do when running
    def on_start(self):
		GPIO.setwarnings(False)
		mode = GPIO.BCM if self.config["mode"] == "bcm" else GPIO.BOARD
		GPIO.setmode(mode)
    
    # What to do when shutting down
    def on_stop(self):
        pass
        
    # receive a callback and send a message to the hub
    def event_callback(self, pin):
        if pin not in self.pins: return
        sensor_id = self.pins[pin]
        value = 1 if GPIO.input(pin) else 0
        self.log_debug("GPIO input on pin "+str(pin)+" is now "+str(value))
        message = Message(self)
        message.recipient = "controller/hub"
        message.command = "IN"
        message.args = sensor_id
        message.set("value", value)
        self.send(message)

    # What to do when receiving a request for this module
    def on_message(self, message):
        sensor_id = message.args
        if message.command == "IN":
            # ensure configuration is valid
            if not self.is_valid_configuration(["pin"], message.get_data(), False): return
            pin = message.get("pin")
            # if the raw data is cached, take it from there, otherwise request the data and cache it
            cache_key = "/".join([str(pin)])
            if self.cache.find(cache_key): 
                data = self.cache.get(cache_key)
            else:
                data = GPIO.input(pin)
                self.cache.add(cache_key, data)
            # send the response back
            message.reply()
            message.set("value", data)
            self.send(message)
        elif message.command == "OUT":
            data = int(message.get("value"))
            if data != 0 and data != 1: 
                self.log_error("invalid value: "+str(data))
                return
            self.log_info("setting GPIO pin "+str(pin)+" to "+str(data))
            GPIO.output(pin,data)


    # What to do when receiving a new/updated configuration for this module    
    def on_configuration(self,message):
        # module's configuration
        if message.args == self.fullname:
            if not self.is_valid_module_configuration(["mode"], message.get_data()): return
            self.config = message.get_data()
        # sensors to register
        elif message.args.startswith("sensors/"):
            sensor_id = message.args.replace("sensors/","")
            sensor = message.get_data()
            # a sensor has been deleted
            if message.is_null:
                for pin in self.pins:
                    id = self.pins[pin]
                    if id != sensor_id: continue
                    GPIO.remove_event_detect(pin)
                    del self.pins[pin]
            # a sensor has been added/updated
            else: 
                # filter in only relevant sensors
                if "service" not in sensor or sensor["service"]["name"] != self.name or sensor["service"]["mode"] != "passive": return
                if "edge_detect" not in sensor["service"]["configuration"]: return
                # configuration settings
                configuration = sensor["service"]["configuration"]
                if not self.is_valid_configuration(["pin", "edge_detect"], configuration, False): return
                pin = configuration["pin"]
                edge_detect = configuration["edge_detect"]
                # register the pin
                if pin in self.pins:
                    self.log_error("pin "+pin+" already registered with sensor "+self.pins[pin])
                    return
                self.pins[pin] = sensor_id
                GPIO.setup(pin, GPIO.IN)
                # add callbacks
                if edge_detect == "rising": GPIO.add_event_detect(pin, GPIO.RISING, callback=self.event_callback)
                elif edge_detect == "falling": GPIO.add_event_detect(pin, GPIO.FALLING, callback=self.event_callback)
                elif edge_detect == "both": GPIO.add_event_detect(pin, GPIO.BOTH, callback=self.event_callback)
                else:
                    self.log_error("invalid pull_up_down: "+pull_up_down)
                    return
                self.log_debug("registered sensor "+sensor_id)