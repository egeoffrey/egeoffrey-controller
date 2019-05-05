### service/dht: retrieve temperature/humidity from a DHT11/DHT22 sensor
## HOW IT WORKS: 
## DEPENDENCIES:
# OS: 
# Python: Adafruit-Python-DHT
## CONFIGURATION:
# required: 
# optional: 
## COMMUNICATION:
# INBOUND: 
# - IN: 
#   required: measure, type (dht11|dht22), pin
#   optional: 
# OUTBOUND: 

import Adafruit_DHT

from sdk.module.service import Service

import sdk.utils.exceptions as exception

class Dht(Service):
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
            if not self.is_valid_configuration(["measure", "type", "pin"], message.get_data()): return
            measure = message.get("measure")
            type = message.get("type")
            pin = message.get("pin")
            # select the device
            if type == "dht11": dht_sensor = Adafruit_DHT.DHT11
            elif type == "dht22": ht_sensor = Adafruit_DHT.DHT22
            else: 
                self.log_error("invalid type "+type)
                return
            # if the raw data is cached, take it from there, otherwise request the data and cache it
            cache_key = "/".join([type, str(pin)])
            if self.cache.find(cache_key): 
                data = self.cache.get(cache_key)
            else:
                try: 
                    humidity, temperature = Adafruit_DHT.read_retry(dht_sensor, pin)
                except Exception,e: 
                    self.log_error("unable to connect to the sensor: "+exception.get(e))
                    return
                if humidity is not None and temperature is not None and humidity <= 100:
                    # if this is a valid measure, return both the measures
                    data = str(temperature)+"|"+str(humidity)
                    self.cache.add(cache_key,data)
            split = data.split("|")
            if measure == "temperature": data = split[0]
            if measure == "humidity": data = split[1]
            # send the response back
            message.reply()
            message.set("value", data)
            self.send(message)

    # What to do when receiving a new/updated configuration for this module    
    def on_configuration(self,message):
        pass