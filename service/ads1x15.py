### service/ads1x15: retrieve values from a ads1x15 analog to digital converter
## HOW IT WORKS: 
## DEPENDENCIES:
# OS: 
# Python: Adafruit-ADS1x15
## CONFIGURATION:
# required: 
# optional: 
## COMMUNICATION:
# INBOUND: 
# - IN: 
#   required: channel, type (ads1115|ads1015), address, gain (2/3|1|2|4|8|16), output (volt|raw|integer|percentage)
#   optional: 
# OUTBOUND: 

import Adafruit_ADS1x15

from sdk.module.service import Service

class Ads1x15(Service):
    # What to do when initializing
    def on_init(self):
        # define the maxium voltage for each gain
        self.gain_ratio = {
            "2/3": 6.144,
            "1": 4.096,
            "2": 2.048,
            "4": 1.024,
            "8": 0.512,
            "16": 0.256,
        }
    
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
            if not self.is_valid_configuration(["channel", "type", "address", "gain", "output"], message.get_data()): return
            channel = message.get("channel")
            type = message.get("type")
            address = message.get("address")
            gain = message.get("gain")
            output = message.get("output")
            self.log_debug("Reading channel "+str(channel)+" from "+type+"("+str(address)+") with gain "+gain+" ("+str(self.gain_ratio[gain])+"V) output "+output)
            # convert the address in hex
            address = int(address[2:], 16)
            # select the device
            if type == "ads1115": adc = Adafruit_ADS1x15.ADS1115(address=address)
            elif type == "ads1015": adc = Adafruit_ADS1x15.ADS1015(address=address)
            else: 
                self.log_error("invalid type "+type)
                return
            # if the raw data is cached, take it from there, otherwise request the data and cache it
            cache_key = "/".join([address, str(channel)])
            if self.cache.find(cache_key): 
                data = self.cache.get(cache_key)
            else:
                # read the value
                data = adc.read_adc(channel, gain=int(gain))
                self.cache.add(cache_key, data)
            max = 1
            # ads1115 is 16bit, ads1015 12 bit
            if type == "ads1115": max = 32768
            elif type == "ads1015": max = 2048
            # normalize the value
            value = float(data)
            # calculate the voltage based on the maximum voltage from the gain set
            volt = value*self.gain_ratio[gain]/max
            # return an arduino like value between 0 and 1024
            integer = int(volt*1024/self.gain_ratio[gain])
            # return a percentage based on the maximum value it can assume from the gain set
            percentage = int(volt*100/self.gain_ratio[gain])
            self.log_debug("Parsed "+str(value)+" -> "+str(volt)+"V -> "+str(integer)+"/1024 -> "+str(percentage)+"%")
            # return the output
            if output == "volt": value = volt
            elif output == "raw": value = value
            elif output == "integer": value = integer
            elif output == "percentage": value =  percentage
            message.reply()
            message.set("value", value)
            # send the response back
            self.send(message)

    # What to do when receiving a new/updated configuration for this module    
    def on_configuration(self,message):
        pass