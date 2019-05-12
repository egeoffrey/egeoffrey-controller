### service/weatherchannel: retrieve weather information from weatherchannel
## HOW IT WORKS: 
## DEPENDENCIES:
# OS: 
# Python: 
## CONFIGURATION:
# required: api_key
# optional: 
## COMMUNICATION:
# INBOUND: 
# - IN: 
#   required:  request (alerts), latitude, longitude
#   optional: 
# OUTBOUND: 

import json
 
from sdk.module.service import Service
from sdk.utils.datetimeutils import DateTimeUtils

import sdk.utils.web
import sdk.utils.numbers
import sdk.utils.exceptions as exception

class Weatherchannel(Service):
   # What to do when initializing
    def on_init(self):
        # constants
        self.url = 'https://api.weather.com/v1/geocode/'
        # configuration file
        self.config = {}
        # helpers
        self.date = None
        self.units = None
        self.language = None
        # require configuration before starting up
        self.add_configuration_listener("house", True)
        self.add_configuration_listener(self.fullname, True)

    # map between user requests and openweathermap requests
    def get_request_type(self,request):
        if request in ["alerts"]: return "forecast"
        return None
    
    # What to do when running    
    def on_start(self):
        pass
    
    # What to do when shutting down
    def on_stop(self):
        pass

    # What to do when receiving a request for this module
    def on_message(self, message):
        if message.command == "IN":
            if not self.is_valid_configuration(["request", "latitude", "longitude"], message.get_data()): return
            sensor_id = message.args
            request = message.get("request")
            location = str(message.get("latitude"))+","+str(message.get("longitude"))
            if self.get_request_type(request) is None:
                self.log_error("invalid request "+request)
                return
            # if the raw data is cached, take it from there, otherwise request the data and cache it
            cache_key = "/".join([location, self.get_request_type(request)])
            if self.cache.find(cache_key): 
                data = self.cache.get(cache_key)
            else:
                url = self.url+location+'/'+self.get_request_type(request)+'/wwir.json?apiKey='+sekf.config['api_key']+'&units='+unit+'&language='+self.language
                try:
                    data = sdk.utils.web.get(url)
                except Exception,e: 
                    self.log_error("unable to connect to "+url+": "+exception.get(e))
                    return
                self.cache.add(cache_key,data)
            # parse the raw data
            try: 
                parsed_json = json.loads(data)
            except Exception,e: 
                self.log_error("invalid JSON returned")
                return
            # reply to the requesting module 
            message.reply()
            # handle the request
            if request == "alerts":
                alert = ""
                if isinstance(parsed_json["forecast"]["precip_time_24hr"], basestring): alert = parsed_json["forecast"]["phrase"]
                message.set("value", alert)
            # send the response back
            self.send(message)

    # What to do when receiving a new/updated configuration for this module
    def on_configuration(self,message):
        # we need house timezone
        if message.args == "house":
            if not self.is_valid_module_configuration(["timezone", "units", "language"], message.get_data()): return False
            self.date = DateTimeUtils(message.get("timezone"))
            self.units = message.get("units")
            self.language = message.get("language")
        # module's configuration
        if message.args == self.fullname:
            if not self.is_valid_module_configuration(["api_key"], message.get_data()): return False
            self.config = message.get_data()
