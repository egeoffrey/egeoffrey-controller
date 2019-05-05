### service/wunderground: retrieve weather information from Weather Underground
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
#   required:  request ("temperature", "pressure", "condition", "humidity", "wind", "wind_gust", "wind_dir", "forecast_", "record_temperature", "record_temperature_year", "normal_temperature", "rain", "snow"), latitude, longitude
#   optional: 
# OUTBOUND: 

import json
import datetime
import time
 
from sdk.module.service import Service
from sdk.utils.datetimeutils import DateTimeUtils

import sdk.utils.web
import sdk.utils.numbers
import sdk.utils.exceptions as exception

class Wunderground(Service):
    # What to do when initializing
    def on_init(self):
        # constants
        self.url = 'http://api.wunderground.com/api'
        self.forecast_max_entries = 5
        # configuration
        self.config = {}
        # helpers
        self.date = None
        self.units = None
        self.language = None
        # require configuration before starting up
        self.add_configuration_listener("house", True)
        self.add_configuration_listener(self.fullname, True)

    # map between user requests and wunderground requests (a wunderground request can contain multiple measures)
    def get_request_type(self, request):
        if request in ["temperature", "pressure", "condition", "humidity", "wind", "wind_gust", "wind_dir"]: return "conditions"
        elif request.startswith("forecast_"): return "forecast10day"
        elif request in ["record_temperature", "record_temperature_year", "normal_temperature"]: return "almanac"
        elif request in ["rain", "snow"]: return "yesterday"
    
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
            location = "lat="+str(message.get("latitude"))+"&lon="+str(message.get("longitude"))
            if self.get_request_type(request) is None:
                self.log_error("invalid request "+request)
                return
            # if the raw data is cached, take it from there, otherwise request the data and cache it
            cache_key = "/".join([location,self.get_request_type(request)])
            if self.cache.find(cache_key): 
                data = self.cache.get(cache_key)
            else: 
                url = self.url+"/"+self.config["api_key"]+"/"+self.get_request_type(request)+"/lang:"+self.language.upper()+"/q/"+location+".json"
                try:
                    data = sdk.utils.web.get(url)
                except Exception,e: 
                    self.log_error("unable to connect to "+url+": "+exception.get(e))
                    return
                self.cache.add(cache_key,data)
            try: 
                parsed_json = json.loads(data)
            except Exception,e: 
                self.log_error("invalid JSON returned")
                return
            if "response" in parsed_json and "error" in parsed_json["response"]:
                self.log_error(str(parsed_json["response"]["error"]["description"]))
                return
            # reply to the requesting module 
            message.reply()
            # handle the request
            # TODO: units
            if request == "temperature":
                message.set("value", float(parsed_json['current_observation']['temp_c']))
                message.set("timestamp", self.date.timezone(int(parsed_json['current_observation']['observation_epoch'])))
            elif request == "humidity":
                message.set("value", int(parsed_json['current_observation']['relative_humidity'].replace('%','')))
                message.set("timestamp", self.date.timezone(int(parsed_json['current_observation']['observation_epoch'])))
            elif request == "wind":
                message.set("value", float(parsed_json['current_observation']['wind_kph']))
                message.set("timestamp", self.date.timezone(int(parsed_json['current_observation']['observation_epoch'])))
            elif request == "wind_gust":
                message.set("value", float(parsed_json['current_observation']['wind_gust_kph']))
                message.set("timestamp", self.date.timezone(int(parsed_json['current_observation']['observation_epoch'])))
            elif request == "pressure":
                message.set("value", float(parsed_json['current_observation']['pressure_mb']))
                message.set("timestamp", self.date.timezone(int(parsed_json['current_observation']['observation_epoch'])))
            elif request == "condition": 
                message.set("value", parsed_json['current_observation']['icon'])
                message.set("timestamp", self.date.timezone(int(parsed_json['current_observation']['observation_epoch'])))
            elif request == "wind_dir":
                direction = parsed_json['current_observation']['wind_dir']
                if len(direction) > 0 and (direction[0] == "N" or direction[0] == "W" or direction[0] == "S" or direction[0] == "E"): direction = direction[0]
                else: direction = "-"
                message.set("value", direction)
                message.set("timestamp", self.date.timezone(int(parsed_json['current_observation']['observation_epoch'])))
            elif request == "forecast_condition":
                for entry in parsed_json['forecast']['simpleforecast']['forecastday'][:self.forecast_max_entries]:
                    message.clear()
                    message.set("statistics", "day/avg")
                    message.set("value", entry["icon"])
                    message.set("timestamp", self.date.day_start(self.date.timezone(int(entry["date"]["epoch"]))))
                    self.send(message)
                message.clear()
            elif request == "forecast_pop":
                for entry in parsed_json['forecast']['simpleforecast']['forecastday'][:self.forecast_max_entries]:
                    message.clear()
                    message.set("statistics", "day/avg")
                    message.set("value", entry["pop"] if entry["pop"] > 0 else 0)
                    message.set("timestamp", self.date.day_start(self.date.timezone(int(entry["date"]["epoch"]))))
                    self.send(message)
                message.clear()
            elif request == "forecast_rain":
                for entry in parsed_json['forecast']['simpleforecast']['forecastday'][:self.forecast_max_entries]:
                    message.clear()
                    message.set("statistics", "day/avg")
                    message.set("value", entry["qpf_allday"]["mm"] if entry["qpf_allday"]["mm"] > 0 else 0)
                    message.set("timestamp", self.date.day_start(self.date.timezone(int(entry["date"]["epoch"]))))
                    self.send(message)
                message.clear()
            elif request == "forecast_snow":
                for entry in parsed_json['forecast']['simpleforecast']['forecastday'][:self.forecast_max_entries]:
                    message.clear()
                    message.set("statistics", "day/avg")
                    message.set("value", entry["snow_allday"]["cm"]*10 if entry["snow_allday"]["cm"] > 0 else 0)
                    message.set("timestamp", self.date.day_start(self.date.timezone(int(entry["date"]["epoch"]))))
                    self.send(message)
                message.clear()
            elif request == "forecast_temperature":
                for entry in parsed_json['forecast']['simpleforecast']['forecastday'][:self.forecast_max_entries]:
                    message.clear()
                    message.set("statistics", "day/min")
                    message.set("value", entry["low"]["celsius"])
                    message.set("timestamp", self.date.day_start(self.date.timezone(int(entry["date"]["epoch"]))))
                    self.send(message)
                    message.clear()
                    message.set("statistics", "day/max")
                    message.set("value", entry["high"]["celsius"])
                    message.set("timestamp", self.date.day_start(self.date.timezone(int(entry["date"]["epoch"]))))
                    self.send(message)
                message.clear()
            elif request == "record_temperature": 
                message.set("statistics", "day/min")
                message.set("value", int(parsed_json['almanac']['temp_low']['record']['C']))
                message.set("timestamp", self.date.day_start(self.date.now()))
                self.send(message)
                message.clear()
                message.set("statistics", "day/max")
                message.set("value", int(parsed_json['almanac']['temp_high']['record']['C']))
                message.set("timestamp", self.date.day_start(self.date.now()))
            elif request == "record_temperature_year":
                message.set("statistics", "day/min")
                message.set("value", int(parsed_json['almanac']['temp_low']['recordyear']))
                message.set("timestamp", self.date.day_start(self.date.now()))
                self.send(message)
                message.clear()
                message.set("statistics", "day/max")
                message.set("value", int(parsed_json['almanac']['temp_high']['recordyear']))
                message.set("timestamp", self.date.day_start(self.date.now()))
            elif request == "normal_temperature":
                message.set("statistics", "day/min")
                message.set("value", int(parsed_json['almanac']['temp_low']['normal']['C']))
                message.set("timestamp", self.date.day_start(self.date.now()))
                self.send(message)
                message.clear()
                message.set("statistics", "day/max")
                message.set("value", int(parsed_json['almanac']['temp_high']['normal']['C']))
                message.set("timestamp", self.date.day_start(self.date.now()))
            elif request == "rain":
                message.set("statistics", "day/avg")
                message.set("value", float(parsed_json['history']['dailysummary'][0]['precipm']))
                date_dict = parsed_json['history']['dailysummary'][0]['date']
                date = datetime.datetime.strptime(date_dict["mday"]+"-"+date_dict["mon"]+"-"+date_dict["year"],"%d-%m-%Y")
                message.set("timestamp", self.date.timezone(int(time.mktime(date.timetuple()))))
            elif request == "snow":
                message.set("statistics", "day/avg")
                message.set("value", float(parsed_json['history']['dailysummary'][0]['precipm']) if sdk.utils.numbers.is_number(parsed_json['history']['dailysummary'][0]['precipm']) else 0)
                date_dict = parsed_json['history']['dailysummary'][0]['date']
                date = datetime.datetime.strptime(date_dict["mday"]+"-"+date_dict["mon"]+"-"+date_dict["year"],"%d-%m-%Y")
                message.set("timestamp", self.date.timezone(int(time.mktime(date.timetuple()))))
            # send the response back
            self.send(message)

    # What to do when receiving a new/updated configuration for this module
    def on_configuration(self,message):
        # we need house timezone
        if message.args == "house":
            if not self.is_valid_module_configuration(["timezone", "units", "language"], message.get_data()): return
            self.date = DateTimeUtils(message.get("timezone"))
            self.units = message.get("units")
            self.language = message.get("language")
        # module's configuration
        if message.args == self.fullname:
            if not self.is_valid_module_configuration(["api_key"], message.get_data()): return
            self.config = message.get_data()

