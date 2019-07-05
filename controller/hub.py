### controller/hub: request module executions and receive measures from sensors
## HOW IT WORKS: 
## DEPENDENCIES:
# OS: 
# Python: 
## CONFIGURATION:
# required: poll_at_startup, calculate, retain
# optional: 
## COMMUNICATION:
# INBOUND: 
# - IN: new value coming from a sensor (soliceted or unsolicited)
# - POLL: requested to invoke the service associated to the sensor
# - SET: asked to set a new value to a sensor
# OUTBOUND: 
# - service/* IN: invoke the service associated to the sensor
# - service/* OUT: trigger an action to an actuator
# - controller/db CALC_HOUR_STATS/CALC_DAY_STATS: periodically calculate aggregates
# - controller/db PURGE_SENSOR: periodically purge old data
# - controller/db SAVE: save new measures

import sys  
reload(sys)  
sys.setdefaultencoding('utf8')

from sdk.python.module.controller import Controller
from sdk.python.module.helpers.scheduler import Scheduler
from sdk.python.module.helpers.message import Message
from sdk.python.utils.datetimeutils import DateTimeUtils

import sdk.python.utils.command
import sdk.python.utils.numbers
import sdk.python.utils.strings
import sdk.python.utils.exceptions as exception

class Hub(Controller):
    # What to do when initializing    
    def on_init(self):
        # module's configuration
        self.config = {}
        # map sensor_id with sensor configuration
        self.sensors = {}
        # map sensor_id with scheduler job_id
        self.jobs = {}
        # scheduler is needed for polling sensors
        self.scheduler = Scheduler(self)
        # date/time helper
        self.date = None
        # request required configuration files
        self.add_configuration_listener("controller/hub", True)
        self.add_configuration_listener("house", True)
        # subscribe for acknowledgments from the database for saved values
        self.add_inspection_listener("controller/db", "*/*", "SAVED", "#")

   
    # what to do during execution of the sensor's schedule
    def run_sensor(self, sensor_id):
        # prepare and send a message to the requested module
        # TODO: provide a function for this and check for required arguments
        # TODO: escape "/"
        sensor = self.sensors[sensor_id]
        if "service" in sensor and sensor["service"]["mode"] == "active":
            message = Message(self)
            message.recipient = "service/"+sensor["service"]["name"]
            message.command = "IN"
            message.args = sensor_id
            message.set_data(sensor["service"]["configuration"])
            self.log_debug("["+sensor_id+"] requesting "+message.recipient+" for "+str(message.get_data()))
            self.send(message)
     
    # calculate hourly/daily aggregated statistics for all the sensors
    def calculate_stats(self, group_by):
        self.log_debug("Requesting database to calculate "+group_by+" statistics")
        for sensor_id in self.sensors:
            sensor = self.sensors[sensor_id]
            # if we need to calculate aggregated statistics for this sensor
            if "calculate" in sensor and sensor["calculate"] in self.config["calculate"]:
                # ask the database module to calculate the aggregated statistics
                message = Message(self)
                message.recipient = "controller/db"
                message.command = "CALC_"+group_by.upper()+"_STATS"
                message.args = sensor_id
                message.set_data(self.config["calculate"][sensor["calculate"]]["operations"])
                self.send(message)

    # apply configured retention policies for all the sensors
    def retention_policies(self):
        for sensor_id in self.sensors:
            sensor = self.sensors[sensor_id]
            # if we need to apply retention policies for this sensor
            if "retain" in sensor and sensor["retain"] in self.config["retain"]:
                # ask the database module to purge the data
                message = Message(self)
                message.recipient = "controller/db"
                message.command = "PURGE_SENSOR"
                message.args = sensor_id
                message.set_data(self.config["retain"][sensor["retain"]])
                self.send(message)

    # schedule a given sensor for execution
    def add_sensor(self, sensor_id, sensor):
        self.log_debug("Received configuration for sensor "+sensor_id)
        # clean it up first
        self.remove_sensor(sensor_id)
        self.sensors[sensor_id] = sensor
        if "service" in sensor and "schedule" in sensor["service"]:
            # schedule it
            self.log_debug("Scheduling "+sensor_id+" with the following settings: "+str(sensor["service"]["schedule"]))
            # "schedule" contains apscheduler settings for this sensor
            job = sensor["service"]["schedule"]
            # add function to call and args
            job["func"] = self.run_sensor
            job["args"] = [sensor_id]
            # schedule the job for execution
            self.jobs[sensor_id] = self.scheduler.add_job(job).id
            # also run it now if requested
            # TODO: keep poll_at_startup?
            if self.config["poll_at_startup"]:
                self.run_sensor(sensor_id)
                
    # delete a sensor
    def remove_sensor(self, sensor_id):
        if sensor_id in self.sensors: 
            self.log_debug("Removing sensor "+sensor_id)
            del self.sensors[sensor_id]
            # if already scheduled, stop it
            if sensor_id in self.jobs:
                self.scheduler.remove_job(self.jobs[sensor_id])
        
    # What to do when running
    def on_start(self):
        # 1) ask for all sensor's configuration
        self.add_configuration_listener("sensors/#")
        # 2) schedule statistics calculation
        # TODO: what if different houses live in different timezones
        # every hour (just after the top of the hour) calculate for each sensor statistics of the previous hour
        job = {"func": self.calculate_stats, "trigger":"cron", "minute": 0, "second": sdk.python.utils.numbers.randint(1,59), "args": ["hour"]}
        self.scheduler.add_job(job)
        # every day (just after midnight) calculate for each sensor statistics of the previous day (using hourly averages)
        job = {"func": self.calculate_stats, "trigger":"cron", "hour": 0, "minute": 0, "second": sdk.python.utils.numbers.randint(1,59), "args": ["day"]}
        self.scheduler.add_job(job)
        # 3) schedule to apply configured retention policies (every day just after 1am)
        job = {"func": self.retention_policies, "trigger":"cron", "hour": 1, "minute": 0, "second": sdk.python.utils.numbers.randint(1,59)}
        # 4) start the scheduler 
        self.scheduler.start()
        # TODO: should be able to detect DST change and reschedule all the jobs
        
    # What to do when shutting down
    def on_stop(self):
        # stop the scheduler
        self.scheduler.stop()

    # save the value assigned to sensor_id in the database
    def save_value(self, sensor_id, message):
        # 1) normalize the data structure and add current timestamp if not provided by the sensor
        if not message.has("value"):
            # value is missing, assume the value is in the data
            value = message.get_data()
            message.set("value", value)
        if not message.has("timestamp"):
            message.set("timestamp", self.date.now())
        # 2) post-process the value if requested
        sensor = self.sensors[sensor_id]
        if "post_processor" in sensor and sensor["post_processor"] in self.config["post_processors"]:
            try:
                orig_value = message.get("value")
                command = self.config["post_processors"][sensor["post_processor"]].replace("%value%",str(orig_value))
                message.set("value", sdk.python.utils.command.run(command))
                self.log_debug("["+sensor_id+"] transforming "+str(orig_value)+" into "+str(message.get("value")))
            except Exception,e: 
                self.log_error("["+sensor_id+"] Unable to post-process "+str(orig_value)+" by running "+str(command)+": "+exception.get(e))
        # 3) normalize the value according to its format
        try:
            # TODO: format does not exist anymore
            message.set("value", sdk.python.utils.numbers.normalize(message.get("value"), sensor["format"]))
        except Exception,e: 
            # TODO: make exception part of the class
            self.log_error("["+sensor_id+"] Unable to normalize "+str(message.get("value"))+": "+exception.get(e))
        #TODO: ifnotexists?
        # 4) attach retention policies to be applied straight away
        if "retain" in sensor and sensor["retain"] in self.config["retain"]:
            message.set("retain", self.config["retain"][sensor["retain"]])
        # 5) ask the db to store the value
        message.forward("controller/db")
        message.command = "SAVE"
        message.args = sensor_id
        # 6) request to update hourly/daily stats
        if "calculate" in sensor and sensor["calculate"] in self.config["calculate"]:
            message.set("calculate", self.config["calculate"][sensor["calculate"]]["operations"])
        self.log_debug("["+sensor_id+"] requesting database to store: "+str(message.get_data()))
        self.send(message)

    # What to do when receiving a request for this module    
    def on_message(self, message):
        # new value coming from a sensor (soliceted or unsolicited), save it
        if message.command == "IN":
            # retrieve the sensor_id for which this message is directed to
            sensor_id = message.args
            if sensor_id not in self.sensors:
                self.log_error("unable to match service request with registered sensor: "+message.dump())
                return
            sensor = self.sensors[sensor_id]
            # ignore incoming messages for sensors registered as actuators
            if sensor["service"]["mode"] == "actuator": return
            # save the value in the db
            self.save_value(sensor_id, message)
        # requested to invoke the service associated to the sensor
        elif message.command == "POLL":
            sensor_id = message.args
            self.run_sensor(sensor_id)
        # asked to set a new value to a sensor, save it and if the sensor has a service associated, send an OUT message
        elif message.command == "SET":
            # TODO: check if sensor_id is registered
            sensor_id = message.args
            sensor = self.sensors[sensor_id]
            # save the value in the db
            self.save_value(sensor_id, message)
            # if there is a service associated, forward the message to do the actual action
            if "service" in sensor and sensor["service"]["mode"] == "actuator":
                message.forward("service/"+sensor["service"]["name"])
                message.command = "OUT"
                # merge the request with the service's configuration
                data = message.get_data()
                data.update(sensor["service"]["configuration"])
                message.set_data(data)
                self.send(message)
        # the database just stored a new value, print it out since we have the sensor's context
        elif message.sender == "controller/db" and message.command == "SAVED":
            # catch only new values saved, not aggregations run
            if message.has("from_save") and message.get("from_save"):
                sensor_id = message.args
                sensor = self.sensors[sensor_id]
                description = sensor["description"] if "description" in sensor else ""
                unit = sensor["unit"] if "unit" in sensor else ""
                value = sdk.python.utils.strings.truncate(message.get("value"), 50)+unit
                if sensor["format"] == "calendar": value = "<calendar>"
                elif sensor["format"] == "image": value = "<image>"
                self.log_value("["+self.date.timestamp2date(message.get("timestamp"))+"] ["+sensor_id+"] \""+description+"\": "+value)

     # What to do when receiving a new/updated configuration for this module    
    def on_configuration(self, message):
        # ignore deleted configuration files while service is restarting
        if message.is_null and not message.args.startswith("sensors/"): return
        # module's configuration
        if message.args == self.fullname:
            if not self.is_valid_module_configuration(["poll_at_startup", "calculate", "retain"], message.get_data()): return False
            self.config = message.get_data()
        # we need the house timezone to set the timestamp when not provided by the sensor
        elif message.args == "house":
            if not self.is_valid_module_configuration(["timezone"], message.get_data()): return False
            self.date = DateTimeUtils(message.get("timezone"))
        # add/remove sensor
        elif message.args.startswith("sensors/"):
            if not self.configured: return
            sensor_id = message.args.replace("sensors/","")
            if message.is_null: self.remove_sensor(sensor_id)
            # TODO: check sensor mandatory config
            else: 
                sensor = message.get_data()
                if "disabled" in sensor and sensor["disabled"]: self.remove_sensor(sensor_id)
                else: self.add_sensor(sensor_id, sensor)
