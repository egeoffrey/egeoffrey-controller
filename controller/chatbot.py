### controller/chatbot: interactively respond to user's queries
## HOW IT WORKS: 
## DEPENDENCIES:
# OS:
# Python: fuzzywuzzy
## CONFIGURATION:
# required: vocabulary
# optional: 
## COMMUNICATION:
# INBOUND: 
# - ASK: receive request from other modules
# OUTBOUND: 
# - controller/alerter RUN: run a rule
# - controller/db GET: request value of a sensor to the database

import sys
reload(sys)  
sys.setdefaultencoding('utf8')
from fuzzywuzzy import fuzz
from fuzzywuzzy import process
import re

from sdk.module.controller import Controller
from sdk.module.helpers.message import Message

import sdk.utils.numbers

class Chatbot(Controller):
    # What to do when initializing    
    def on_init(self):
        # constants
        self.scorer = fuzz.token_set_ratio
        self.not_understood_score = 50
        self.cleanup = re.compile('[^a-zA-Z ]')
        # static/user vocabulary from the configuration
        self.vocabulary = {}
        # dynamic vocabulary from rules
        self.vocabulary_rules = {}
        # dynamic vocabulary from sensors
        self.vocabulary_sensors_text = {}
        self.vocabulary_sensors_image = {}
        # map sensor_id to sensor
        self.sensors = {}
        # request required configuration files
        self.add_configuration_listener("controller/chatbot", True)
        self.add_configuration_listener("rules/#")
        self.add_configuration_listener("sensors/#")
    
    # What to do when running
    def on_start(self):
        pass
        
    # What to do when shutting down
    def on_stop(self):
        pass
        
    # add a random prefix to a given text so to prentent a more dynamic interaction
    def add_prefix(self, text):
        add_prefix_rnd = sdk.utils.numbers.randint(0, 100)
        if add_prefix_rnd < 50:
            return self.vocabulary["prefix"][sdk.utils.numbers.randint(0, len(self.vocabulary["prefix"])-1)]+" "+text[0].lower() + text[1:]
        return text
    
    # return a random wait message
    def get_wait_message(self):
        return self.vocabulary["wait"][sdk.utils.numbers.randint(0,len(self.vocabulary["wait"])-1)]	
    
    # evaluate how confident we are to respond with on of the items of the kb provided for the current request
    def evaluate(self, request, kb):
        return process.extractOne(request, kb.keys(), scorer=self.scorer)
    
    # What to do when receiving a request for this module    
    def on_message(self, message):
        # somebody ask this chatbot about something
        if message.command == "ASK":
            request = message.get("request")
            accept = message.get("accept")
            # remove weird characters from the request
            request = self.cleanup.sub(' ', request)
            action = None
            # build up the vocabularies to check based on what the sender asks
            vocabularies = []
            if "text" in accept: vocabularies.extend([self.vocabulary["custom"], self.vocabulary_rules, self.vocabulary_sensors_text])
            if "image" in accept: vocabularies.extend([self.vocabulary_sensors_image])
            # evaluate each dictionary individually until we find a good answer
            for kb in vocabularies:
                keywords, score = self.evaluate(request, kb)
                # if we are confident enough
                if score > self.not_understood_score:
                    actions = kb[keywords]
                    # pick up a random action and break
                    action = actions[sdk.utils.numbers.randint(0,len(actions)-1)]
                    self.log_info("I've been asked by "+message.sender+" '"+request+"'. I am "+str(score)+"% sure to respond with '"+str(action)+"'")
                    break
            # if we have no good answer, just tell the sender
            if action is None:
                action = self.vocabulary["not_understood"][sdk.utils.numbers.randint(0,len(self.vocabulary["not_understood"])-1)]
                self.log_info("I've been asked by "+message.sender+" '"+request+"' but I'm not sure enough so I'd respond with '"+str(action)+"'")
            # reponse is a static text
            if keywords in self.vocabulary["custom"] or action in self.vocabulary["not_understood"]:
                # respond back
                message.reply()
                message.set("type", "text")
                message.set("content", action)
                self.send(message)
            # reponse is associated to a rule
            elif keywords in self.vocabulary_rules:
                # ask alerter to run the rule (requesting module has to intercept NOTIFY broadcast
                alerter_msg = Message(self)
                alerter_msg.recipient = "controller/alerter"
                alerter_msg.command = "RUN"
                alerter_msg.args = action
                self.sessions.register(alerter_msg, {
                    "message": message
                })
                self.send(alerter_msg)
            # reponse is associated to a sensor
            elif keywords in self.vocabulary_sensors_text or keywords in self.vocabulary_sensors_image:
                # ask the db for the latest value of the sensor (continues in message.command == "GET")
                db_msg = Message(self)
                db_msg.recipient = "controller/db"
                db_msg.command = "GET"
                db_msg.args = action
                self.sessions.register(db_msg, {
                    "message": message,
                    "description": keywords.lower()
                })
                self.send(db_msg)
        # received latest value from a sensor
        elif message.sender == "controller/db" and message.command == "GET":
            session = self.sessions.restore(message)
            if session is None: return
            sensor_id = message.args
            sensor = self.sensors[sensor_id]
            value = str(message.get("data")[0])
            if sensor_id in self.sensors and "unit" in self.sensors[sensor_id]: value = value+str(self.sensors[sensor_id]["unit"])
            message = session["message"]
            message.reply()
            if sensor["format"] == "image":
                type = "image"
                message.set("description", sensor["description"] if "description" in sensor else "")
            else:
                value_is = self.vocabulary["value_is"][sdk.utils.numbers.randint(0, len(self.vocabulary["value_is"])-1)]
                value = session["description"]+" "+value_is+" "+value
                type = "text"
            message.set("type", type)
            message.set("content", value)
            self.send(message)
        # received response from alerter after running the requested rule
        elif message.sender == "controller/alerter" and message.command == "RUN":
            session = self.sessions.restore(message)
            if session is None: return
            text = message.get_data()
            # retrieve requesting message
            message = session["message"]
            message.reply()
            message.set("type", "text")
            value_is = self.vocabulary["value_is"][sdk.utils.numbers.randint(0, len(self.vocabulary["value_is"])-1)]
            message.set("content", text)
            self.send(message)
            
            
     # What to do when receiving a new/updated configuration for this module    
    def on_configuration(self, message):
        # TODO: remove from vocabulary
        if message.is_null: return
        # module's configuration
        if message.args == self.fullname:
            if not self.is_valid_module_configuration(["vocabulary"], message.get_data()): return
            self.vocabulary = message.get("vocabulary")
        # rules
        elif message.args.startswith("rules/"):
            if not self.configured: return
            rule_id = message.args.replace("rules/","")
            rule = message.get_data()
            if "disabled" in rule and rule["disabled"]: return
            # ignore rules with a condition
            if len(rule["conditions"]) != 0: return
            self.log_debug("adding rule "+rule_id+" to the vocabulary")
            self.vocabulary_rules[rule["text"]] = [rule_id]
        # sensors
        elif message.args.startswith("sensors/"):
            if not self.configured: return
            sensor_id = message.args.replace("sensors/","")
            sensor = message.get_data()
            if "disabled" in sensor and sensor["disabled"]: return
            if "description" not in sensor or sensor["format"] not in ["int", "float_1", "float_2", "string", "image"]: return
            self.log_debug("adding sensor "+sensor_id+" to the vocabulary")
            if sensor["format"] == "image": self.vocabulary_sensors_image[sensor["description"]] = [sensor_id]
            else: self.vocabulary_sensors_text[sensor["description"]] = [sensor_id]
            self.sensors[sensor_id] = sensor

