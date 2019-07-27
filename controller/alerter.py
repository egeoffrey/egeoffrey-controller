### controller/alerter: alert the user when configured rules triggers
## HOW IT WORKS: on_message(RUN)/scheduler -> run_rule() -> on_message(GET) -> evaluate_rule()
## DEPENDENCIES:
# OS:
# Python: 
## CONFIGURATION:
# required: retention
# optional: 
## COMMUNICATION:
# INBOUND: 
# - RUN: run manually a given rule
# OUTBOUND: 
# - controller/db GET*: request to the database
# - controller/hub SET/POLL: action to execute when a rule trigger
# - controller/alerter RUN: action to execute when a rule trigger
# - controller/db SAVE_ALERT: save notification to db
# - */* NOTIFY: notify output modules
# - controller/db PURGE_ALERTS: periodically purge old alerts from db

# TODO: move this into sdk
import sys 
reload(sys)  
sys.setdefaultencoding('utf8')
import re
import time

from sdk.python.module.controller import Controller
from sdk.python.module.helpers.scheduler import Scheduler
from sdk.python.module.helpers.message import Message

import sdk.python.utils.numbers
import sdk.python.utils.strings

class Alerter(Controller):
    # What to do when initializing
    def on_init(self):
        # module's configuration
        self.config = {}
        # map rule_id with rule configuration        
        self.rules = {} 
        # map rule_id with an array of request_id the rule will wait for to complete
        self.requests = {} 
        # map for each rule_id constants/variable_id with their values
        self.values = {} 
        # for manually run rules, map rule_id with requesting message
        self.on_demand = {}
        # for rule without a schedule, map sensor_id with an array of rules to run upon a change
        self.triggers = {}
        # map rule_id with the timestamp it run last so to avoid running it too frequently
        self.last_run = {}
        # map rule_id with scheduler job_id
        self.jobs = {}
        # scheduler is needed for scheduling rules
        self.scheduler = Scheduler(self)
        # require module configuration before starting up
        self.config_schema = 1
        self.rules_config_schema = 1
        self.add_configuration_listener(self.fullname, "+", True)
        # subscribe for acknowledgments from the database for saved values
        self.add_inspection_listener("controller/db", "*/*", "SAVED", "#")
        
    # calculate a sub expression
    def evaluate_condition(self, a, operator, b):
        # prepare the values (can be an array)
        if isinstance(a, list): a = a[0]
        if isinstance(b, list): b = b[0]
        # perform integrity checks
        if a is None or b is None: return None
        if not sdk.python.utils.numbers.is_number(a) or not sdk.python.utils.numbers.is_number(b): return None
        # calculate the expression
        # TODO: contains?
        if operator == "+": return float(a)+float(b)
        elif operator == "-": return float(a)-float(b)
        elif operator == "*": return float(a)*float(b)
        elif operator == "/": return float(a)/float(b)
        return None

    # evaluate if a condition is met
    def is_true(self, a, operator, b):
        evaluation = True
        # get a's value
        if not isinstance(a, list): a = [a]
        if len(a) == 0: return False
        a = a[0]
        # prepare b's value
        if not isinstance(b, list): b = [b]
        if len(b) == 0: return False
        # b can be have multiple values, cycle through all of them
        for value in b:
            if value is None or a is None: evaluation = False
            elif operator == "==":
                if value != a: evaluation = False
            elif operator == "!=":
                if value == a: evaluation = False
            elif operator == ">":
                if not sdk.python.utils.numbers.is_number(value) or not sdk.python.utils.numbers.is_number(a): return False
                if float(value) >= float(a): evaluation = False
            elif operator == "<":
                if not sdk.python.utils.numbers.is_number(value) or not sdk.python.utils.numbers.is_number(a): return False
                if float(value) <= float(a): evaluation = False
            else: evaluation = False
        # return the evaluation
        return evaluation
        
    # replace placeholders (%placeholder%) with their values
    def format_placeholders(self, rule_id, repeat_for_i, text):
        placeholders = re.findall("%([^%]+)%", text)         
        for placeholder in placeholders:
            if isinstance(self.values[rule_id][repeat_for_i][placeholder], list):
                if len(self.values[rule_id][repeat_for_i][placeholder]) == 0: value = ""
                else: value = self.values[rule_id][repeat_for_i][placeholder][0]
            else:
                value = self.values[rule_id][repeat_for_i][placeholder]
            text = text.replace("%"+placeholder+"%", str(value))
        return text

    # retrieve the values of all the configured variables of the given rule_id. Continues in on_messages() when receiving values from db
    def run_rule(self, rule_id):
        rule = self.rules[rule_id]
        # ensure this rule is not run too often to avoid loops
        if rule_id in self.last_run and time.time() - self.last_run[rule_id] < 3: return
        self.last_run[rule_id] = time.time()
        self.log_debug("Running rule "+rule_id)
        # for each sensor we need the value which will be asked to the db module. Keep track of both values and requests
        self.requests[rule_id] = {}
        self.values[rule_id] = {}
        # when "for" is used, the same rule is run independently for each item 
        repeat_for = rule["for"] if "for" in rule else ["_default_"]
        for repeat_for_i in repeat_for:
            self.requests[rule_id][repeat_for_i] = []
            self.values[rule_id][repeat_for_i] = {}
        # for every constant, store its value as is so will be ready for the evaluation
        if "constants" in rule:
            for repeat_for_i in repeat_for:
                for constant_id, value in rule["constants"].iteritems():
                    self.values[rule_id][repeat_for_i][constant_id] = value
        # for every variable, retrieve its latest value to the database
        if "variables" in rule:
            for i in range(len(repeat_for)):
                repeat_for_i = repeat_for[i]
                for variable_id, variable in rule["variables"].iteritems():
                    # a variable can contain %i% which is replaced with every item of "for"
                    variable_i = variable.replace("%i%", repeat_for_i)
                    # match the variable string (0: request, 1: start, 2: end, 3: sensor_id)
                    match = re.match('^(DISTANCE|TIMESTAMP|ELAPSED|COUNT|SCHEDULE|)\s*(-\d+)?(,-\d+)?\s*(\S+)$', variable_i)
                    if match is None: continue
                    # query db for the data
                    command, start, end, sensor_id = match.groups()
                    message = Message(self)
                    message.recipient = "controller/db"
                    message.command = message.command = "GET_"+command if command != "" else "GET"
                    message.args = sensor_id
                    message.set("start", -1) if start is None else start
                    message.set("end", -1) if end is None else end
                    self.sessions.register(message, {
                        "rule_id": rule_id,
                        "variable_id": variable_id,
                        "%i%": repeat_for_i,
                    })
                    self.log_debug("["+rule_id+"]["+variable_id+"] requesting db for "+message.command+" "+message.args+": "+str(message.get_data()))
                    self.send(message)
                    # keep track of the requests so that once all the data will be available the rule will be evaluated
                    self.requests[rule_id][repeat_for_i].append(message.get_request_id())
                # add a placeholder at the end to ensure the rule is not evaluated before all the definitions are retrieved
                self.requests[rule_id][repeat_for_i].append("LAST")
        # if the rule requires no data to retrieve, just evaluate it
        if len(self.requests[rule_id][repeat_for_i]) == 0:
            self.evaluate_rule(rule_id, repeat_for_i)

    # evaluate the conditions of a rule, once all the variables have been collected
    def evaluate_rule(self, rule_id, repeat_for_i):
        rule = self.rules[rule_id]
        # 1) evaluate all the conditions of the rule
        or_evaluations = []
        for or_conditions in rule["conditions"]:
            and_evaluations = []
            for and_conditions in or_conditions:
                # remove spaces
                and_conditions = re.sub(' +',' ', and_conditions)
                # look for sub expressions (grouped within parenthesis) and calculate them individually
                expressions = re.findall("\(([^\)]+)\)", and_conditions)
                for i in range(len(expressions)):
                    expression = expressions[i]
                    # subexpression will become internal variables
                    placeholder = "%exp_"+str(i)+"%" 
                    # expression format is "exp1 operator exp2" (e.g. a == b)
                    exp1, operator, exp2 = expression.split(' ') 
                    # calculate the sub expression
                    exp1_value = self.values[rule_id][repeat_for_i][exp1]
                    exp2_value = self.values[rule_id][repeat_for_i][exp2]
                    exp_value = self.evaluate_condition(exp1_value, operator, exp2_value)
                    self.log_debug("["+rule_id+"]["+repeat_for_i+"] resolving "+exp1+" ("+sdk.python.utils.strings.truncate(str(exp1_value), 50)+") "+operator+" "+exp2+" ("+sdk.python.utils.strings.truncate(str(exp2_value), 50)+"): "+str(exp_value)+" (alias "+placeholder+")")
                    # store the sub expressions result in the values
                    self.values[rule_id][repeat_for_i][placeholder] = exp_value
                    and_conditions = and_conditions.replace("("+expression+")", placeholder)
                # evaluate the main expression
                a, operator, b = and_conditions.split(' ')
                a_value = self.values[rule_id][repeat_for_i][a]
                # TODO: make this a function
                b_value = self.values[rule_id][repeat_for_i][b]
                sub_evaluation = self.is_true(a_value, operator, b_value)
                self.log_debug("["+rule_id+"]["+repeat_for_i+"] evaluating condition "+a+" ("+sdk.python.utils.strings.truncate(str(a_value), 50)+") "+operator+" "+b+" ("+sdk.python.utils.strings.truncate(str(b_value), 50)+"): "+str(sub_evaluation))
                and_evaluations.append(sub_evaluation)
            # evaluation is true if all the conditions are met
            and_evaluation = True
            for evaluation in and_evaluations:
                if not evaluation: and_evaluation = False
            self.log_debug("["+rule_id+"]["+repeat_for_i+"] AND block evaluates to "+str(and_evaluation))
            or_evaluations.append(and_evaluation)
        # evaluation is true if at least one condition is met
        or_evaluation = False
        for evaluation in or_evaluations:
            if evaluation: or_evaluation = True
        # if there were no conditions, the rule evaluates to true
        if len(or_evaluations) == 0: or_evaluation = True
        self.log_debug("["+rule_id+"]["+repeat_for_i+"] rule evaluates to "+str(or_evaluation))
        # evaluate to false, just return
        if not or_evaluation: return
        # 2) execute the requested actions
        if "actions" in rule:
            for action in rule["actions"]:
                action = re.sub(' +', ' ', action).replace("%i%", repeat_for_i)
                # TODO: make this a function
                # replace constants and variables placeholders in the action with their values
                action = self.format_placeholders(rule_id, repeat_for_i, action)
                # TODO: ifnotexists, force
                # execute the action
                action_split = action.split(" ")
                command = action_split[0]
                # set the sensor to a value or poll it
                if command == "SET" or command == "POLL":
                    sensor_id = action_split[1]
                    message = Message(self)
                    message.recipient = "controller/hub"
                    message.command = command
                    message.args = sensor_id
                    if command == "SET": message.set_data(action_split[2])
                    self.send(message)
                # run another rule
                elif command == "RUN":
                    rule_to_run = action_split[1]
                    message = Message(self)
                    message.recipient = "controller/alerter"
                    message.command = command
                    message.args = rule_to_run
                    self.send(message)
        # 3) format the alert text
        # TODO: sensor's unit to alert_text + sensor description for %i%
        alert_text = rule["text"].replace("%i%", repeat_for_i)
        # replace constants and variables placeholders in the alert text with their values
        # TODO: aliases, suffix
        alert_text = self.format_placeholders(rule_id, repeat_for_i, alert_text)
        # 4) notify about the alert and save it
        if rule["severity"] != "none" and rule_id not in self.on_demand:
            self.log_info("["+rule_id+"]["+rule["severity"]+"] "+alert_text)
            if rule["severity"] != "debug":
                # ask db to save the alert
                message = Message(self)
                message.recipient = "controller/db"
                message.command = "SAVE_ALERT"
                message.args = rule["severity"]
                message.set_data(alert_text)
                self.send(message)
                # trigger output modules for notifications
                message = Message(self)
                message.recipient = "*/*"
                message.command = "NOTIFY"
                message.args = rule["severity"]+"/"+rule_id
                message.set_data(alert_text)
                self.send(message)
        # 5) if rule is manually requested to run, respond back
        if rule_id in self.on_demand:
            # retrieve original message
            message = self.on_demand[rule_id]
            message.reply()
            message.set_data(alert_text)
            self.send(message)
            del self.on_demand[rule_id]
        # 6) clean up, rule completed execution, remove the rule_id from the request queue and all the collected values
        del self.requests[rule_id][repeat_for_i]
        if len(self.requests[rule_id]) == 0: del self.requests[rule_id]
        del self.values[rule_id][repeat_for_i]
        if len(self.values[rule_id]) == 0: del self.values[rule_id]
        
    # add a rule. Continues in run_rule() when rule is executed
    def add_rule(self, rule_id, rule):
        self.log_debug("Received configuration for rule "+rule_id)
        # clean it up first
        self.remove_rule(rule_id)
        self.rules[rule_id] = rule
        # TODO: "startup" rules
        # rule will be run upon a schedule
        if rule["type"] == "recurrent":
            # schedule the rule execution
            self.log_debug("Scheduling "+rule_id+" with the following settings: "+str(rule["schedule"]))
            # "schedule" contains apscheduler settings for this sensor
            job = rule["schedule"]
            # add function to call and args
            job["func"] = self.run_rule  # first thing to do when executing is populating the variables with values
            job["args"] = [rule_id]
            # schedule the job for execution and keep track of the job id
            self.jobs[rule_id] = self.scheduler.add_job(job).id
        # for rules without a schedule, rule will be run every time one of the variables will change value
        elif rule["type"] == "realtime":
            if "variables" in rule:
                # when "for" is used, the same rule is run independently for each item 
                repeat_for = rule["for"] if "for" in rule else ["_default_"]
                for repeat_for_i in repeat_for:
                    for variable_id, variable in rule["variables"].iteritems():
                        # a variable can contain %i% which is replaced with every item of "for"
                        variable_i = variable.replace("%i%", repeat_for_i)
                        # match the variable string (0: request, 1: start, 2: end, 3: sensor_id)
                        match = re.match('^(DISTANCE|TIMESTAMP|ELAPSED|COUNT|SCHEDULE|)\s*(-\d+)?(,-\d+)?\s*(\S+)$', variable_i)
                        if match is None: return
                        command, start, end, sensor_id = match.groups()
                        # add the sensor_id to the triggers so the rule will be run upon any change
                        if sensor_id not in self.triggers: 
                            self.triggers[sensor_id] = []
                        self.triggers[sensor_id].append(rule_id)
    
    # remove a rule
    def remove_rule(self, rule_id):
        if rule_id in self.rules:
            self.log_debug("Removing rule "+rule_id)
            # delete the rule from every data structure
            del self.rules[rule_id]
            if rule_id in self.jobs: self.scheduler.remove_job(self.jobs[rule_id])
            if rule_id in self.requests: del self.requests[rule_id]
            if rule_id in self.values: del self.values[rule_id]
            for sensor_id in self.triggers:
                if rule_id in self.triggers[sensor_id]:
                    self.triggers[sensor_id].remove(rule_id)
                    if len(self.triggers[sensor_id]) == 0: del self.triggers[sensor_id]
        
    # apply configured retention policies for saved alerts
    def retention_policies(self):
        # ask the database module to purge the data
        message = Message(self)
        message.recipient = "controller/db"
        message.command = "PURGE_ALERTS"
        message.set_data(self.config["retention"])
        self.send(message)
        
    # What to do when running    
    def on_start(self):
        # ask for all rules' configuration
        self.add_configuration_listener("rules/#", "+")
        # schedule to apply configured retention policies (every day just after 1am)
        job = {"func": self.retention_policies, "trigger":"cron", "hour": 1, "minute": 0, "second": sdk.python.utils.numbers.randint(1,59)}
        self.scheduler.add_job(job)
        # start the scheduler 
        self.scheduler.start()
        
    # What to do when shutting down
    def on_stop(self):
        self.scheduler.stop()

    # What to do when receiving a request for this module. Continues in evaluate_rule() once all the variables are set
    def on_message(self, message):
        # handle responses from the database
        if message.sender == "controller/db" and message.command.startswith("GET"):
            session = self.sessions.restore(message)
            if session is None: return
            # cache the value of the variable
            self.log_debug("["+session["rule_id"]+"]["+session["%i%"]+"] received from db "+session["variable_id"]+": "+str(message.get("data")))
            self.values[session["rule_id"]][session["%i%"]][session["variable_id"]] = message.get("data")
            # remove the request_id from the queue of the rule
            self.requests[session["rule_id"]][session["%i%"]].remove(message.get_request_id())
            # if there is only the LAST element in the queue, we have all the values, ready to evaluate the rule
            if len(self.requests[session["rule_id"]][session["%i%"]]) == 1 and self.requests[session["rule_id"]][session["%i%"]][0] == "LAST":
                self.evaluate_rule(session["rule_id"], session["%i%"])
        # run a given rule
        elif message.command == "RUN":
            rule_id = message.args
            if message.sender == "controller/chatbot": self.on_demand[rule_id] = message
            self.run_rule(rule_id)
        # the database just stored a new value, print it out since we have the sensor's context
        elif message.sender == "controller/db" and message.command == "SAVED":
            sensor_id = message.args
            if message.has("group_by"): sensor_id = sensor_id+"/"+message.get("group_by")
            # check if a rule has this sensor_id among its variables, if so, run it
            for sensor in self.triggers:
                # sensor can contain also e.g. day/avg, check if starts with sensor_id
                if sensor.startswith(sensor_id):
                    for rule_id in self.triggers[sensor]:
                        self.run_rule(rule_id)
        
    # What to do when receiving a rule
    def on_configuration(self, message):
        # ignore deleted configuration files while service is restarting
        if message.is_null and not message.args.startswith("rules/"): return
        # module's configuration
        if message.args == self.fullname and not message.is_null:
            if message.config_schema != self.config_schema: 
                return False
            # ensure the configuration file contains all required settings
            if not self.is_valid_configuration(["retention"], message.get_data()): return False
            self.config = message.get_data()
        # add/remove rule
        elif message.args.startswith("rules/"):
            if not self.configured: return
            if message.config_schema != self.rules_config_schema: 
                return
            rule_id = message.args.replace("rules/","")
            if message.is_null: self.remove_rule(rule_id)
            # TODO: check rule mandatory config
            else: 
                rule = message.get_data()
                if "disabled" in rule and rule["disabled"]: self.remove_rule(rule_id)
                else: self.add_rule(rule_id, rule)
            