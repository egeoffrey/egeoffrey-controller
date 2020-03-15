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

import re
import time
import copy

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
        # map sendor_id with sensor configuration
        self.sensors = {}
        # map for each rule_id/variable, the associated sensor_id
        self.variables = {}
        # scheduler is needed for scheduling rules
        self.scheduler = Scheduler(self)
        # regular expression used to parse variables
        self.variable_regexp = '^(DISTANCE|TIMESTAMP|ELAPSED|COUNT|SCHEDULE|POSITION_LABEL|POSITION_TEXT|)\s*(-\d+)?(,-\d+)?\s*(\S+)$'
        # require module configuration before starting up
        self.config_schema = 1
        self.rules_config_schema = 2
        self.sensors_config_schema = 1
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
            elif operator == "in":
                evaluation = str(a) in str(value)
            else: evaluation = False
        # return the evaluation
        return evaluation
        
    # replace placeholders (%placeholder%) with their values
    def format_placeholders(self, rule_id, macro, text):
        # find all &placeholder%
        placeholders = re.findall("%([^%]+)%", text)         
        for placeholder in placeholders:
            # replace the macro with its name
            if placeholder == "i":
                macro_text = macro
                if macro in self.sensors:
                    sensor = self.sensors[macro]
                    if "description" in sensor:
                        macro_text = sensor["description"]
                text = text.replace("%i%", macro_text)
            else:
                # get the value of the placeholder
                if isinstance(self.values[rule_id][macro][placeholder], list):
                    if len(self.values[rule_id][macro][placeholder]) == 0: value = ""
                    else: value = self.values[rule_id][macro][placeholder][0]
                else:
                    value = self.values[rule_id][macro][placeholder]
                # append the unit to the value if we got this sensor's configuration
                if rule_id in self.variables and macro in self.variables[rule_id] and placeholder in self.variables[rule_id][macro]:
                    sensor_id = self.variables[rule_id][macro][placeholder]
                    if sensor_id in self.sensors and "unit" in self.sensors[sensor_id]:
                        value = str(value)+str(self.sensors[sensor_id]["unit"])
                text = text.replace("%"+placeholder+"%", str(value))
        return text

    # retrieve the values of all the configured variables of the given rule_id. Continues in on_messages() when receiving values from db
    def run_rule(self, rule_id, macro=None):
        # if macro is defined, run the rule for that macro only, otherwise run for all the macros
        macros = [macro] if macro is not None else list(self.rules[rule_id].keys())
        for macro in macros:
            rule = self.rules[rule_id][macro]
            # ensure this rule is not run too often to avoid loops
            if rule_id in self.last_run and macro in self.last_run[rule_id] and time.time() - self.last_run[rule_id][macro] < 3: return
            # keep track of the last time this run has run
            if rule_id not in self.last_run: self.last_run[rule_id] = {}
            self.last_run[rule_id][macro] = time.time()
            self.log_debug("["+rule_id+"]["+macro+"] running rule")
            # for each sensor we need the value which will be asked to the db module. Keep track of both values and requests
            if rule_id not in self.requests: self.requests[rule_id] = {}
            self.requests[rule_id][macro] = []
            if rule_id not in self.values: self.values[rule_id] = {}
            self.values[rule_id][macro] = {}
            # for every constant, store its value as is so will be ready for the evaluation
            if "constants" in rule:
                for constant_id, value in rule["constants"].items():
                    self.values[rule_id][macro][constant_id] = value
            # for every variable, retrieve its latest value to the database
            if "variables" in rule:
                for variable_id, variable in rule["variables"].items():
                    # process the variable string (0: request, 1: start, 2: end, 3: sensor_id)
                    match = re.match(self.variable_regexp, variable)
                    if match is None: continue
                    # query db for the data
                    command, start, end, sensor_id = match.groups()
                    message = Message(self)
                    message.recipient = "controller/db"
                    message.command = message.command = "GET_"+command if command != "" else "GET"
                    message.args = sensor_id
                    start = -1 if start is None else int(start)
                    end = -1 if end is None else int(end.replace(",",""))
                    message.set("start", start)
                    message.set("end", end)
                    self.sessions.register(message, {
                        "rule_id": rule_id,
                        "variable_id": variable_id,
                        "macro": macro,
                    })
                    self.log_debug("["+rule_id+"]["+macro+"]["+variable_id+"] requesting db for "+message.command+" "+message.args+": "+str(message.get_data()))
                    self.send(message)
                    # keep track of the requests so that once all the data will be available the rule will be evaluated
                    self.requests[rule_id][macro].append(message.get_request_id())
                # add a placeholder at the end to ensure the rule is not evaluated before all the definitions are retrieved
                self.requests[rule_id][macro].append("LAST")
            # if the rule requires no data to retrieve, just evaluate it
            if len(self.requests[rule_id][macro]) == 0:
                self.evaluate_rule(rule_id, macro)

    # evaluate the conditions of a rule, once all the variables have been collected
    def evaluate_rule(self, rule_id, macro):
        rule = self.rules[rule_id][macro]
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
                    exp1_value = self.values[rule_id][macro][exp1]
                    exp2_value = self.values[rule_id][macro][exp2]
                    exp_value = self.evaluate_condition(exp1_value, operator, exp2_value)
                    self.log_debug("["+rule_id+"]["+macro+"] resolving "+exp1+" ("+sdk.python.utils.strings.truncate(str(exp1_value), 50)+") "+operator+" "+exp2+" ("+sdk.python.utils.strings.truncate(str(exp2_value), 50)+"): "+str(exp_value)+" (alias "+placeholder+")")
                    # store the sub expressions result in the values
                    self.values[rule_id][macro][placeholder] = exp_value
                    and_conditions = and_conditions.replace("("+expression+")", placeholder)
                # evaluate the main expression
                a, operator, b = and_conditions.split(' ')
                a_value = self.values[rule_id][macro][a]
                b_value = self.values[rule_id][macro][b]
                sub_evaluation = self.is_true(a_value, operator, b_value)
                self.log_debug("["+rule_id+"]["+macro+"] evaluating condition "+a+" ("+sdk.python.utils.strings.truncate(str(a_value), 50)+") "+operator+" "+b+" ("+sdk.python.utils.strings.truncate(str(b_value), 50)+"): "+str(sub_evaluation))
                and_evaluations.append(sub_evaluation)
            # evaluation is true if all the conditions are met
            and_evaluation = True
            for evaluation in and_evaluations:
                if not evaluation: and_evaluation = False
            self.log_debug("["+rule_id+"]["+macro+"] AND block evaluates to "+str(and_evaluation))
            or_evaluations.append(and_evaluation)
        # evaluation is true if at least one condition is met
        or_evaluation = False
        for evaluation in or_evaluations:
            if evaluation: or_evaluation = True
        # if there were no conditions, the rule evaluates to true
        if len(or_evaluations) == 0: or_evaluation = True
        self.log_debug("["+rule_id+"]["+macro+"] rule evaluates to "+str(or_evaluation))
        # evaluate to false, just return
        if not or_evaluation: return
        # 2) execute the requested actions
        if "actions" in rule:
            for action in rule["actions"]:
                action = re.sub(' +', ' ', action)
                # replace constants and variables placeholders in the action with their values
                action = self.format_placeholders(rule_id, macro, action)
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
        # replace constants and variables placeholders in the alert text with their values
        alert_text = self.format_placeholders(rule_id, macro, rule["text"])
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
        del self.requests[rule_id][macro]
        if len(self.requests[rule_id]) == 0: del self.requests[rule_id]
        del self.values[rule_id][macro]
        if len(self.values[rule_id]) == 0: del self.values[rule_id]
        
    # add a rule. Continues in run_rule() when rule is executed
    def add_rule(self, rule_id, rule):
        self.log_debug("Received configuration for rule "+rule_id)
        # clean it up first
        self.remove_rule(rule_id)
        # if macros are defined, repeat the same independently for each macro
        macros = rule["macros"] if "macros" in rule else ["_default_"]
        for macro in macros:
            # create a copy of the rule and keep track of it
            rule_i = copy.deepcopy(rule)
            if rule_id not in self.rules: self.rules[rule_id] = {}
            self.rules[rule_id][macro] = rule_i
            # for each variable
            if "variables" in rule_i:
                for variable_id in rule_i["variables"]:
                    # replace the macro placeholder if any
                    rule_i["variables"][variable_id] = rule_i["variables"][variable_id].replace("%i%", macro)
                    # process the variable content (0: request, 1: start, 2: end, 3: sensor_id)
                    match = re.match(self.variable_regexp, rule_i["variables"][variable_id])
                    if match is None: return
                    command, start, end, sensor_id = match.groups()
                    # request sensor's configuration for each variable, will be used when formatting the notification text
                    if command == "":
                        # remove any sub query (e.g. day/avg)
                        sensor_name = re.sub(r'\/(day|hour)\/[^\/]+$', '', sensor_id)
                        if sensor_name not in self.sensors:
                            # request the sensor's configuration
                            self.add_configuration_listener("sensors/"+sensor_name, self.sensors_config_schema)
                            # keep track of the associated between this variable_id and the sensor
                            if rule_id not in self.variables: self.variables[rule_id] = {}
                            if macro not in self.variables[rule_id]: self.variables[rule_id][macro] = {}
                            self.variables[rule_id][macro][variable_id] = sensor_name
            # for each action
            if "actions" in rule_i:
                # replace the macro placeholder if any
                for i in range(0, len(rule_i["actions"]))   :
                    rule_i["actions"][i] = rule_i["actions"][i].replace("%i%", macro)
            # for each trigger
            if "triggers" in rule_i:
                # replace the macro placeholder if any
                for i in range(0, len(rule_i["triggers"])):
                    rule_i["triggers"][i] = rule_i["triggers"][i].replace("%i%", macro)
            # if the rule is recurrent, we need to schedule its execution
            if rule_i["type"] == "recurrent":
                # schedule the rule execution
                self.log_debug("["+rule_id+"]["+macro+"] scheduling with the following settings: "+str(rule_i["schedule"]))
                # "schedule" contains apscheduler settings for this rule
                job = rule_i["schedule"]
                # add function to call and args
                job["func"] = self.run_rule
                job["args"] = [rule_id, macro]
                # schedule the job for execution and keep track of the job id
                if rule_id not in self.jobs: self.jobs[rule_id] = {}
                self.jobs[rule_id][macro] = self.scheduler.add_job(job).id
            # if the rule is realtime, add each sensor_id to the triggers so the rule will be run upon any change
            if rule_i["type"] == "realtime":
                if "triggers" in rule_i:
                    for sensor_id in rule_i["triggers"]:
                        if sensor_id not in self.triggers: self.triggers[sensor_id] = []
                        self.triggers[sensor_id].append(rule_id+"!"+macro)
                else:
                    self.log_warning("rule "+rule_id+" is of type realtime but no triggers are defined")
        # retrieve the sensor for each macro
        if "macros" in rule:
            for sensor_id in rule["macros"]:
                if sensor_id not in self.sensors:
                    # request the sensor's configuration
                    self.add_configuration_listener("sensors/"+sensor_id, self.sensors_config_schema)

    # remove a rule
    def remove_rule(self, rule_id):
        if rule_id in self.rules:
            self.log_debug("Removing rule "+rule_id)
            # delete the rule from every data structure
            del self.rules[rule_id]
            if rule_id in self.jobs: 
                for macro in self.jobs[rule_id]:
                    self.scheduler.remove_job(self.jobs[rule_id][macro])
            if rule_id in self.requests: del self.requests[rule_id]
            if rule_id in self.values: del self.values[rule_id]
            triggers = copy.deepcopy(self.triggers)
            for sensor_id in triggers:
                rules = list(self.triggers[sensor_id])
                for rule in rules:
                    if rule.startswith(rule_id+"!"):
                        self.triggers[sensor_id].remove(rule)
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
            self.log_debug("["+session["rule_id"]+"]["+session["macro"]+"] received from db "+session["variable_id"]+": "+str(message.get("data")))
            self.values[session["rule_id"]][session["macro"]][session["variable_id"]] = message.get("data")
            # remove the request_id from the queue of the rule
            self.requests[session["rule_id"]][session["macro"]].remove(message.get_request_id())
            # if there is only the LAST element in the queue, we have all the values, ready to evaluate the rule
            if len(self.requests[session["rule_id"]][session["macro"]]) == 1 and self.requests[session["rule_id"]][session["macro"]][0] == "LAST":
                self.evaluate_rule(session["rule_id"], session["macro"])
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
            for sensor_i in self.triggers:
                # sensor can contain also e.g. day/avg, check if starts with sensor_id
                if sensor_i.startswith(sensor_id):
                    for rule in self.triggers[sensor_i]:
                        rule_id, macro = rule.split("!")
                        self.run_rule(rule_id, macro)
        
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
        # add/remove sensors
        elif message.args.startswith("sensors/"):
            sensor_id = message.args.replace("sensors/","")
            # deleted sensor
            if message.is_null:
                if sensor_id in self.sensors: 
                    del self.sensors[sensor_id]
            # receiving sensor configuration
            else:
                self.sensors[sensor_id] = message.get_data()
        # add/remove rule
        elif message.args.startswith("rules/"):
            if not self.configured: 
                return
            # upgrade the rule schema
            if message.config_schema == 1 and not message.is_null:
                rule = message.get_data()
                if "for" in rule:
                    rule["macros"] = rule["for"]
                    del rule["for"]
                self.upgrade_config(message.args, message.config_schema, 2, rule)
                return
            if message.config_schema != self.rules_config_schema: 
                return
            rule_id = message.args.replace("rules/","")
            if message.is_null: 
                self.remove_rule(rule_id)
            else: 
                rule = message.get_data()
                if not self.is_valid_configuration(["text", "type", "severity"], rule): return
                if "disabled" in rule and rule["disabled"]: 
                    self.remove_rule(rule_id)
                else: self.add_rule(rule_id, rule)
            