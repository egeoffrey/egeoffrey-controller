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

from sdk.module.controller import Controller
from sdk.module.helpers.scheduler import Scheduler
from sdk.module.helpers.message import Message

import sdk.utils.numbers

class Alerter(Controller):
    # What to do when initializing
    def on_init(self):
        # module's configuration
        self.config = {}
        # map rule_id with rule configuration        
        self.rules = {} 
        # map rule_id with an array of request_id the rule will wait for to complete
        self.requests = {} 
        # map for each rule_id constants/variable_id with the value
        self.values = {} 
        # for manually run rules, map rule_id with requesting message
        self.on_demand = {}
        # map rule_id with scheduler job_id
        self.jobs = {}
        # scheduler is needed for scheduling rules
        self.scheduler = Scheduler(self)
        # require module configuration before starting up
        self.add_configuration_listener(self.fullname, True)
        
    # calculate a sub expression
    def evaluate_condition(self, a, operator, b):
        # prepare the values (can be an array)
        if isinstance(a, list): a = a[0]
        if isinstance(b, list): b = b[0]
        # perform integrity checks
        if a is None or b is None: return None
        if not sdk.utils.numbers.is_number(a) or not sdk.utils.numbers.is_number(b): return None
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
        if not isinstance(a,list): a = [a]
        a = a[0]
        # prepare b's value
        if not isinstance(b,list): b = [b]
        # b can be have multiple values, cycle through all of them
        for value in b:
            if value is None or a is None: evaluation = False
            elif operator == "==":
                if value != a: evaluation = False
            elif operator == "!=":
                if value == a: evaluation = False
            elif operator == ">":
                if not sdk.utils.numbers.is_number(value) or not sdk.utils.numbers.is_number(a): return False
                if float(value) >= float(a): evaluation = False
            elif operator == "<":
                if not sdk.utils.numbers.is_number(value) or not sdk.utils.numbers.is_number(a): return False
                if float(value) <= float(a): evaluation = False
            else: evaluation = False
        # return the evaluation
        return evaluation

    # retrieve the values of all the configured variables of the given rule_id. Continues in on_messages() when receiving values from db
    def run_rule(self, rule_id):
        rule = self.rules[rule_id]
        # for each sensor we need a value from, ask it to the db module
        # TODO: %i and for
        self.requests[rule_id] = []
        self.values[rule_id] = {}
        # if every constant, store its value as it is so will be ready for the evaluation
        if "constants" in rule:
            for constant_id, value in rule["constants"].iteritems():
                self.values[rule_id][constant_id] = value
        # for every variable, request its latest value to the database
        for variable_id, variable in rule["variables"].iteritems():
            # match the variable string (0: request, 1: start, 2: end, 3: sensor_id)
            # TODO: simplify this by allowing free text (e.g. get/...)
            match = re.match('^(DISTANCE|TIMESTAMP|ELAPSED|COUNT|)\s*(-\d+)?(,-\d+)?\s*(\S+)$', variable)
            if match is None: continue
            # query db for the data
            command, start, end, sensor_id = match.groups()
            message = Message(self)
            message.recipient = "controller/db"
            message.command = message.command = "GET_"+command if command != "" else "GET"
            message.args = sensor_id
            # TODO: how to pass formatter without knowing the sensor's format
            message.set("start", -1) if start is None else start
            message.set("end", -1) if end is None else end
            self.sessions.register(message, {
                "rule_id": rule_id,
                "variable_id": variable_id
            })
            self.log_debug("["+rule_id+"]["+variable_id+"] requesting db for "+message.command+" "+message.args+": "+str(message.get_data()))
            self.send(message)
            # keep track of the requests so that once all the data will be available the rule will be evaluated
            self.requests[rule_id].append(message.get_request_id())
        # add a placeholder at the end to ensure the rule is not evaluated before all the definitions are retrieved
        self.requests[rule_id].append("LAST")

    # evaluate the conditions of a rule, once all the variables have been collected
    def evaluate_rule(self, rule_id):
        rule = self.rules[rule_id]
        evaluation = True
        # TODO: add support for OR (e.g. arrays whose elements are evaluated in AND and then OR applied
        # 1) evaluate all the conditions of the rule
        for condition in rule["conditions"]:
            condition = re.sub(' +',' ',condition)  # remove spaces
            # look for sub expressions (grouped within parenthesis) and calculate them individually
            expressions = re.findall("\(([^\)]+)\)", condition)
            for i in range(len(expressions)):
                expression = expressions[i]
                # subexpression will become internal variables
                placeholder = "%exp_"+str(i)+"%" 
                # expression format is "exp1 operator exp2" (e.g. a == b)
                exp1, operator, exp2 = expression.split(' ') 
                # calculate the sub expression
                exp1_value = self.values[rule_id][exp1]
                exp2_value = self.values[rule_id][exp2]
                exp_value = self.evaluate_condition(exp1_value, operator, exp2_value)
                self.log_debug("["+rule_id+"] resolving "+exp1+" ("+str(exp1_value)+") "+operator+" "+exp2+" ("+str(exp2_value)+"): "+str(exp_value)+" (alias "+placeholder+")")
                # store the sub expressions result in the values
                self.values[rule_id][placeholder] = exp_value
                condition = condition.replace("("+expression+")",placeholder)
            # evaluate the main expression
            a, operator, b = condition.split(' ')
            a_value = self.values[rule_id][a]
            # TODO: make this join a function
            b_value = self.values[rule_id][b]
            sub_evaluation = self.is_true(a_value, operator, b_value)
            self.log_debug("["+rule_id+"] evaluating condition "+a+" ("+str(a_value)+") "+operator+" "+b+" ("+str(b_value)+"): "+str(sub_evaluation))
            if not sub_evaluation: evaluation = False # if this condition is false, the entire evaluation will be false
        self.log_debug("["+rule_id+"] rule evaluates to "+str(evaluation))
        # evaluate to false, just returns
        if not evaluation: return
        # 2) execute the requested actions
        if "actions" in rule:
            for action in rule["actions"]:
                action = re.sub(' +', ' ', action)  # remove spaces
                # TODO: make this a function
                # replace constants and variables placeholders in the action with their values
                placeholders = re.findall("%([^%]+)%", action)         
                for placeholder in placeholders:
                    value = self.values[rule_id][placeholder][0] if isinstance(self.values[rule_id][placeholder],list) else self.values[rule_id][placeholder]
                    action = action.replace("%"+placeholder+"%", str(value))
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
        # TODO: sensor's unit to alert_text
        alert_text = rule["text"]
        # replace constants and variables placeholders in the alert text with their values
        placeholders = re.findall("%([^%]+)%", alert_text)
        # TODO: aliases, suffix
        for placeholder in placeholders:
            value = self.values[rule_id][placeholder][0] if isinstance(self.values[rule_id][placeholder],list) else self.values[rule_id][placeholder]
            alert_text = alert_text.replace("%"+placeholder+"%",str(value))
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
        del self.requests[rule_id]
        del self.values[rule_id]
        
    # schedule a given rule for execution. Continues in run_rule() when rule is executed
    def add_rule(self, rule_id, rule):
        self.log_debug("Received configuration for rule "+rule_id)
        # clean it up first
        self.remove_rule(rule_id)
        self.rules[rule_id] = rule
        # TODO: "startup" rules
        # no schedule set
        if "schedule" not in rule: return
        # schedule the rule execution
        self.log_debug("Scheduling "+rule_id+" with the following settings: "+str(rule["schedule"]))
        # "schedule" contains apscheduler settings for this sensor
        job = rule["schedule"]
        # add function to call and args
        job["func"] = self.run_rule  # first thing to do when executing is populating the variables with values
        job["args"] = [rule_id]
        # schedule the job for execution and keep track of the job id
        self.jobs[rule_id] = self.scheduler.add_job(job).id
        
    def remove_rule(self, rule_id):
        if rule_id in self.rules: 
            self.log_debug("Removing rule "+rule_id)
            del self.rules[rule_id]
            if rule_id in self.jobs: self.scheduler.remove_job(self.jobs[rule_id])
        
    # apply configured retention policies for saved alerts
    def retention_policies(self):
        # ask the database module to purge the data
        message = Message(self)
        message.recipient = "controller/db"
        message.command = "PURGE_ALERTS"
        message.set_data("retention")
        self.send(message)
        
    # What to do when running    
    def on_start(self):
        # ask for all rules' configuration
        self.add_configuration_listener("rules/#")
        # schedule to apply configured retention policies (every day just after 1am)
        job = {"func": self.retention_policies, "trigger":"cron", "hour": 1, "minute": 0, "second": sdk.utils.numbers.randint(1,59)}
        self.scheduler.add_job(job)
        # start the scheduler 
        self.scheduler.start()
        
    # What to do when shutting down
    def on_stop(self):
        self.scheduler.stop()

    # What to do when receiving a request for this module. Continues in evaluate_rule() once all the variables are set
    def on_message(self, message):
        # TODO: realtime alerts
        # handle responses from the database
        if message.sender == "controller/db" and message.command.startswith("GET"):
            session = self.sessions.restore(message)
            if session is None: return
            # cache the value of the variable
            self.values[session["rule_id"]][session["variable_id"]] = message.get("data")
            # remove the request_id from the queue of the rule
            for rule_id, requests in self.requests.iteritems():
                # this is the rule waiting for this message
                if message.get_request_id() in self.requests[rule_id]: 
                    self.requests[rule_id].remove(message.get_request_id())
                    # if there is only the LAST element in the queue, we have all the values, ready to evaluate the rule
                    if len(self.requests[rule_id]) == 1 and self.requests[rule_id][0] == "LAST":
                        self.evaluate_rule(rule_id)
                        break
        # run a given rule
        elif message.command == "RUN":
            rule_id = message.args
            if message.sender == "controller/chatbot": self.on_demand[rule_id] = message
            self.run_rule(rule_id)
        
    # What to do when receiving a rule
    def on_configuration(self, message):
        # ignore deleted configuration files while service is restarting
        if message.is_null and not message.args.startswith("rules/"): return
        # module's configuration
        if message.args == self.fullname:
            # ensure the configuration file contains all required settings
            if not self.is_valid_module_configuration(["retention"], message.get_data()): return False
            self.config = message.get_data()
        # add/remove rule
        elif message.args.startswith("rules/"):
            if not self.configured: return
            rule_id = message.args.replace("rules/","")
            if message.is_null: self.remove_rule(rule_id)
            # TODO: check rule mandatory config
            else: 
                rule = message.get_data()
                if "disabled" in rule and rule["disabled"]: self.remove_rule(rule_id)
                else: self.add_rule(rule_id, rule)
            