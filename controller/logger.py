### controller/logger: listen from log messages on the bus and print them out
## HOW IT WORKS: 
## DEPENDENCIES:
# OS: 
# Python: 
## CONFIGURATION:
# required: 
# optional: 
## COMMUNICATION:
# INBOUND: 
# - LOG: print out a new log message
# OUTBOUND: 

import sys 
reload(sys)  
sys.setdefaultencoding('utf8')
import time

from sdk.python.module.controller import Controller
from sdk.python.module.helpers.scheduler import Scheduler
from sdk.python.module.helpers.message import Message

import sdk.python.utils.strings
import sdk.python.utils.numbers

class Logger(Controller):
    # What to do when initializing
    def on_init(self):
        # TODO: log to file, allow to enable different logging
        # maximum log messages per second to print
        self.max_msg_rate = 5
        self.msg_count = 0
        self.msg_time = time.time()
        # scheduler is needed for purging old logs
        self.scheduler = Scheduler(self)
        # require module configuration before starting up
        self.add_configuration_listener(self.fullname, True)
        
    # What to do when running    
    def on_start(self):
        # schedule to apply configured retention policies
        job = {"func": self.retention_policies, "trigger":"cron", "hour": 1, "minute": 0, "second": sdk.python.utils.numbers.randint(1,59)}
        self.scheduler.add_job(job)
        # start the scheduler 
        self.scheduler.start()

        
    # What to do when shutting down
    def on_stop(self):
        self.scheduler.stop()
    
    # log the message
    def log(self, log_message):
        # print the message
        print sdk.python.utils.strings.format_log_line(log_message.args, log_message.sender, log_message.get_data())
        # ask db to save the log
        message = Message(self)
        message.recipient = "controller/db"
        message.command = "SAVE_LOG"
        message.args = log_message.args
        message.set_data("["+log_message.sender+"] "+str(log_message.get_data()))
        self.send(message)
        
    # apply configured retention policies for saved logs
    def retention_policies(self):
        # ask the database module to purge the data
        message = Message(self)
        message.recipient = "controller/db"
        message.command = "PURGE_LOGS"
        message.set_data(self.config["retention"])
        self.send(message)
    
    # What to do when receiving a request for this module
    def on_message(self, message):
        # TODO: optionally log to file
        # print out the log message
        if message.command == "LOG":
            # if we are into the same second of the last message, check if not printing too many messages
            if time.time() == self.msg_time:
                self.msg_count = self.msg_count + 1
                if self.msg_count == self.max_msg_rate:
                    print "Too many logs in a short time, suppressing output"
                    return
                if self.msg_count > self.max_msg_rate:
                    return
            else:
                self.msg_time = time.time()
                self.msg_count = 1
            # log the line
            self.log(message)

     # What to do when receiving a new/updated configuration for this module    
    def on_configuration(self, message):
        # TODO: add configuration (db yes/no, etc.)
        pass