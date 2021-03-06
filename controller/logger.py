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

import time
import collections
import os
import logging 
import logging.handlers

from sdk.python.module.controller import Controller
from sdk.python.module.helpers.scheduler import Scheduler
from sdk.python.module.helpers.message import Message

import sdk.python.utils.strings
import sdk.python.utils.numbers

class Logger(Controller):
    # What to do when initializing
    def on_init(self):
        # module's configuration
        self.config = {}
        # logger
        self.logger = logging.getLogger("eGeoffrey")
        # maximum log messages per second to print
        self.max_msg_rate = 5
        self.msg_count = 0
        self.msg_time = time.time()
        # message queue
        self.queue = collections.deque(maxlen=50)
        self.is_logging = False
        # scheduler is needed for purging old logs
        self.scheduler = Scheduler(self)
        # require module configuration before starting up
        self.config_schema = 2
        self.add_configuration_listener(self.fullname, "+", True)
        
    # What to do when running    
    def on_start(self):
        if self.config["file_enable"]:
            #configure loggers
            self.logger.setLevel(logging.INFO)
            # configure console logging
            console = logging.StreamHandler()
            console.setLevel(logging.INFO)
            console.setFormatter(logging.Formatter("%(message)s"))
            self.logger.addHandler(console)
            # configure file logging
            log_dir = os.path.abspath(os.path.dirname(__file__))+"/../logs"
            if not os.path.exists(log_dir):
                try:
                    os.makedirs(log_dir)
                except Exception,e: 
                    print "unable to create directory "+log_dir+": "+exception.get(e)
            if os.path.exists(log_dir):
                file = logging.handlers.RotatingFileHandler(log_dir+"/egeoffrey.log", maxBytes=self.config["file_rotate_size"]*1024*1024, backupCount=self.config["file_rotate_count"])
                file.setLevel(logging.INFO)
                file.setFormatter(logging.Formatter("%(message)s"))
                self.logger.addHandler(file)
        if self.config["database_enable"]:
            # schedule to apply configured retention policies to the logs stored into the database
            job = {"func": self.retention_policies, "trigger":"interval", "hours": 1}
            self.scheduler.add_job(job)
            # start the scheduler 
            self.scheduler.start()

        
    # What to do when shutting down
    def on_stop(self):
        self.scheduler.stop()
        
    # log a message
    def __do_log(self, message):
        # print the message
        self.logger.info(sdk.python.utils.strings.format_log_line(message.args, message.sender, message.get_data()))
        # ask db to save the log
        db_message = Message(self)
        db_message.recipient = "controller/db"
        db_message.command = "SAVE_LOG"
        db_message.args = message.args
        db_message.set_data("["+message.sender+"] "+str(message.get_data()))
        self.send(db_message)
    
    # log the message
    def log(self, message):
        # if logging another message, queue this one
        if self.is_logging:
            self.queue.append(message)
            return
        # log this message
        self.is_logging = True
        self.__do_log(message)
        # done logging, check if there were queued messages
        while True:
            try:
                message = self.queue.popleft()
                self.__do_log(message)
            except IndexError:
                break
        # release the lock, ready to log a new message
        self.is_logging = False
        
    # apply configured retention policies for saved logs
    def retention_policies(self):
        # ask the database module to purge the data
        message = Message(self)
        message.recipient = "controller/db"
        message.command = "PURGE_LOGS"
        message.set_data(self.config["database_retention"])
        self.send(message)
    
    # What to do when receiving a request for this module
    def on_message(self, message):
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
        # module's configuration
        if message.args == self.fullname and not message.is_null:
            # upgrade the config schema
            if message.config_schema == 1:
                config = message.get_data()
                config["database_retention"] = config["retention"]
                del config["retention"]
                config["database_enable"] = True
                config["file_enable"] = True
                config["file_rotate_size"] = 5
                config["file_rotate_count"] = 5
                self.upgrade_config(message.args, message.config_schema, 2, config)
                return False
            if message.config_schema != self.config_schema: 
                return False
            self.config = message.get_data()
