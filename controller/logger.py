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

from datetime import datetime

from sdk.module.controller import Controller

import sdk.utils.strings

class Logger(Controller):
    # What to do when initializing
    def on_init(self):
        pass
        
    # What to do when running    
    def on_start(self):
        pass
        
    # What to do when shutting down
    def on_stop(self):
        pass

    # What to do when receiving a request for this module    
    def on_message(self, message):
        # TODO: optionally log to file 
        # TODO: find a way to prevent loops
        # print out the log message
        if message.command == "LOG":
            print "["+datetime.now().strftime("%Y-%m-%d %H:%M:%S")+"]["+message.house_id+"]["+str(message.sender)+"] "+str(message.args.upper()) + ": "+str(message.get_data())

     # What to do when receiving a new/updated configuration for this module    
    def on_configuration(self, message):
        self.on_message(message)