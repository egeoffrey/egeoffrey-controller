### make a phone call through an attached serial device (just a few rings)
## HOW IT WORKS: 
## DEPENDENCIES:
# OS:
# Python: pyserial
## CONFIGURATION:
# required: port, baud, to
# optional: 
## COMMUNICATION:
# INBOUND: 
# - NOTIFY: receive a notification request
# OUTBOUND: 

import serial
from curses import ascii

from sdk.module.notification import Notification

import sdk.utils.exceptions as exception

class Gsm_call(Notification):
    # What to do when initializing
    def on_init(self):
        # constants
        self.timeout = 30
        self.duration = 15
        # configuration settings
        self.house = {}
        # require configuration before starting up
        self.add_configuration_listener("house", True)

    # What to do when running
    def on_start(self):
        pass
        
    # What to do when shutting down
    def on_stop(self):
        pass

    # make a call
    def make_call(self, modem, to):
        self.log_debug("Calling "+str(to))
        self.sleep(2)
        # place the call
        modem.write(b'ATD'+str(to)+'\r')
        # make the phone ring for the configured tie
        self.sleep(self.duration)
        # hung up 
        modem.write(b'ATH\r')
        
   # What to do when ask to notify
    def on_notify(self, severity, text):
        # connect to the modem
        try:
            self.log_debug("Connecting to GSM modem on port "+self.config["port"]+" with baud rate "+str(self.config["baud"]))
            modem = serial.Serial(self.config["port"], self.config["baud"], timeout=0.5)
        except Exception,e:
            self.log_error("Unable to connect to the GSM modem: "+exception.get(e))
            return
        # for each recipient
        for to in self.config["to"]:
            try: 
                i = self.timeout
                done = False
                while True:
                    # place the call
                    if i == 30: self.make_call(modem, to)
                    # read the output
                    output = modem.readlines()
                    for line in output:
                        line = str(line).rstrip()
                        if line == "": continue
                        self.log_debug("Modem output: "+line)
                        if '"SOUNDER",0' in line or '"CALL",0' in line:
                            self.log_info("Called "+str(to))
                            done = True
                        if "ERROR" in line:
                            done = True
                            break
                    if done: break
                    i = i - 1
                    if i == 0:
                        # timeout reached
                        self.log_error("Unable to call "+str(to)+": timeout reached")
                        break
            except Exception,e:
                self.log_error("Failed to send SMS to "+str(to)+": "+exception.get(e))
        # disconect
        modem.close()

     # What to do when receiving a new/updated configuration for this module    
    def on_configuration(self, message):
        if message.args == "house":
            if not self.is_valid_module_configuration(["name"], message.get_data()): return False
            self.house = message.get_data()
        # module's configuration
        if message.args == self.fullname:
            if not self.is_valid_module_configuration(["port", "baud", "to"], message.get_data()): return False
            self.config = message.get_data()