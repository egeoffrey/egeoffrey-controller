### play a sound through a buzzer connected to a pin
## HOW IT WORKS: 
## DEPENDENCIES:
# OS:
# Python: OPi.GPIO
## CONFIGURATION:
# required: pin, duration
# optional: 
## COMMUNICATION:
# INBOUND: 
# - NOTIFY: receive a notification request
# OUTBOUND: 

import OPi.GPIO as GPIO
import time

from sdk.module.notification import Notification

import sdk.utils.exceptions as exception

class Buzzer_raspi(Notification):
    # What to do when initializing
    def on_init(self):
        pass

    # What to do when running
    def on_start(self):
        # initialize GPIO
        GPIO.setmode(GPIO.BCM)
        GPIO.setwarnings(False)
        GPIO.setup(self.config["pin"], GPIO.OUT)
        
    # What to do when shutting down
    def on_stop(self):
        pass
        
   # What to do when ask to notify
    def on_notify(self, severity, text):
        self.log_debug("activating buzzer on pin "+str(self.config["pin"])+" for "+str(self.config["duration"])+" seconds")
        GPIO.output(self.config["pin"], GPIO.HIGH)
        time.sleep(self.config["duration"])
        GPIO.output(self.config["pin"], GPIO.LOW)

     # What to do when receiving a new/updated configuration for this module    
    def on_configuration(self, message):
        # module's configuration
        if message.args == self.fullname:
            if not self.is_valid_module_configuration(["pin", "duration"], message.get_data()): return
            self.config = message.get_data()