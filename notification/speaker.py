### play a notification out loud through an attached speaker
## HOW IT WORKS: 
## DEPENDENCIES:
# OS: mpg123
# Python: 
## CONFIGURATION:
# required: device, engine (picotts|google)
# optional: 
## COMMUNICATION:
# INBOUND: 
# - NOTIFY: receive a notification request
# OUTBOUND: 

from sdk.module.notification import Notification

import sdk.utils.command
import sdk.utils.exceptions as exception

class Speaker(Notification):
    # What to do when initializing
    def on_init(self):
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
        
    # play an audio file
    def play(self, filename):
        device = "-t alsa "+str(self.config["device"]) if self.config["device"] != "" else ""
        self.log_debug(sdk.utils.command.run("play "+filename+" "+device, background=False))

   # What to do when ask to notify
    def on_notify(self, severity, text):
        # TODO: queue if already playing something
        self.log_debug("Saying: "+text)
        output_file = "/tmp/audio_output.wav"
        # use the picotts engine
        if self.config["engine"] == "picotts": 
            # create the wav file
            self.log_debug(sdk.utils.command.run(["pico2wave", "-l", self.house["language"], "-w", output_file, text], shell=False))
            # play it
            self.play(output_file)
        # use the google API
        elif self.config["engine"] == "google": 
            # create the wav file
            self.log_debug(sdk.utils.command.run(["gtts-cli", "-l", self.house["language"], "-o", output_file+".mp3", text], shell=False))
            self.log_debug(sdk.utils.command.run(["mpg123", "-w", output_file, output_file+".mp3"], shell=False))
            # play it
            self.play(output_file)

     # What to do when receiving a new/updated configuration for this module    
    def on_configuration(self, message):
        if message.args == "house":
            if not self.is_valid_module_configuration(["language"], message.get_data()): return False
            self.house = message.get_data()
        # module's configuration
        if message.args == self.fullname:
            if not self.is_valid_module_configuration(["device", "engine"], message.get_data()): return False
            self.config = message.get_data()