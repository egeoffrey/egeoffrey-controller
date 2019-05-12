### capture voice commands and respond accordingly
## HOW IT WORKS: 
## DEPENDENCIES:
# OS:
# Python: SpeechRecognition
## CONFIGURATION:
# required: device, engine (google|pocketsphinx), speaker
# optional: 
## COMMUNICATION:
# INBOUND: 
# OUTBOUND: 
# - notification/speaker RUN: respond to the voice command through the speaker
# - controller/chatbot ASK: ask the chatbot how to respond to the command

import speech_recognition

from sdk.module.interaction import Interaction
from sdk.module.helpers.message import Message

import sdk.utils.exceptions as exception
import sdk.utils.command
import sdk.utils.numbers

# listen from voice input through a microphone
class Microphone(Interaction):
    # What to do when initializing
    def on_init(self):
        self.verbose = True
        self.recorder_max_duration = 60
        self.recorder_start_duration = 0.1
        self.recorder_start_threshold = 1
        self.recorder_end_duration = 3
        self.recorder_end_threshold = 0.1
        # module's configuration
        self.config = {}
        self.house = {}
        # request required configuration files
        self.add_configuration_listener("house", True)
        self.add_configuration_listener(self.fullname, True)
        
    # What to do when running
    def on_start(self):
        input_file = "/tmp/audio_input.wav"
        listening_message = True
        while True:
            if listening_message: self.log_info("Listening for voice commands...")
            # run sox to record a voice sample trimming silence at the beginning and at the end
            device = "-t alsa "+str(self.config["device"]) if self.config["device"] != "" else ""
            command = "sox "+device+" "+input_file+" trim 0 "+str(self.recorder_max_duration)+" silence 1 "+str(self.recorder_start_duration)+" "+str(self.recorder_start_threshold)+"% 1 "+str(self.recorder_end_duration)+" "+str(self.recorder_end_threshold)+"%"
            sdk.utils.command.run(command)
            # ensure the sample contains any sound
            max_amplitude = sdk.utils.command.run("killall sox 2>&1 2>/dev/null; sox "+input_file+" -n stat 2>&1|grep 'Maximum amplitude'|awk '{print $3}'")
            if not sdk.utils.numbers.is_number(max_amplitude) or float(max_amplitude) == 0: 
                listening_message = False
                continue
            self.log_info("Captured voice sample, processing...")
            listening_message = True
            # recognize the speech
            request = ""
            if self.config["engine"] == "google":
                # use the speech recognition engine to make google recognizing the file
                recognizer = speech_recognition.Recognizer()
                # open the input file
                with speech_recognition.AudioFile(input_file) as source:
                    audio = recognizer.record(source)
                try:
                    # perform the speech recognition
                    results = recognizer.recognize_google(audio, show_all=True, language=self.house["language"])
                    # identify the best result
                    if len(results) != 0:
                        best_result = max(results["alternative"], key=lambda alternative: alternative["confidence"])
                        request = best_result["transcript"]
                except speech_recognition.UnknownValueError:
                    self.log_warning("Google Speech Recognition could not understand the audio")
                except speech_recognition.RequestError as e:
                    self.log_warning("Could not request results from Google Speech Recognition module; {0}".format(e))
            elif self.config["engine"] == "pocketsphinx":
                # run pocketsphinx to recognize the speech in the audio file
                language = self.house["language"].replace("-","_")
                command = "pocketsphinx_continuous -infile "+input_file+" -hmm /usr/share/pocketsphinx/model/hmm/"+language+"/hub4wsj_sc_8k/ -dict /usr/share/pocketsphinx/model/lm/"+language+"/cmu07a.dic 2>/dev/null"
                output = sdk.utils.command.run(command)
                request = output.replace("000000000: ","")
            if self.debug:
                # repeat the question
                message = Message(self)
                message.recipient = "notification/"+self.config["speaker"]
                message.command = "RUN"
                message.args = "info"
                message.set_data("I have understood: "+request)
                self.send(message)
            # ask the chatbot what to respond
            message = Message(self)
            message.recipient = "controller/chatbot"
            message.command = "ASK"
            message.set("request", request)
            message.set("accept", ["text"])
            self.send(message)
        
    # What to do when shutting down
    def on_stop(self):
        sdk.utils.command.run("killall sox 2>&1 2>/dev/null")
        
    # What to do when receiving a request for this module    
    def on_message(self, message):
        # handle response from the chatbot
        if message.sender == "controller/chatbot" and message.command == "ASK":
            content = message.get("content")
            message = Message(self)
            message.recipient = "notification/"+self.config["speaker"]
            message.command = "RUN"
            message.args = "info"
            message.set_data(content)
            self.send(message)
        
     # What to do when receiving a new/updated configuration for this module    
    def on_configuration(self, message):
        if message.args == "house":
            if not self.is_valid_module_configuration(["language"], message.get_data()): return False
            self.house = message.get_data()
        # module's configuration
        if message.args == self.fullname:
            if not self.is_valid_module_configuration(["device", "engine", "speaker"], message.get_data()): return False
            self.config = message.get_data()