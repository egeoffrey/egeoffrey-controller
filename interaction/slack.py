### connect to a slack channel as a bot and interact with the user
## HOW IT WORKS: 
## DEPENDENCIES:
# OS:
# Python: slackclient
## CONFIGURATION:
# required: bot_token, bot_name, channel
# optional: 
## COMMUNICATION:
# INBOUND: 
# OUTBOUND: 
# - controller/chatbot ASK: ask the chatbot how to respond to the request

import json
import time
import base64
from slackclient import SlackClient

from sdk.module.interaction import Interaction
from sdk.module.helpers.message import Message

import sdk.utils.exceptions as exception

# send a notification through slack
class Slack(Interaction):
    # What to do when initializing
    def on_init(self):
        # constants
        self.debug = True
        self.sleep_on_error = 60
        self.tmp_file = "/tmp/myHouse_slack_image.jpg"
        # variables
        self.slack = None
        self.slack_initialized = False
        self.slack_connected = False
        self.channel_id = None
        self.bot_id = None
        # module's configuration
        self.config = {}
        # request required configuration files
        self.add_configuration_listener(self.fullname, True)

    # return the ID corresponding to the bot name
    def get_user_id(self, username):
        users = self.slack.api_call("users.list")
        if not users.get('ok'): return None
        users = users.get('members')
        for user in users:
            if 'name' in user and user.get('name') == username: return user.get('id')
        return None
        
    # return the ID corresponding to the channel name
    def get_channel_id(self, channelname):
        channels = self.slack.api_call("channels.list")
        if not channels.get('ok'): return None
        channels = channels.get('channels')
        for channel in channels:
            if 'name' in channel and channel.get('name') == channelname: return channel.get('id')
        return None
        
    # initialize the integration
    def slack_init(self):
        if self.slack_initialized: return
        self.log_debug("Initializing slack...")
        # initialize the library
        try:
            # initialize the library
            self.slack = SlackClient(self.config["bot_token"])
            # test the authentication
            auth = self.slack.api_call("auth.test")
            if not auth["ok"]:
                self.log_error("authentication failed: "+auth["error"])
                return 
            # retrieve the bot id
            self.bot_id = self.get_user_id(self.config["bot_name"])
            if self.bot_id is None:
                self.log_error("unable to find your bot "+self.config["bot_name"]+". Ensure it is configured correctly")
                return
            # retrieve the channel id
            self.channel_id = self.get_channel_id(self.config["channel"])
            if self.channel_id is None:
                self.log_error("unable to find the channel "+self.config["channel"])
                return 
            self.slack_initialized = True
        except Exception,e:
            self.log_warning("unable to initialize slack: "+exception.get(e))
    
    # connect to the RTM API
    def slack_connect(self):
        if self.slack_connected: return
        if self.slack.rtm_connect():
            self.log_info("slack bot online ("+self.config["bot_name"]+")")
            self.slack_connected = True
            return
        self.log_error("unable to connect to slack")
            
    # send a message to slack
    def slack_message(self, channel, message):
        if not self.slack_connected: return
        # TODO: image
        try:
            self.slack.api_call("chat.postMessage", channel=channel, text=message, as_user=True)    
        except Exception,e:
            self.log_warning("unable to post message to slack: "+exception.get(e))
            
    # send an image to slack
    def slack_image(self, channel, message, title):
        # save the file first since when using content= the filetype is not inferred correctly
        f = open(self.tmp_file, "w")
        f.write(base64.b64decode(message))
        f.close()
        # upload the file
        try:
            self.slack.api_call("files.upload", channels=channel, file=open(self.tmp_file,'rb'), filename=self.tmp_file, title=title)
        except Exception,e:
            self.log_warning("unable to upload file to slack: "+exception.get(e))
            
    # send a typing message
    def slack_typing(self, channel):
        typing_event = {
            "id": 1,
            "type": "typing",
            "channel": channel
        }
        self.slack.server.send_to_websocket(typing_event)

    
    # What to do when running
    def on_start(self):
        while self.stopping == False:
            # init slack
            self.slack_init()
            if not self.slack_initialized: 
                self.sleep(self.sleep_on_error)
                continue
            # connect to slack
            self.slack_connect()
            if not self.slack_connected: 
                self.sleep(self.sleep_on_error)
                continue            
            # read a rtm stream
            try: 
                output_list = self.slack.rtm_read()
            except Exception,e:
                self.log_warning("unable to read from slack: "+exception.get(e))
                self.slack_initialized = False
                self.slack_connected = False
                self.sleep(self.sleep_on_error)
                continue
            if output_list and len(output_list) > 0:
                for output in output_list:
                    if not output or 'text' not in output: continue
                    if output['user'] == self.bot_id: continue
                    # if the message is to the bot
                    if self.bot_id in output['text'] or self.config["bot_name"] in output['text'] or output['channel'].startswith("D"):
                        # clean up the request
                        request = output['text']
                        request = request.replace(self.config["bot_name"], '')
                        request = request.replace(self.bot_id, '')
                        request = request.lower()
                        channel = output['channel']
                        # ask our chatbot what to respond
                        message = Message(self)
                        message.recipient = "controller/chatbot"
                        message.command = "ASK"
                        message.set("request", request)
                        message.set("accept", ["text","image"])
                        self.sessions.register(message, {
                            "channel": channel
                        })
                        self.send(message)
                        self.slack_typing(channel)
            self.sleep(1)
        
    # What to do when shutting down
    def on_stop(self):
        pass
        
    # What to do when receiving a request for this module    
    def on_message(self, message):
        # handle response from the chatbot
        if message.sender == "controller/chatbot" and message.command == "ASK":
            session = self.sessions.restore(message)
            if session is None: return
            channel = session["channel"]
            type = message.get("type")
            content = message.get("content")
            self.slack_typing(channel)
            if type == "text":
                # post the text response
                self.slack_message(channel, content)
            elif type == "image":
                # upload the image
                self.slack_image(channel, content, message.get("description"))

        
     # What to do when receiving a new/updated configuration for this module    
    def on_configuration(self, message):
        # module's configuration
        if message.args == self.fullname:
            if not self.is_valid_module_configuration(["bot_token", "bot_name", "channel"], message.get_data()): return False
            self.config = message.get_data()