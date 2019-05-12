### send a notification through slack
## HOW IT WORKS: 
## DEPENDENCIES:
# OS:
# Python: slackclient
## CONFIGURATION:
# required: bot_token, bot_name, token
# optional: 
## COMMUNICATION:
# INBOUND: 
# - NOTIFY: receive a notification request
# OUTBOUND: 

from slackclient import SlackClient

from sdk.module.notification import Notification

import sdk.utils.exceptions as exception

class Slack(Notification):
    # What to do when initializing
    def on_init(self):
        # variables
        self.slack = None
        self.slack_initialized = False
        self.channel_id = None
        self.bot_id = None

    # What to do when running
    def on_start(self):
        self.slack_init()
        
    # What to do when shutting down
    def on_stop(self):
        pass
        
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
            
    # send a message to slack
    def slack_message(self, message):
        # TODO: image
        try:
            self.slack.api_call("chat.postMessage", channel=self.channel_id, text=message, as_user=True)	
        except Exception,e:
            self.log_warning("unable to post message to slack: "+exception.get(e))
        
   # What to do when ask to notify
    def on_notify(self, severity, text):
        if not self.slack_initialized: return
        self.log_debug("saying: "+text)
        self.slack_message(text)

     # What to do when receiving a new/updated configuration for this module    
    def on_configuration(self, message):
        # module's configuration
        if message.args == self.fullname:
            if not self.is_valid_module_configuration(["bot_token", "bot_name", "channel"], message.get_data()): return False
            self.config = message.get_data()