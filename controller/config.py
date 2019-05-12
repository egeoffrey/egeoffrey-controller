### controller/config: makes the entire configuration available on the message bus for the other modules
## HOW IT WORKS: 
## DEPENDENCIES:
# OS:
# Python: pyyaml
## CONFIGURATION:
# required: 
# optional: 
## COMMUNICATION:
# INBOUND: 
# - SAVE: save a new/updated configuration file
# - DELETE: delete a configuration file
# OUTBOUND: 

import os
import time
import hashlib
import yaml

from sdk.module.controller import Controller
from sdk.module.helpers.message import Message

import sdk.constants as constants
import sdk.utils.exceptions as exception

class Config(Controller):
    # What to do when initializing    
    def on_init(self):
        # TODO: when disconnect re-publish the configuration
        self.config_dir = os.getenv("MYHOUSE_CONFIG_DIR", os.path.abspath(os.path.dirname(__file__))+"/../config")
        self.force_reload = int(os.getenv("MYHOUSE_CONFIG_FORCE_RELOAD", 0))
        self.old_index = {}
        self.clear_config = False
        self.log_debug("Configuration directory set to "+self.config_dir)
    
    # read all the files in the configuration directory and returns and an index with filename and hash
    def build_index(self):
        if not os.path.isdir(self.config_dir): return 
        index = {}
        # walk through the filesystem containing the house configuration
        for (current_path, dirnames, filenames) in os.walk(self.config_dir): 
            for filename in filenames:
                if filename[0] == ".": 
                    self.log_debug("ignoring hidden file: "+filename)
                    continue # skip files beginning with a dot
                file = current_path+os.sep+filename
                # parse the file paths
                name, extension = os.path.splitext(file)
                if extension != ".yml": 
                    self.log_warning("ignoring file with invalid extension: "+filename)
                    continue # skip files with invalid extensions
                # remove base configuration dir to build the topic
                topic = name.replace(self.config_dir+os.sep,"")
                # read the file's content and update the index with the corresponding hash
                with open(file) as f: content = f.read()
                try:
                    yaml.load(content, Loader=yaml.SafeLoader)
                except Exception,e: 
                    self.log_warning("configuration file in an invalid YAML format: "+filename+" - "+exception.get(e))
                    continue
                hasher = hashlib.md5()
                hasher.update(content)
                index[topic] = { "file": file, "hash": hasher.hexdigest() }
        return index
    
    # publish a configuration file
    def publish_config(self, topic, content):
        if topic != "__index": self.log_debug("Publishing configuration "+topic)
        message = Message(self)
        message.recipient = "*/*"
        message.command = "CONF"
        message.args = topic
        message.set_data(content)
        # configuration is retained so when a module connects, immediately get the latest config
        message.retain = True 
        self.send(message)
    
    # delete a retained message from the bus
    def delete_config(self, topic):
        # send a null so to cancel retention
        if topic != "__index": self.log_debug("Removing configuration "+topic)
        message = Message(self)
        message.recipient = "*/*"
        message.command = "CONF"
        message.args = topic
        # remove the retained message
        message.set_null()
        message.retain = True
        self.send(message)
        
    # load and publish the current configuration
    def load_config(self):
        # 1) build an index of the current configuration
        new_index = self.build_index()
        # 2) request the old index
        self.old_index = {}
        listener = self.add_configuration_listener("__index")
        # sleep by continuously check if the index has been received
        dec = 0
        while (dec <= 20): 
            if self.old_index: break
            time.sleep(0.1)
            dec = dec+1
        self.remove_listener(listener)
        # 3) if there is no old configuration, better clearing up the entire retained configuration
        if not self.old_index or self.force_reload:
            self.log_debug("clearing up retained configuration")
            self.clear_config = True
            # subscribe for receiving all the configurations
            listener = self.add_configuration_listener("#") 
            time.sleep(2)
            self.remove_listener(listener)
            self.clear_config = False
            self.old_index = {}
        # 4) publish new/updated configuration files
        for topic in new_index:
            # if the file was also in the old index and has the same hash, skip it
            if topic in self.old_index and new_index[topic]["hash"] == self.old_index[topic]["hash"]: continue
            # read the file and publish it
            with open(new_index[topic]["file"]) as f: file = f.read()
            content = yaml.load(file, Loader=yaml.SafeLoader)
            # TODO: how to keep order of configuration file
            self.publish_config(topic, content)
        # 5) delete removed configuration files
        for topic in self.old_index:
            if topic not in new_index:
                self.delete_config(topic)
        # 6) publish/update the new index on the bus
        self.delete_config("__index")
        self.publish_config("__index", new_index)
        self.log_info("Configuration successfully published")
    
    # What to do when running
    def on_start(self):
        self.load_config()
        
    # What to do when shutting down
    def on_stop(self):
        pass

    # What to do when receiving a request for this module    
    def on_message(self, message):
        # update/save a configuration file
        if message.command == "SAVE":
            file = message.args
            # ensure filename is valid
            if ".." in file:
                self.log_warning("invalid file "+file)
                return
            content = None
            # ensure the file is in the correct format
            try:
                content = yaml.safe_dump(message.get_data(), default_flow_style=False)
            except Exception,e: 
                self.log_warning("unable to save "+file+", invalid YAML format: "+message.dump()+" - "+exception.get(e))
                return
            if content is None: return
            # handle subdirectories
            filename = os.path.basename(file)
            directories = os.path.dirname(file)
            if directories != "":
                path = self.config_dir+os.sep+directories
                if not os.path.exists(path):
                    try:
                        os.makedirs(path)
                    except Exception,e: 
                        self.log_error("unable to create directory "+path+": "+exception.get(e))
                        return
            # save the file
            file_path = self.config_dir+os.sep+file+".yml"
            f = open(file_path, "w")
            f.write(content)
            f.close()
            self.log_info("Saved configuration file "+file+" as per "+message.sender+" request")
            # reload the service (a bug in mqtt prevent from unsubscribing and subscribing to the same topic again)
            self.watchdog.restart_module(self.fullname)
        # delete a configuration file
        elif message.command == "DELETE":
            file = message.args
            # ensure filename is valid
            if ".." in file:
                self.log_warning("invalid file "+file)
                return
            file_path = self.config_dir+os.sep+file+".yml"
            # ensure the file exists
            if not os.path.isfile(file_path):
                self.log_warning(file_path+" does not exist")
                return
            os.remove(file_path)
            self.log_info("Successfully deleted file "+file)
            # reload the service (a bug in mqtt prevent from unsubscribing and subscribing to the same topic again)
            self.watchdog.restart_module(self.fullname)

     # What to do when receiving a new/updated configuration for this module    
    def on_configuration(self, message):
        # received old index
        if message.args == "__index":
            self.old_index = message.get_data()
            return
        # if receving other configurations, we probably are in a clear configuratio file, delete them all
        if self.clear_config:
            # prevent a loop with the message below
            if message.get_data() == "" or message.is_null or message.recipient == "controller/logger": return 
            self.delete_config(message.args)
            
