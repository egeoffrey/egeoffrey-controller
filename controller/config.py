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

from sdk.python.module.controller import Controller
from sdk.python.module.helpers.message import Message

import sdk.python.constants as constants
import sdk.python.utils.exceptions as exception

class Config(Controller):
    # What to do when initializing    
    def on_init(self):
        # variables
        self.config_dir = os.getenv("MYHOUSE_CONFIG_DIR", os.path.abspath(os.path.dirname(__file__))+"/../config")
        self.log_debug("Configuration directory set to "+self.config_dir)
        self.force_reload = int(os.getenv("MYHOUSE_CONFIG_FORCE_RELOAD", 0))
        self.accept_default_config = int(os.getenv("MYHOUSE_CONFIG_ACCEPT_DEFAULTS", 1))
        # keep track of the old config index
        self.old_index = None
        # status flags
        self.load_config_running = False
        self.reload_config = False
        self.clear_config_running = False
    
    # return a hash
    def get_hash(self, content):
        hasher = hashlib.md5()
        hasher.update(content)
        return hasher.hexdigest()
    
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
                # read the file's content
                with open(file) as f: content = f.read()
                # ensure the yaml file is valid
                try:
                    yaml.load(content, Loader=yaml.SafeLoader)
                except Exception,e: 
                    self.log_warning("configuration file in an invalid YAML format: "+filename+" - "+exception.get(e))
                    continue
                # update the index with the corresponding hash
                index[topic] = { "file": file, "hash": self.get_hash(content) }
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
    def clear_config(self, topic):
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
        # avoid running this function multiple times in parallel (e.g. while starting and receiving updates), queue a reload
        if self.load_config_running:
            self.reload_config = True
            return
        self.load_config_running = True
        # 1) build an index of the current configuration
        new_index = self.build_index()
        # 2) request the old index
        if self.old_index is None: 
            listener = self.add_configuration_listener("__index")
            # sleep by continuously checking if the index has been received
            dec = 0
            while (dec <= 20): 
                if self.old_index: break
                self.sleep(0.1)
                dec = dec+1
            #self.remove_listener(listener)
        # 3) if there is no old configuration, better clearing up the entire retained configuration
        if not self.old_index or self.force_reload:
            self.log_info("clearing up retained configuration")
            self.clear_config_running = True
            # subscribe for receiving all the configurations
            listener = self.add_configuration_listener("#")
            # clear configuration happening in on_configuration() while sleeping
            self.sleep(5)
            self.remove_listener(listener)
            self.clear_config_running = False
            self.old_index = {}
        # reset force
        if self.force_reload: self.force_reload = False
        # 4) publish new/updated configuration files only
        for topic in new_index:
            # if the file was also in the old index and has the same hash, skip it
            if topic in self.old_index and new_index[topic]["hash"] == self.old_index[topic]["hash"]: continue
            # otherwise read the file and publish it
            with open(new_index[topic]["file"]) as f: file = f.read()
            content = yaml.load(file, Loader=yaml.SafeLoader)
            # TODO: how to keep order of configuration file
            self.publish_config(topic, content)
        # 5) delete removed configuration files
        for topic in self.old_index:
            # topic is in old but not in new index
            if topic not in new_index:
                self.clear_config(topic)
        # 6) publish/update the new index on the bus
        self.clear_config("__index")
        self.publish_config("__index", new_index)
        self.old_index = new_index
        self.log_info("Configuration successfully published")
        self.load_config_running = False
        # if a reload was queued, reload the config
        if self.reload_config: 
            self.reload_config = False
            self.load_config()
    
    # delete a configuration file
    def delete_config_file(self, file):
        # ensure filename is valid
        if ".." in file:
            self.log_warning("invalid file "+file)
            return
        file_path = self.config_dir+os.sep+file+".yml"
        # ensure the file exists
        if not os.path.isfile(file_path):
            self.log_warning(file_path+" does not exist")
            return
        # delete the file
        os.remove(file_path)
        self.log_info("Successfully deleted file "+file)
        # reload the service (a bug in mqtt prevent from unsubscribing and subscribing to the same topic again)
        #self.watchdog.restart_module(self.fullname)
        self.load_config()
    
    # save a new/updated configuration file
    def save_config_file(self, file, data, reload_after_save=True):
        # ensure filename is valid
        if ".." in file:
            self.log_warning("invalid file "+file)
            return
        content = None
        # ensure the file is in the correct format
        try:
            content = yaml.safe_dump(data, default_flow_style=False)
        except Exception,e: 
            self.log_warning("unable to save "+file+", invalid YAML format: "+str(data)+" - "+exception.get(e))
            return
        if content is None: return
        # create subdirectories
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
        self.log_info("Saved configuration file "+file)
        # restart the module or just reload the configuration
        if reload_after_save: 
            self.load_config()
    
    # What to do when running
    def on_start(self):
        # TODO: handle reconnection. cannot be moved into on_connect or will not work
        self.load_config()
        # receive manifest files with default config
        self.add_broadcast_listener("+/+", "MANIFEST", "#")
        
    # What to do when shutting down
    def on_stop(self):
        pass

    # what to do when connecting
    def on_connect(self):
        pass
        
    # What to do when receiving a request for this module    
    def on_message(self, message):
        # update/save a configuration file
        if message.command == "SAVE":
            #self.save_config_file(message.args, message.get_data())
            self.save_config_file(filename, file_content)
        # delete a configuration file
        elif message.command == "DELETE":
            self.delete_config_file(message.args)
        # receive manifest file
        elif message.command == "MANIFEST":
            if message.is_null: return
            manifest = message.get_data()
            self.log_debug("Received manifest from "+message.sender)
            if not self.accept_default_config or self.force_reload or not "default_config" in manifest: return
            # if there is a default configuration in the manifest file, save it
            default_config = manifest["default_config"]
            for entry in default_config:
                for filename in entry:
                    file_content = entry[filename]
                    # ensure the file is in the correct format
                    try:
                        content = yaml.safe_dump(file_content, default_flow_style=False)
                    except Exception,e: 
                        self.log_warning("unable to save "+filename+", invalid YAML format: "+str(file_content)+" - "+exception.get(e))
                        return
                    # ignore existing files whose content hasn't changed
                    if self.old_index is not None and filename in self.old_index and self.old_index[filename]["hash"] == self.get_hash(content): 
                        continue
                    # save the new/updated default configuration
                    self.log_debug("Received new default configuration file "+filename)
                    # TODO: wait a bit before reloading
                    self.save_config_file(filename, file_content, False)
                    self.load_config()

     # What to do when receiving a new/updated configuration for this module    
    def on_configuration(self, message):
        # received old index
        if message.args == "__index":
            self.old_index = message.get_data()
            return
        # if receiving other configurations, we probably are in a clear configuration phase, delete them all
        if self.clear_config_running:
            # prevent a loop with the message below
            if message.get_data() == "" or message.is_null or message.recipient == "controller/logger": return 
            self.clear_config(message.args)
            
