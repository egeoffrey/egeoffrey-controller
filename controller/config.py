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
import re
import datetime

import paho.mqtt.client as mqtt

from sdk.python.module.controller import Controller
from sdk.python.module.helpers.scheduler import Scheduler
from sdk.python.module.helpers.message import Message

import sdk.python.utils.exceptions as exception

class Config(Controller):
    # What to do when initializing    
    def on_init(self):
        # variables
        self.config_dir = os.getenv("EGEOFFREY_CONFIG_DIR", os.path.abspath(os.path.dirname(__file__))+"/../config")
        self.log_debug("Configuration directory set to "+self.config_dir)
        self.force_reload = int(os.getenv("EGEOFFREY_CONFIG_FORCE_RELOAD", 0))
        self.force_reload_timeout = int(os.getenv("EGEOFFREY_CONFIG_FORCE_RELOAD_TIMEOUT", 10))
        self.accept_default_config = int(os.getenv("EGEOFFREY_CONFIG_ACCEPT_DEFAULTS", 1))
        # keep track of the old config index
        self.index = None
        self.index_key = "__index"
        self.index_version = 1
        self.supported_manifest_schema = 2
        # scheduler is used for scheduling config reload
        self.scheduler = Scheduler(self)
        # keep track of received manifests
        self.manifests = {}
        # status flags
        self.load_config_running = False
        self.clear_config_running = False
        self.is_config_online = False
        self.reload_scheduled = False
    
    # return a hash
    def get_hash(self, content):
        hasher = hashlib.md5()
        hasher.update(content)
        return hasher.hexdigest()
        
    # split filename from version given a filename
    def parse_filename(self, filename):
        match = re.match("^(.+)\.(\d)$", filename)
        if match is None: return None
        else: return match.groups()
        
    # split filename from version given a topic
    def parse_topic(self, topic):
        match = re.match("^(\d)\/(.+)$", topic)
        if match is None: return None
        else: return match.groups()
    
    # read all the files in the configuration directory and returns and an index with filename and hash
    def build_index(self):
        if not os.path.isdir(self.config_dir): return 
        index = {}
        # walk through the filesystem containing the house configuration
        for (current_path, dirnames, filenames) in os.walk(self.config_dir): 
            for filename in filenames:
                # skip files beginning with a dot
                if filename[0] == ".": 
                    continue 
                file = current_path+os.sep+filename
                # parse the file paths
                name, extension = os.path.splitext(file)
                # skip files with invalid extensions
                if extension != ".yml": 
                    continue 
                # ensure the filename contains the version and retrieve it
                if self.parse_filename(name) is None:
                    self.log_warning("ignoring "+filename+" since version is invalid")
                    continue
                name_without_version, version = self.parse_filename(name)
                # read the file's content
                with open(file) as f: content = f.read()
                # ensure the yaml file is valid
                try:
                    content_data = yaml.load(content, Loader=yaml.SafeLoader)
                except Exception,e: 
                    self.log_warning("configuration file in an invalid YAML format: "+filename+" - "+exception.get(e))
                    continue
                # remove base configuration dir to build the topic and append version number
                topic = name_without_version.replace(self.config_dir+os.sep,"")
                # if there are multiple versions of the file, only publish the latest
                if topic in index:
                    if version < index[topic]["version"]: continue
                # update the index with the corresponding hash    
                index[topic] = { "file": file, "version": version, "hash": self.get_hash(content) }
                # if config is not retained on the bus, also keep track of the entire content
                if self.gateway_version >= 2:
                    index[topic]["content"] = content_data
        return index
    
    # publish a configuration file
    def publish_config(self, filename, version, content, recipient="*/*", retain=True):
        if filename != self.index_key: 
            self.log_debug("Publishing configuration "+filename+" (v"+str(version)+") for "+recipient)
        message = Message(self)
        message.recipient = recipient
        message.command = "CONF"
        message.args = filename
        message.config_schema = version
        message.set_data(content)
        # configuration is retained so when a module connects, immediately get the latest config
        message.retain = retain 
        self.send(message)
    
    # delete a retained message from the bus
    def clear_config(self, filename, version):
        # send a null so to cancel retention
        if filename != self.index_key: 
            self.log_debug("Unpublishing configuration "+filename+" (v"+str(version)+")")
        message = Message(self)
        message.recipient = "*/*"
        message.command = "CONF"
        message.args = filename
        message.config_schema = version
        # remove the retained message
        message.set_null()
        message.retain = True
        self.send(message)
        
    def reload_config(self):
        # schedule to reload the configuration in a few seconds (so to allow other consecutive changes to happen)
        if self.reload_scheduled: return
        job = {}
        job["trigger"] = "date"
        job["run_date"] = datetime.datetime.now() + datetime.timedelta(seconds=5)
        job["func"] = self.load_config
        self.scheduler.add_job(job)
        self.reload_scheduled = True
        
    # load and publish the current configuration
    def load_config(self):
        # avoid running this function multiple times in parallel (e.g. while starting and receiving updates)
        if self.load_config_running: return
        self.load_config_running = True
        # 1) build an index of the current configuration
        new_index = self.build_index()
        if new_index is None:
            self.log_error("Unable to load configuration from "+self.config_dir)
            return
        # if the configuration is retained on the bus, the old index is there as well, get it for comparing what has changed
        if self.gateway_version == 1:
            # 2) request the old index from the bus
            if self.index is None and not self.force_reload: 
                listener = self.add_configuration_listener(self.index_key, self.index_version)
                # sleep by continuously checking if the index has been received
                dec = 0
                while (dec <= 20): 
                    if self.index: break
                    self.sleep(0.1)
                    dec = dec+1
                # index received from the bus, remove listener
                self.remove_listener(listener)
            # 3) if there is no old configuration, better clearing up the entire retained configuration
            if not self.index or self.force_reload:
                self.log_info("clearing up retained configuration")
                self.clear_config_running = True
                # subscribe for receiving all the configurations
                listener = self.add_configuration_listener("#")
                # clear configuration happening in on_configuration() while sleeping
                self.sleep(30)
                self.remove_listener(listener)
                self.clear_config_running = False
                self.index = {}
            # reset force reload flag
            if self.force_reload: 
                self.force_reload = False
        else:
            # just initialize index data strcture
            if self.index is None: 
                self.index = {}
        # 4) publish new/updated configuration files only
        for topic in new_index:
            # if the file was also in the old index with the same version and has the same hash, skip it
            if topic in self.index and new_index[topic]["version"] == self.index[topic]["version"] and new_index[topic]["hash"] == self.index[topic]["hash"]: 
                continue
            if self.gateway_version >= 2 and not self.index: 
                continue
            # otherwise read the file and publish it
            with open(new_index[topic]["file"]) as f: 
                file = f.read()
            content = yaml.load(file, Loader=yaml.SafeLoader)
            self.publish_config(topic, new_index[topic]["version"], content)
        # 5) delete removed configuration files
        for topic in self.index:
            # topic is in old but not in new index
            if topic not in new_index:
                self.clear_config(topic, self.index[topic]["version"])
        # 6) publish/update the new index on the bus
        if self.gateway_version == 1 :
            self.clear_config(self.index_key, self.index_version)
            self.publish_config(self.index_key, self.index_version, new_index)
            self.log_info("Configuration successfully published")
        # save the updated index
        self.index = new_index
        self.load_config_running = False
        self.reload_scheduled = False
    
    # delete a configuration file
    def delete_config_file(self, filename, version):
        # ensure filename is valid
        if ".." in filename:
            self.log_warning("invalid file "+filename)
            return
        file_path = self.config_dir+os.sep+filename+"."+version+".yml"
        # ensure the file exists
        if not os.path.isfile(file_path):
            self.log_warning(file_path+" does not exist")
            return
        # backup existing file
        with open(file_path) as f: backup_content = f.read()
        f = open(file_path.replace(".yml", ".bck"), "w")
        f.write(backup_content)
        f.close()
        # delete the file
        os.remove(file_path)
        self.clear_config(filename, version)
        self.log_info("Successfully deleted file "+filename+" (v"+str(version)+")")
        # reload the configuration
        self.reload_config()
        
    # rename a configuration file
    def rename_config_file(self, from_filename, to_filename, version):
        # validate input
        if ".." in from_filename or ".." in to_filename:
            self.log_warning("invalid file format")
            return
        # define source and destination file paths
        from_file_path = self.config_dir+os.sep+from_filename+"."+version+".yml"
        to_file_path = self.config_dir+os.sep+to_filename+"."+version+".yml"
        # check if the file exists
        if not os.path.isfile(from_file_path):
            self.log_warning(from_filename+" does not exist")
            return
        # rename the file
        try:
            os.rename(from_file_path, to_file_path)
        except Exception,e: 
            self.log_warning("unable to rename "+from_file_path+" into "+to_file_path+": "+exception.get(e))
            return
        self.clear_config(from_filename, version)
        self.log_info("Successfully renamed file "+from_filename+" into "+to_filename+" (v"+str(version)+")")
        # reload the configuration
        self.reload_config()
    
    # save a new/updated configuration file
    def save_config_file(self, file, version, data, reload_after_save=True):
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
        directories = os.path.dirname(file)
        if directories != "":
            path = self.config_dir+os.sep+directories
            if not os.path.exists(path):
                try:
                    os.makedirs(path)
                except Exception,e: 
                    self.log_error("unable to create directory "+path+": "+exception.get(e))
                    return
        # backup existing file if any
        file_path = self.config_dir+os.sep+file+"."+version+".yml"
        if os.path.isfile(file_path):
            with open(file_path) as f: backup_content = f.read()
            f = open(file_path.replace(".yml", ".bck"), "w")
            f.write(backup_content)
            f.close()
        # save the file
        f = open(file_path, "w")
        f.write(content)
        f.close()
        self.log_info("Saved configuration file "+file+" (v"+str(version)+")")
        # restart the module or just reload the configuration
        if reload_after_save: 
            self.reload_config()
    
    # What to do when running
    def on_start(self):
        # ensure config path exists, otherwise create it
        if not os.path.exists(self.config_dir):
            try:
                os.makedirs(self.config_dir)
            except Exception,e: 
                self.log_error("unable to create directory "+self.config_dir+": "+exception.get(e))
        self.load_config()
        # ask for manifest files with default config
        self.add_manifest_listener()
        self.sleep(self.force_reload_timeout)
        while True:
            # periodically ensure there is a configuration available (e.g. to republish if the broker restarts)
            if not self.load_config_running and self.gateway_version == 1:
                # ask for the index
                self.is_config_online = False
                listener = self.add_configuration_listener(self.index_key, self.index_version)
                # sleep by continuously checking if the index has been received
                dec = 0
                while (dec <= 20): 
                    if self.is_config_online: break
                    self.sleep(0.1)
                    dec = dec+1
                self.remove_listener(listener)
                # looks like the index is no more there, better reloading the config
                if not self.is_config_online:
                    self.log_warning("configuration has disappear from the gateway, re-publishing it again")
                    self.force_reload = True
                    self.load_config()
            time.sleep(60)
        
    # What to do when shutting down
    def on_stop(self):
        pass

    # what to do when connecting
    def on_connect(self):
        pass
        
    # What to do when receiving a request for this module    
    def on_message(self, message):
        # requested to update/save a configuration file
        if message.command == "SAVE":
            if self.parse_topic(message.args) is None: return
            version, filename = self.parse_topic(message.args)
            self.save_config_file(filename, version, message.get_data())
        # requested to delete a configuration file
        elif message.command == "DELETE":
            if self.parse_topic(message.args) is None: return
            version, filename = self.parse_topic(message.args)
            self.delete_config_file(filename, version)
        # requested to rename a configuration file
        elif message.command == "RENAME":
            if self.parse_topic(message.args) is None: return
            version, from_filename = self.parse_topic(message.args)
            to_filename = message.get_data()
            self.rename_config_file(from_filename, to_filename, version)
        # a module just subscribed to a configuration topic
        elif message.command == "SUBSCRIBE":
            # if config is not loaded yet, return, the sender will call back later
            if self.load_config_running:
                return
            # split pattern requested from configuration version
            match = re.match("^([^\/]+)\/(.+)$", message.get_data())
            if match is None: return
            version, pattern =  match.groups()
            # check if we have this configuration file by cycling through all the topics
            if self.index is None:
                return
            for topic in self.index:
                # if the pattern subscribed matches the configuration topic
                if mqtt.topic_matches_sub(pattern, topic):
                    # when a service subscribes to all the sensors, just send over those associated with those service to avoid sending out too many messages
                    if self.gateway_version >= 2 and message.sender.startswith("service/") and pattern == "sensors/#":
                        if "service" not in self.index[topic]["content"] or "service/"+self.index[topic]["content"]["service"]["name"] != message.sender:
                            continue
                    # respond to the module (directly) with the requested configuration file
                    self.publish_config(topic, self.index[topic]["version"], self.index[topic]["content"], message.sender, False)
            # ack the subscribe request so the sender module will not re-send the subscribe request message again
            ack_message = Message(self)
            ack_message.recipient = message.sender
            ack_message.command = "SUBSCRIBE_ACK"
            ack_message.set_data(message.get_data())
            self.send(ack_message)
        # receive manifest file (configuration is already loaded), it may contain default configurations
        elif message.command == "MANIFEST":
            if message.is_null: 
                return
            manifest = message.get_data()
            # ensure this is a manifest we can handle
            if manifest["manifest_schema"] != self.supported_manifest_schema: 
                return
            self.log_debug("Received manifest from "+message.sender)
            # if not accepting default configurations or if there are no default config, just return
            if not self.accept_default_config or self.force_reload or not "default_config" in manifest: 
                return
            # ensure we have not already received the same manifest before
            if message.sender in self.manifests and self.manifests[message.sender] == self.get_hash(manifest):
                return
            self.manifests[message.sender] = self.get_hash(manifest)
            # if there is a default configuration in the manifest file, save it
            default_config = manifest["default_config"]
            for entry in default_config:
                for filename_with_version in entry:
                    if self.parse_filename(filename_with_version) is None: return
                    filename, version = self.parse_filename(filename_with_version)
                    file_content = entry[filename_with_version]
                    # do not overwrite existing files since the user may have changed default values
                    # for updated configurations, prevent saving the new version, letting the module managing the upgrade
                    if filename in self.index: 
                        continue
                    # ensure the file is in a valid YAML format
                    try:
                        content = yaml.safe_dump(file_content, default_flow_style=False)
                    except Exception,e:
                        self.log_warning("unable to save "+filename+", invalid YAML format: "+str(file_content)+" - "+exception.get(e))
                        return
                    # save the new/updated default configuration file
                    self.log_debug("Received new default configuration file "+filename)
                    self.save_config_file(filename, version, file_content, False)
                    self.reload_config()

    # What to do when receiving a new/updated configuration for this module    
    def on_configuration(self, message):
        # received old index
        if message.args == "__index":
            self.index = message.get_data()
            self.is_config_online = True
            return
        # if receiving other configurations, we probably are in a clear configuration phase, delete them all
        if self.clear_config_running:
            # prevent a loop with the message below
            if message.get_data() == "" or message.is_null or message.recipient == "controller/logger": return 
            self.clear_config(message.args, message.config_schema)
            
