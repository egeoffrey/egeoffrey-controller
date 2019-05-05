### service/mqtt: interact with sensors through a mqtt broker
## HOW IT WORKS: 
## DEPENDENCIES:
# OS: 
# Python: paho-mqtt
## CONFIGURATION:
# required: hostname, port
# optional: username, password
## COMMUNICATION:
# INBOUND: 
# - OUT: 
#   required: topic, value
#   optional: 
# OUTBOUND:
# - controller/hub IN: 
#   required: topic
#   optional: 

import paho.mqtt.client as mqtt

from sdk.module.service import Service
from sdk.module.helpers.message import Message

import sdk.utils.exceptions as exception

class Mqtt(Service):
    # What to do when initializing
    def on_init(self):
        # TODO: reusing sdk mqtt class?
        # configuration
        self.config = {}
        # map sensor_id with service configuration
        self.sensors = {}
        # track the topics subscribed
        self.topics_to_subscribe = []
        self.topics_subscribed = []
        # mqtt object
        self.client = mqtt.Client()
        self.mqtt_connected = False
        # require configuration before starting up
        self.add_configuration_listener(self.fullname, True)
        self.add_configuration_listener("sensors/#")
        
    # What to do when running
    def on_start(self):
        # receive callback when conneting
        def on_connect(client, userdata, flags, rc):
            if rc == 0:
                self.log_debug("Connected to the MQTT gateway ("+str(rc)+")")
                # subscribe to previously queued topics
                for topic in self.topics_to_subscribe:
                    self.subscribe_topic(topic)
                self.topics_to_subscribe = []
                self.mqtt_connected = True
            
        # receive a callback when receiving a message
        def on_message(client, userdata, msg):
            # find the sensor matching the topic
            for sensor_id in self.sensors:
                sensor = self.sensors[sensor_id]
                if mqtt.topic_matches_sub(sensor["topic"], msg.topic):
                    self.log_debug("received "+str(msg.payload)+" for "+sensor_id+" on topic "+str(msg.topic))
                    # prepare the message
                    message = Message(self)
                    message.recipient = "controller/hub"
                    message.command = "IN"
                    message.args = sensor_id
                    message.set("value", msg.payload)
                    # send the measure to the controller
                    self.send(message)
            
        # connect to the gateway
        try: 
            self.log_info("Connecting to MQTT gateway on "+self.config["hostname"]+":"+str(self.config["port"]))
            if "username" in self.config and "password" in self.config: self.client.username_pw_set(self.config["username"], password=self.config["password"])
            self.client.connect(self.config["hostname"], self.config["port"], 60)
        except Exception,e:
            self.log_warning("Unable to connect to the MQTT gateway: "+exception.get(e))
            return
        # set callbacks
        self.client.on_connect = on_connect
        self.client.on_message = on_message
        # start loop (in the background)
        # TODO: reconnect
        try: 
            self.client.loop_start()
        except Exception,e: 
            self.log_error("Unexpected runtime error: "+exception.get(e))
    
    # What to do when shutting down
    def on_stop(self):
        self.client.loop_stop()
        self.client.disconnect()
        
    # What to do when receiving a request for this module
    def on_message(self, message):
        sensor_id = message.args
        if message.command == "OUT":
            if not self.mqtt_connected: return
            if not self.is_valid_configuration(["topic", "value"], message.get_data()): return
            topic = message.get("topic")
            data = message.get("value")
            # send the message
            self.log_info("sending message "+str(data)+" to "+topic)
            self.client.publish(topic, str(data))

    # subscribe to a mqtt topic
    def subscribe_topic(self, topic):
        self.log_debug("Subscribing to the MQTT topic "+topic)
        self.topics_subscribed.append(topic)
        self.client.subscribe(topic)      

    # What to do when receiving a new/updated configuration for this module    
    def on_configuration(self,message):
        # module's configuration
        if message.args == self.fullname:
            if not self.is_valid_module_configuration(["hostname", "port"], message.get_data()): return
            self.config = message.get_data()
        # sensors to register
        elif message.args.startswith("sensors/"):
            sensor_id = message.args.replace("sensors/","")
            sensor = message.get_data()
            # a sensor has been deleted
            if message.is_null:
                if sensor_id in self.sensors: 
                    sensor = self.sensors[sensor_id]
                    # unsubscribe from the topic
                    self.client.unsubscribe(sensor["topic"])
                    # delete the sensor
                    del self.sensors[sensor_id]
            # a sensor has been added/updated
            else: 
                # filter in only relevant sensors
                # TODO: certificate, client_id, ssl
                if "service" not in sensor or sensor["service"]["name"] != self.name or sensor["service"]["mode"] != "passive": return
                configuration = sensor["service"]["configuration"]
                if not self.is_valid_configuration(["topic"], configuration): return
                # keep track of the sensor's configuration
                self.sensors[sensor_id] = configuration
                # subscribe to the topic if connected, otherwise queue the request
                if self.mqtt_connected: self.subscribe_topic(configuration["topic"])
                else: self.topics_to_subscribe.append(configuration["topic"])
                self.log_info("registered sensor "+sensor_id)