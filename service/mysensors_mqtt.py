### service/mysensors_mqtt: interact with a MySensors mqtt broker
## HOW IT WORKS: 
## DEPENDENCIES:
# OS: 
# Python: paho-mqtt
## CONFIGURATION:
# required: "hostname", "port", "subscribe_topic_prefix", "publish_topic_prefix"
# optional: 
## COMMUNICATION:
# INBOUND: 
# - OUT: 
#   required: "node_id", "child_id", "command", "type", "value"
#   optional: 
# OUTBOUND: 
# - controller/hub IN: 
#   required: "node_id", "child_id", "command", "type", "value"
#   optional: 

import time
import Queue
import paho.mqtt.client as mqtt

from sdk.module.service import Service
from sdk.module.helpers.message import Message

import sdk.utils.exceptions as exception

class Mysensors_mqtt(Service):
    # What to do when initializing
    def on_init(self):
        # configuration
        self.config = {}
        self.units = None
        # map sensor_id with service configuration
        self.sensors = {}
        # queue of messages for sleeping nodes
        self.queue = {}
        # gateway object
        self.gateway = None
        self.connected = False
        # constants
        self.commands = ["PRESENTATION","SET","REQ","INTERNAL","STREAM"]
        self.acks = ["NOACK","ACK"]
        self.types = []
        self.types.append(["S_DOOR","S_MOTION","S_SMOKE","S_BINARY","S_DIMMER","S_COVER","S_TEMP","S_HUM","S_BARO","S_WIND","S_RAIN","S_UV","S_WEIGHT","S_POWER","S_HEATER","S_DISTANCE","S_LIGHT_LEVEL","S_ARDUINO_NODE","S_ARDUINO_REPEATER_NODE","S_LOCK","S_IR","S_WATER","S_AIR_QUALITY","S_CUSTOM","S_DUST","S_SCENE_CONTROLLER","S_RGB_LIGHT","S_RGBW_LIGHT","S_COLOR_SENSOR","S_HVAC","S_MULTIMETER","S_SPRINKLER","S_WATER_LEAK","S_SOUND","S_VIBRATION","S_MOISTURE","S_INFO","S_GAS","S_GPS","S_WATER_QUALITY"])
        self.types.append(["V_TEMP","V_HUM","V_STATUS","V_PERCENTAGE","V_PRESSURE","V_FORECAST","V_RAIN","V_RAINRATE","V_WIND","V_GUST","V_DIRECTION","V_UV","V_WEIGHT","V_DISTANCE","V_IMPEDANCE","V_ARMED","V_TRIPPED","V_WATT","V_KWH","V_SCENE_ON","V_SCENE_OFF","V_HVAC_FLOW_STATE","V_HVAC_SPEED","V_LIGHT_LEVEL","V_VAR1","V_VAR2","V_VAR3","V_VAR4","V_VAR5","V_UP","V_DOWN","V_STOP","V_IR_SEND","V_IR_RECEIVE","V_FLOW","V_VOLUME","V_LOCK_STATUS","V_LEVEL","V_VOLTAGE","V_CURRENT","V_RGB","V_RGBW","V_ID","V_UNIT_PREFIX","V_HVAC_SETPOINT_COOL","V_HVAC_SETPOINT_HEAT","V_HVAC_FLOW_MODE","V_TEXT","V_CUSTOM","V_POSITION","V_IR_RECORD","V_PH","V_ORP","V_EC","V_VAR","V_VA","V_POWER_FACTOR"])
        self.types.append(["V_TEMP","V_HUM","V_STATUS","V_PERCENTAGE","V_PRESSURE","V_FORECAST","V_RAIN","V_RAINRATE","V_WIND","V_GUST","V_DIRECTION","V_UV","V_WEIGHT","V_DISTANCE","V_IMPEDANCE","V_ARMED","V_TRIPPED","V_WATT","V_KWH","V_SCENE_ON","V_SCENE_OFF","V_HVAC_FLOW_STATE","V_HVAC_SPEED","V_LIGHT_LEVEL","V_VAR1","V_VAR2","V_VAR3","V_VAR4","V_VAR5","V_UP","V_DOWN","V_STOP","V_IR_SEND","V_IR_RECEIVE","V_FLOW","V_VOLUME","V_LOCK_STATUS","V_LEVEL","V_VOLTAGE","V_CURRENT","V_RGB","V_RGBW","V_ID","V_UNIT_PREFIX","V_HVAC_SETPOINT_COOL","V_HVAC_SETPOINT_HEAT","V_HVAC_FLOW_MODE","V_TEXT","V_CUSTOM","V_POSITION","V_IR_RECORD","V_PH","V_ORP","V_EC","V_VAR","V_VA","V_POWER_FACTOR"])
        self.types.append(["I_BATTERY_LEVEL","I_TIME","I_VERSION","I_ID_REQUEST","I_ID_RESPONSE","I_INCLUSION_MODE","I_CONFIG","I_FIND_PARENT","I_FIND_PARENT_RESPONSE","I_LOG_MESSAGE","I_CHILDREN","I_SKETCH_NAME","I_SKETCH_VERSION","I_REBOOT","I_GATEWAY_READY","I_SIGNING_PRESENTATION","I_NONCE_REQUEST","I_NONCE_RESPONSE","I_HEARTBEAT_REQUEST","I_PRESENTATION","I_DISCOVER_REQUEST","I_DISCOVER_RESPONSE","I_HEARTBEAT_RESPONSE","I_LOCKED","I_PING","I_PONG","I_REGISTRATION_REQUEST","I_REGISTRATION_RESPONSE","I_DEBUG","I_SIGNAL_REPORT_REQUEST","I_SIGNAL_REPORT_REVERSE","I_SIGNAL_REPORT_RESPONSE","I_PRE_SLEEP_NOTIFICATION","I_POST_SLEEP_NOTIFICATION"])
        self.types.append(["ST_FIRMWARE_CONFIG_REQUEST","ST_FIRMWARE_CONFIG_RESPONSE","ST_FIRMWARE_REQUEST","ST_FIRMWARE_RESPONSE","ST_SOUND","ST_IMAGE"])
        # require configuration before starting up
        self.add_configuration_listener("house", True)
        self.add_configuration_listener(self.fullname, True)
        self.add_configuration_listener("sensors/#")

    # process an inbound message
    def process_inbound(self, node_id, child_id, command, ack, type, payload):
        # ensure command and type are valid
        if command >= len(self.commands):
            self.log_error("["+str(node_id)+"]["+str(child_id)+"] command not supported: "+str(command))
            return
        if type >= len(self.types[command]):
            self.log_error("["+str(node_id)+"]["+str(child_id)+"] type not supported: "+str(type))
            return
        # map the correspoding command and type string
        command_string = self.commands[command]
        type_string = self.types[command][type]
        ack_string = self.acks[ack]
        self.log_debug("["+str(node_id)+"]["+str(child_id)+"]["+command_string+"]["+type_string+"]["+ack_string+"] received: "+str(payload))
        # handle protocol messages
        if command_string == "PRESENTATION":
            # handle presentation messages
            self.log_info("["+str(node_id)+"]["+str(child_id)+"] presented as "+type_string)
        elif command_string == "SET":
            # handle set messages (messages from sensors handled below)
            self.log_info("["+str(node_id)+"]["+str(child_id)+"]["+command_string+"]["+type_string+"]: "+payload)
        elif command_string == "REQ":
            # handle req messages
            self.log_info("["+str(node_id)+"]["+str(child_id)+"]["+command_string+"]["+type_string+"]: "+payload)
        elif command_string == "INTERNAL":
            # handle internal messages
            if type_string == "I_TIME":
                # return the time as requested by the sensor
                self.log_info("["+str(node_id)+"] requesting timestamp")
                self.tx(node_id, child_id, command_string, type_string, int(time.time()))
            elif type_string == "I_SKETCH_NAME":
                # log the sketch name
                self.log_info("["+str(node_id)+"] reported sketch name: "+str(payload))
            elif type_string == "I_SKETCH_VERSION":
                # log the sketch version
                self.log_info("["+str(node_id)+"] reported sketch version: "+str(payload))
            elif type_string == "I_ID_REQUEST":
                # return the next available id
                self.log_info("["+str(node_id)+"] requesting node_id")
                # TODO: assigne ID
                # get the available id
                #id = self.get_available_id()
                # store it into the database
                #db.set(self.assigned_ids_key,id,sdk.utils.now())
                # send it back
                #self.tx(node_id,child_id,command_string,"I_ID_RESPONSE",str(id))
            elif type_string == "I_CONFIG":
                # return the controller's configuration
                self.log_info("["+str(node_id)+"] requesting configuration")
                metric = "I" if self.units == "imperial" else "M"
                self.tx(node_id, child_id, command_string, type_string, metric)
            elif type_string == "I_BATTERY_LEVEL":
                # log the battery level
                self.log_info("["+str(node_id)+"] reporting battery level: "+str(payload)+"%")
            elif type_string == "I_LOG_MESSAGE":
                # log a custom message
                self.log_info("["+str(node_id)+"] logging: "+str(payload))
            elif type_string == "I_GATEWAY_READY":
                # report gateway report
                log_info("["+str(node_id)+"] reporting gateway ready")
            elif type_string == "I_POST_SLEEP_NOTIFICATION":
                # report awake
                self.log_info("["+str(node_id)+"] reporting awake")
            elif type_string == "I_HEARTBEAT_RESPONSE" or type_string == "I_PRE_SLEEP_NOTIFICATION":
                # handle smart sleep
                self.log_info("["+str(node_id)+"] going to sleep")
                if node_id in self.queue and not self.queue[node_id].empty():
                    # process the queue 
                    while not self.queue[node_id].empty():
                        node_id, child_id, command_string, type_string, payload = self.queue[node_id].get()
                        # send the message
                        self.tx(node_id, child_id, command_string, type_string, payload)
            else: self.log_info("["+str(node_id)+"] received "+type_string)
        elif command_string == "STREAM":
            # handle stream messages
            return
        else: self.log_error(" Invalid command "+command_string)
        # handle messages for registered sensors
        for sensor_id in self.sensors:
            sensor = self.sensors[sensor_id]
            if node_id == sensor["node_id"] and child_id  == sensor["child_id"] and command_string == sensor["command"] and type_string == sensor["type"]: 
                # prepare the message
                message = Message(self)
                message.recipient = "controller/hub"
                message.command = "IN"
                message.args = sensor_id
                message.set("value", payload)
                # send the measure to the controller
                self.send(message)
                
    # transmit a message to a sensor in the radio network
    def tx(self, node_id, child_id, command_string, type_string, payload, ack=0, system_message=False):
        # map the correspoding command and type
        command = commands.index(command_string)
        type = types[command].index(type_string)
        ack_string = acks[ack]
        if not system_message: self.log_info("["+str(node_id)+"]["+str(child_id)+"]["+command_string+"]["+type_string+"] sending message: "+str(payload))
        # publish the payload to the mqtt broker
        topic = self.config["publish_topic_prefix"]+"/"+str(node_id)+"/"+str(child_id)+"/"+str(command)+"/"+str(ack)+"/"+str(type)
        self.log_debug("publishing on topic "+topic+": "+str(payload))
        try: 
            self.gateway.publish(topic, str(payload))
        except Exception,e:
            self.log_error("unable to publish "+str(payload)+" on topic "+topic+": "+exception.get(e))
            
    # connect to the gateway
    def connect(self):
        # receive callback when conneting
        def on_connect(client, userdata, flags, rc):
            if rc == 0:
                log_debug("Connected to the MQTT gateway ("+str(rc)+")")
                # subscribe to topic
                self.subscribe_topic(self.config["subscribe_topic_prefix"])
                self.connected = True
            
        # receive a callback when receiving a message
        def on_message(client, userdata, msg):
            try:
                self.log_debug("received "+str(msg.topic)+": "+str(msg.payload))
                # split the topic
                topic, node_id, child_id, command, ack, type = msg.topic.split("/")
            except Exception,e:
                self.log_error("Invalid format ("+msg.topic+"): "+exception.get(e))
                return
            # process the message
            self.process_inbound(int(node_id),int(child_id),int(command),int(ack),int(type),str(msg.payload))
            
        # connect to the gateway
        self.gateway = mqtt.Client()
        try: 
            self.log_info("Connecting to MQTT gateway on "+self.config["hostname"]+":"+str(self.config["port"]))
            if "username" in self.config and "password" in self.config: self.gateway.username_pw_set(self.config["username"], password=self.config["password"])
            self.gateway.connect(self.config["hostname"], self.config["port"], 60)
        except Exception,e:
            self.log_warning("Unable to connect to the MQTT gateway: "+exception.get(e))
            return
        # set callbacks
        self.gateway.on_connect = on_connect
        self.gateway.on_message = on_message
        
    # What to do when running
    def on_start(self):
        self.log_info("Starting mysensors MQTT gateway")
        self.connect()
        # start loop (in the background)
        # TODO: reconnect
        try: 
            self.gateway.loop_start()
        except Exception,e: 
            self.log_error("Unexpected runtime error: "+exception.get(e))
    
    # What to do when shutting down
    def on_stop(self):
        self.gateway.loop_stop()
        self.gateway.disconnect()
        
    # What to do when receiving a request for this module
    def on_message(self, message):
        sensor_id = message.args
        if message.command == "OUT":
            if not self.connected: return
            if not self.is_valid_configuration(["node_id", "child_id", "command", "type", "value"], message.get_data()): return
            node_id = message.get("node_id")
            child_id = message.get("child_id")
            command_string = message.get("command")
            type_string = message.get("type")
            data = message.get("value")
            queue_size = message.get("queue_size") if message.has("queue_size") else None
            if "queue_size" is None:
                # send the message directly
                self.tx(node_id, child_id, command_string, type_string, data)
            else:
                # may be sleeping, queue it
                self.log_info("["+str(+node_id)+"]["+str(child_id)+"] queuing message: "+str(data))
                if node_id not in self.queue: self.queue[node_id] = Queue.Queue(queue_size)
                if self.queue[node_id].full(): 
                    # if the queue is full, clear it
                    with self.queue[node_id].mutex: self.queue[node_id].queue.clear()
                self.queue[node_id].put([node_id, child_id, command_string, type_string, data])

    # subscribe to a mqtt topic
    def subscribe_topic(self, topic):
        self.log_debug("Subscribing to the MQTT topic "+topic)
        self.gateway.subscribe(topic)        

    # What to do when receiving a new/updated configuration for this module    
    def on_configuration(self,message):
        # we need units
        if message.args == "house":
            if not self.is_valid_module_configuration(["units"], message.get_data()): return
            self.units = message.get("units")
        # module's configuration
        if message.args == self.fullname:
            if not self.is_valid_module_configuration(["hostname", "port", "subscribe_topic_prefix", "publish_topic_prefix"], message.get_data()): return
            self.config = message.get_data()
        # sensors to register
        elif message.args.startswith("sensors/"):
            sensor_id = message.args.replace("sensors/","")
            sensor = message.get_data()
            # a sensor has been deleted
            if message.is_null:
                if sensor_id in self.sensors: del self.sensors[sensor_id]
            # a sensor has been added/updated
            else: 
                # filter in only relevant sensors
                # TODO: certificate, client_id, ssl
                if "service" not in sensor or sensor["service"]["name"] != self.name or sensor["service"]["mode"] != "passive": return
                configuration = sensor["service"]["configuration"]
                if not self.is_valid_configuration(["node_id", "child_id", "command", "type"], configuration): return
                # TODO: check command/type for valid values only
                # keep track of the sensor's configuration
                self.sensors[sensor_id] = configuration
                self.log_info("registered sensor "+sensor_id)