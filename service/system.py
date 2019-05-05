### service/system: collect telemetry information from the system
## HOW IT WORKS: 
## DEPENDENCIES:
# OS: 
# Python: 
## CONFIGURATION:
# required: 
# optional: 
## COMMUNICATION:
# INBOUND: 
# - IN: 
#   required: measure
#   optional: 
# OUTBOUND: 

from sdk.module.service import Service

import sdk.utils.command

class System(Service):
    # What to do when initializing
    def on_init(self):
        # define common commands
        self.commands = {
            'cpu_user': {
                'command_poll': 'top -bn1',
                'command_parse': 'grep "Cpu(s)"|awk \'{print $2}\'',
            },
            'cpu_system': {
                'command_poll': 'top -bn1',
                'command_parse': 'grep "Cpu(s)"|awk \'{print $4}\'',
            },
            'ram_used': {
                'command_poll': 'free -m',
                'command_parse': 'grep Mem:|awk \'{print $3}\'',
            },
            'swap_used': {
                'command_poll': 'free -m',
                'command_parse': 'grep Swap:|awk \'{print $3}\'',
            },
            'load_1': {
                'command_poll': 'uptime',
                'command_parse': 'awk \'{gsub(",","",$(NF-2)); print $(NF-2)}\'',
            },
            'load_5': {
                'command_poll': 'uptime',
                'command_parse': 'awk \'{gsub(",","",$(NF-1)); print $(NF-1)}\'',
            },
            'load_15': {
                'command_poll': 'uptime',
                'command_parse': 'awk \'{gsub(",","",$(NF-0)); print $(NF-0)}\'',
            },
            'network_modules': {
                'command_poll': 'netstat -tunap 2>/dev/null',
                'command_parse': 'grep tcp|grep LISTEN|wc -l',
            },
            'network_connections': {
                'command_poll': 'netstat -tunap 2>/dev/null',
                'command_parse': 'grep tcp|grep -v LISTEN|wc -l',
            },
            'temperature': {
                'command_poll': 'cat /sys/class/thermal/thermal_zone0/temp',
                'command_parse': 'awk \'{printf "%.1f",$0/1000}\'',
            },
            'application_database': {
                'command_poll': 'ls -al /var/lib/redis/',
                'command_parse': 'grep dump.rdb|awk \'{print $5}\' |grep -o \'[0-9.]\\+\' | awk \'{printf "%.1f",$0/1024/1024}\''
            },
            'uptime': {
                'command_poll': 'cat /proc/uptime',
                'command_parse': 'cut -f 1 -d "."'
            },
            'logwatch': {
                'command_poll': 'logwatch --range yesterday --output stdout --format text',
                'command_parse': 'cat'
            },
            'reboot': {
                'command_poll': 'reboot',
                'command_parse': ''
            },
            'shutdown': {
                'command_poll': 'shutdown -h now',
                'command_parse': ''
            },
            'system_logs': {
                'command_poll': 'tail -100 /var/log/messages',
                'command_parse': 'perl -ne \'/^(\\S+ \\S+ \\S+) \\S+ (\\S+): (.+)$/;print \"$1|_|$2|_|$3\\n\"\''
            }
        }
    
    # What to do when running
    def on_start(self):
        pass
    
    # What to do when shutting down    
    def on_stop(self):
        pass

    # What to do when receiving a request for this module        
    def on_message(self, message):
        if message.command == "IN":
            sensor_id = message.args
            # ensure configuration is valid
            if not self.is_valid_configuration(["measure"], message.get_data()): return
            measure = message.get("measure")
            if measure not in self.commands:
                self.log_error("invalid measure "+measure)
                return                
            # if the raw data is cached, take it from there, otherwise request the data and cache it
            command_poll = self.commands[measure]["command_poll"]
            cache_key = "/".join([type, str(command_poll)])
            if self.cache.find(cache_key): 
                data = self.cache.get(cache_key)
            else:
                # run the poll command
                data = sdk.utils.command.run(command_poll)
                self.cache.add(cache_key, data)
            data = str(data).replace("'","''")
            command_parse = self.commands[measure]["command_parse"]
            # no command to run, return the raw data
            if command_parse != "": 
                # run command parse
                command = "echo '"+data+"' |"+command_parse
                data = sdk.utils.command.run(command)
            # send the response back
            message.reply()
            message.set("value", data)
            self.send(message)
            
    # What to do when receiving a new/updated configuration for this module
    def on_configuration(self,message):
        pass
