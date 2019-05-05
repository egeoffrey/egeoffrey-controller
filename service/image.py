### service/image: retrieve images from a url
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
#   required: url
#   optional: username, password
# OUTBOUND: 

import base64
 
from sdk.module.service import Service

import sdk.utils.web
import sdk.utils.exceptions as exception

class Image(Service):
    # What to do when initializing
    def on_init(self):
        self.image_unavailable = "iVBORw0KGgoAAAANSUhEUgAAAT0AAADuCAIAAADEJEf/AAAAAXNSR0IArs4c6QAAAARnQU1BAACxjwv8YQUAAAAJcEhZcwAADsMAAA7DAcdvqGQAAATCSURBVHja7d1rbptAFIDRrL1eSbOSZifdSRvFFapaGeZxZ+AyB31/7UQWx2MGBt4e3x6ScvXmI5C4lcStJG4lbiVxK4lbiVtJ3EriVuJWEreSuJW4lcStJG4lcStxK4lbSdxK3EriVhK3EreSuJXErcStJG4lcSuJW4lbSdxK4lbiVhK3kriVuJXErSRuJW4lcSuJW4lbSdxK4lYStxK3kriVxK3ErSRuJXErcSuJW0ncStxK4lYSt5K4lbiVxK0kbiVuJXEriVuJW0ncSuJW4lYSt5K4lbjVsH5+bT4HcZsJ7a+vDV1xmwwtuuI2JVp0xW1KtOiK25Ro0RW3Wd2iK27RFbdCV9yii664RVfiFl1xqxF0S14evvkq4VbtdE9Biy63aqd7Ilp0uVUL3dPRosut6uheBC263OpxqYEUXW6FrrhF94Wcjx8fQ9v/r9DlVnV0p5lBl9sVqxrfrvkb9fALpW0wt29we93ucWw54gjcvsHtWm7vQde+we1ybm9A177B7Ypus9O1b3C7qNvUdO0b3K7rdofuzjnY7dzviXTtG9ze1m3hxcl/033//l57KvjzJfPp2je4Tea2dqArpFsrtl9v4Rlsbrld0e20a5hHHC1zy+26bmcuP4gdeLnldmm3Selyy+3qbjPS5ZZbblvo/rPir/zlIXS55ZbbOro78Aonn7nlltvZbg/nhw/19s8wc8stt3+wxZ7a2afb+WuZW265bTy+7fku6BxyueWW27rBNmrU7RlyueWW20f57WwC6Y74h+0b3C7kdodlyDRV+MQyt9yu7vaVqyfIELqv3qT5pzK33K7u9pWB7Q376R7+CW655TZmJrlh0V/t/9w8q8wtt6u7LTz47KQbe4jLLbfclr5bD91Yadxyy23FuzXT5ZZbbk9z20yXW265PdNtG11uueX2hHmpTrrmpbjl9hLnQqvoOg/ELbdTr7vop+u6C2419TrH8AWArnPkltux6woKXTXTta6AW26HrOPrvFhyZ7OOj1tuR62bL586qqVr3Ty33A68T80Iuu5Twy23w+8LF07XfeG45TbmPqyHd6sJnKYy3nLLbYzbEnLPk7E7nOb8VOaWW26LzgntPGqk5wG53HLLbcCbt92TdfLTcbnlltt8dLnlltv4CxhH0+WWW27bZ5gLr4sKn2HmlltuRw28ny/cTh3F0uWWW27jx97w28pxyy23vYD//+m7nROac0kGt9xyO7UQutxyy20+utxyy20+utxyy20+utxyy20+utxyexO324xurqoWMGyVPENQ3CZwa+OWW2655danwC233HJr45ZbbrkVt9xyy6245ZZbrbL8QNwKXW4ldLkVuuJW6HLrUxC63ArdEY/5E7eaTRdabpWMLrTcKhldaLlVMrrQcqtkdKHlVhK3EreSuJXErcStJG4lcStxK4lbSdxK3EriVhK3kriVuJXErSRuJW4lcSuJW4lbSdxK4lbiVhK3kriVuJXErSRuJXErcSuJW0ncStxK4lYStxK3kriVxK3ErSRuJXEriVuJW0ncSuJW4lYSt5K4lbiVxK0kbiVuJXEriVuJW0ncSuJWErcSt5K4lcStxK0kbiVxK3EriVtJ3ErcSuJWEreSnv0G71zk2XXRfqkAAAAASUVORK5CYII="
        pass
    
    # What to do when running    
    def on_start(self):
        pass
    
    # What to do when shutting down    
    def on_stop(self):
        pass
    
    # What to do when receiving a request for this module
    def on_message(self, message):
        if message.command == "IN":
            if not self.is_valid_configuration(["url"], message.get_data()): return
            sensor_id = message.args
            url = message.get("url")
            username = message.get("username") if message.has("username") else None
            password = message.get("password") if message.has("password") else None
            # download the image pointed by the url
            try:
                data = sdk.utils.web.get(url, username, password, binary=True)
            except Exception,e: 
                self.log_error("unable to connect to "+url+": "+exception.get(e))
                return
            # return empty if the data is not binary
            if "<html" in data.lower(): data = ""
            if data == "": data = self.image_unavailable
            else: data = base64.b64encode(data)
            # TODO: picture unavailable placeholder
            # reply to the requesting module 
            message.reply()
            message.set("value", data)
            # send the response back
            self.send(message)

    # What to do when receiving a new/updated configuration for this module
    def on_configuration(self,message):
        pass