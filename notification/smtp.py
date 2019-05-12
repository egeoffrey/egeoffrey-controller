### send out email notifications
## HOW IT WORKS: 
## DEPENDENCIES:
# OS:
# Python: 
## CONFIGURATION:
# required: "hostname", "port", "tls", "username", "password", "from", "to", "subject", "template"
# optional: 
## COMMUNICATION:
# INBOUND: 
# - NOTIFY: receive a notification request
# OUTBOUND: 

import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

from sdk.module.notification import Notification

import sdk.utils.exceptions as exception
import sdk.constants

class Smtp(Notification):
    # What to do when initializing
    def on_init(self):
        # configuration settings
        self.house = {}
        # debug smtp
        self.debug_smtp = False
        # require configuration before starting up
        self.add_configuration_listener("house", True)

    # What to do when running
    def on_start(self):
        pass
        
    # What to do when shutting down
    def on_stop(self):
        pass
        
    # return the HTML template of the widget
    def get_email_widget(self, title, body):
        template = '<tr class="total" style="font-family: \'Helvetica Neue\',Helvetica,Arial,sans-serif; \
        box-sizing: border-box; font-size: 14px; margin: 0;"><td class="alignright" width="80%" style="font-family: \'Helvetica \
        Neue\',Helvetica,Arial,sans-serif; box-sizing: border-box; font-size: 14px; vertical-align: top; text-align: center; border-top-width: 2px; \
        border-top-color: #333; border-top-style: solid; border-bottom-color: #333; border-bottom-width: 2px; border-bottom-style: solid; font-weight: 700; \
        margin: 0; padding: 5px 0;" valign="top">#title# \
        <br>#body# \
        </td></tr>'
        template = template.replace("#body#", body)
        template = template.replace("#title#", title)
        return template       

    # detect if the text contains non-ascii characters
    def is_unicode(self, text):
        return not all(ord(c) < 128 for c in text)
        
    # send an email
    def send_email(self, subject, body):
        msg = MIMEMultipart()
        # prepare the message
        msg['From'] = self.config["from"]
        msg['To'] = ", ".join(self.config["to"])
        msg['Subject'] = "["+self.house["name"]+"] "+subject
        if self.is_unicode(body): msg.attach(MIMEText(body.encode('utf-8'), 'html', 'utf-8'))
        else: msg.attach(MIMEText(body, 'html'))
        smtp = smtplib.SMTP(self.config["hostname"], self.config["port"])
        # set debug
        smtp.set_debuglevel(self.debug_smtp)
        # setup TLS
        if self.config["tls"]: smtp.starttls()
        # authenticate
        if self.config["username"] != '': smtp.login(self.config["username"], self.config["password"])
        # send the message
        self.log_info("sending email '"+subject+"' to "+msg['To'])
        smtp.sendmail(self.config["from"], self.config["to"], msg.as_string())
        smtp.quit()

    # What to do when ask to notify
    def on_notify(self, severity, text):
        self.log_info("emailing alert "+text)
        title = self.config["subject"]
        template = self.config["template"]
        #template = template.replace("#url#",conf['gui']['url'])
        template = template.replace("#version#", sdk.constants.VERSION)
        template = template.replace("#title#", title)
        template = template.replace("#body#", self.get_email_widget("Alert", text))
        # send the email
        self.send_email(title, template.encode('utf-8'))

     # What to do when receiving a new/updated configuration for this module    
    def on_configuration(self, message):
        if message.args == "house":
            if not self.is_valid_module_configuration(["name"], message.get_data()): return False
            self.house = message.get_data()
        # module's configuration
        if message.args == self.fullname:
            if not self.is_valid_module_configuration(["hostname", "port", "tls", "username", "password", "from", "to", "subject", "template"], message.get_data()): return False
            self.config = message.get_data()