#docker run --rm -d -p 1883:1883 -p 443:443 -v $(pwd)/mqtt:/mosquitto/config eclipse-mosquitto
# docker run --rm -d -p 6379:6379 -v $(pwd)/redis:/data redis
#mosquitto_sub -t '#' -v
# mosquitto_pub  -t myHouse/v1/default_house/core/sensors/plugin/wunderground/temperature/40.71,-74.0 -q 1
# mosquitto_pub  -t myHouse/v1/default_house/a/b/core/controller/debug/plugin/wunderground -m "1"
export MYHOUSE_ID=default_house
export MYHOUSE_DEBUG=0
export MYHOUSE_LOGGING_REMOTE=0
export MYHOUSE_LOGGING_LOCAL=1
#export MYHOUSE_GATEWAY_HOSTNAME=192.168.0.254
#export MYHOUSE_GATEWAY_PORT=443
#export MYHOUSE_GATEWAY_TRANSPORT=websockets
#export MYHOUSE_GATEWAY_SSL=1
#export MYHOUSE_GATEWAY_CA_CERT=../run/mqtt/ca.crt
#export MYHOUSE_GATEWAY_CERTFILE=../run/mqtt/server.crt
#export MYHOUSE_GATEWAY_KEYFILE=../run/mqtt/server.key
export MYHOUSE_CONFIG_DIR=../run/myHouse/conf
export MYHOUSE_CONFIG_FORCE_RELOAD=0
export MYHOUSE_MODULES="controller/logger, \
	controller/config, \
	controller/db, \
	controller/alerter, \
	controller/chatbot, \
	service/command, \
	service/image, \
	service/mqtt, \
	notification/smtp, \
	controller/hub"
