#!/bin/sh

# welcome message
echo
echo -e "\e[37;42m                     \e[0m"
echo -e "\e[1;37;42m Welcome to myHouse  \e[0m"
echo -e "\e[37;42m                     \e[0m"
echo
echo -e "[\e[33mmyHouse\e[0m] User defined variables:"
MYHOUSE_VERSION=$(echo -e "import sdk.constants\nprint sdk.constants.VERSION"|python)
MYHOUSE_API_VERSION=$(echo -e "import sdk.constants\nprint sdk.constants.API_VERSION"|python)
echo "MYHOUSE_VERSION: $MYHOUSE_VERSION"
echo "MYHOUSE_API_VERSION: $MYHOUSE_API_VERSION"
echo
echo "MYHOUSE_MODULES: $MYHOUSE_MODULES"
echo "MYHOUSE_SDK_BRANCH: $MYHOUSE_SDK_BRANCH"
echo
echo "MYHOUSE_GATEWAY_HOSTNAME: $MYHOUSE_GATEWAY_HOSTNAME"
echo "MYHOUSE_GATEWAY_PORT: $MYHOUSE_GATEWAY_PORT"
echo "MYHOUSE_GATEWAY_TRANSPORT: $MYHOUSE_GATEWAY_TRANSPORT"
echo "MYHOUSE_GATEWAY_SSL: $MYHOUSE_GATEWAY_SSL"
echo 
echo "MYHOUSE_ID: $MYHOUSE_ID"
echo "MYHOUSE_PASSCODE: $MYHOUSE_PASSCODE"
echo
echo "MYHOUSE_DEBUG: $MYHOUSE_DEBUG"
echo "MYHOUSE_LOGGING_LOCAL: $MYHOUSE_LOGGING_LOCAL"
echo "MYHOUSE_LOGGING_REMOTE: $MYHOUSE_LOGGING_REMOTE"
echo

# execute myHouse
if [ "$1" = 'run' ]; then
    echo -e "[\e[33mmyHouse\e[0m] Running pre-init script..."
    ./docker-pre-init.sh
    echo -e "[\e[33mmyHouse\e[0m] Starting myHouse..."
    exec python run.py
fi

# execute the provided command
exec "$@"