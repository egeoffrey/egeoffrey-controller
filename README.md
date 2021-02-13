# egeoffrey-controller

This is an eGeoffrey controller package.

## Description

The eGeoffrey controller manages the configuration of all the modules and coordinates sensors and run alerting rules.

## Install

To install this package, run the following command from within your eGeoffrey installation directory:
```
egeoffrey-cli install egeoffrey-controller
```
After the installation, remember to run also `egeoffrey-cli start` to ensure the Docker image of the package is effectively downloaded and started.
To validate the installation, go and visit the *'eGeoffrey Admin'* / *'Packages'* page of your eGeoffrey instance. All the modules, default configuration files and out-of-the-box contents if any will be automatically deployed and made available.
## Content

The following modules are included in this package.

For each module, if requiring a configuration file to start, its settings will be listed under *'Module configuration'*. Additionally, if the module is a service, the configuration expected to be provided by each registered sensor associated to the service is listed under *'Service configuration'*.

To configure each module included in this package, once started, click on the *'Edit Configuration'* button on the *'eGeoffrey Admin'* / *'Modules'* page of your eGeoffrey instance.
- **controller/logger**: takes care of collecting the logs from all the local and remote modules, storing them in the database and printing them out
  - Module configuration:
    - *database_enable**: enable logging into the database
    - *database_retention**: number of days to keep old logs in the database (e.g. 6)
    - *file_enable**: enable logging to file (in the /logs directory)
    - *file_rotate_size**: rotate the log file when reaching this size (in megabytes) (e.g. 5)
    - *file_rotate_count**: number of files to keep when rotating the logs (e.g. 5)
- **controller/db**: connects to the database and runs queries on behalf of other modules
  - Module configuration:
    - *type**: the underlying database to use
    - *hostname**: the IP/hostname the Redis database is listening to (e.g. egeoffrey-database)
    - *port**: the port the Redis database is listening to (e.g. 6379)
    - *database**: the database number to use for storing the information (e.g. 1)
    - *username*: the username for connecting to the database (e.g. root)
    - *password*: the password for connecting to the database (e.g. password)
- **controller/config**: stores configuration files on behalf of all the modules and makes them available
- **controller/alerter**: keep running the configured rules which would trigger notifications
  - Module configuration:
    - *retention**: number of days to keep old logs in the database (e.g. 6)
    - *loop_safeguard**: prevent the same rule to run again within this timeframe in seconds to avoid loops (e.g. 3)
- **controller/chatbot**: interactive chatbot service
  - Module configuration:
    - *vocabulary**: chatbot's basic vocabulary
- **controller/hub**: hub for collecting new measures from sensors
  - Module configuration:
    - *calculate**: aggregation policies that can be associated to sensors to e.g. automatically calculate average, minimum and maximum per hour/day
    - *retain**: retention policies that can be associated to sensors to e.g. automatically purge old values from the database
    - *post_processors**: set of available post processing commands that can be associated to sensors to e.g. automatically post-process a new value once collected
    - *duplicates_tolerance*: if requested to save the same sensor's value of the latest in a very short time, ignore it (tolerance in seconds)

## Contribute

If you are the author of this package, simply clone the repository, apply any change you would need and run the following command from within this package's directory to commit your changes and automatically push them to Github:
```
egeoffrey-cli commit "<comment>"
```
After taking this action, remember you still need to build (see below) the package (e.g. the Docker image) to make it available for installation.

If you are a user willing to contribute to somebody's else package, submit your PR (Pull Request); the author will take care of validating your contributation, merging the new content and building a new version.

## Build

Building is required only if you are the author of the package. To build a Docker image and automatically push it to [Docker Hub](https://hub.docker.com/r/egeoffrey/egeoffrey-controller), run the following command from within this package's directory:
```
egeoffrey-cli build egeoffrey-controller
```
To function properly, when running in a Docker container, the following additional configuration settings has to be added to e.g. your docker-compose.yml file (when installing through egeoffrey-cli, this is not needed since done automatically upon installation):
```
environment:
- EGEOFFREY_LOGGING_LOCAL=0
volumes:
- ./data/egeoffrey/logs:/egeoffrey/logs
- ./data/egeoffrey/config:/egeoffrey/config
```

## Uninstall

To uninstall this package, run the following command from within your eGeoffrey installation directory:
```
egeoffrey-cli uninstall egeoffrey-controller
```
Remember to run also `egeoffrey-cli start` to ensure the changes are correctly applied.
## Tags

The following tags are associated to this package:
```
controller
```

## Version

The version of this egeoffrey-controller is 1.3-3 on the master branch.
