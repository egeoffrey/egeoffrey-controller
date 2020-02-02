# controller/db: database driver for Redis

import redis

import sdk.python.utils.exceptions as exception
import sdk.python.utils.numbers
import sdk.python.utils.strings

class Db_redis():
    def __init__(self, module):
        self.db = None
        self.connected = False
        self.db_schema_version = 1
        self.query_debug = False
        self.module = module
        
     # connect to the database
    def connect(self):
        hostname = self.module.hostname if self.module.hostname is not None else self.module.config["hostname"]
        port = self.module.port if self.module.port is not None else self.module.config["port"]
        database = self.module.database if self.module.database is not None else self.module.config["database"]
        while not self.connected:
            try: 
                self.module.log_debug("Connecting to database "+str(database)+" at "+hostname+":"+str(port))
                self.db = redis.StrictRedis(host=hostname, port=port, db=database)
                if self.db.ping():
                    self.module.log_info("Connected to database #"+str(database)+" at "+hostname+":"+str(port)+", redis version "+self.db.info().get('redis_version'))
                    self.connected = True
            except Exception,e:
                self.module.log_error("Unable to connect to "+hostname+":"+str(port)+" - "+exception.get(e))
                self.module.sleep(5)
                if self.module.stopping: break       

    # disconnect from the database
    def disconnect(self):
        if self.connected: self.db.connection_pool.disconnect()
        
    # show the available keys applying the given filter
    def keys(self, key):
        if self.query_debug: self.module.log_debug("keys "+key)
        return self.db.keys(key)

    # save a value to the db
    def set(self, key, value, timestamp, log=True):
        if timestamp is None: 
            if log: self.module.log_warning("no timestamp provided for key "+key)
            return 
        # zadd with the score
        value = str(timestamp)+":"+str(value)
        if self.query_debug and log: self.module.log_debug("zadd "+key+" "+str(timestamp)+" "+str(value))
        return self.db.zadd(key, timestamp, value)

    # set a single value into the db
    def set_simple(self, key,value):
        if self.query_debug: self.module.log_debug("set "+str(key))
        self.db.set(key, str(value))

    # get a single value from the db
    def get(self, key):
        if self.query_debug: self.module.log_debug("get "+key)
        return self.db.get(key)

    # get a range of values from the db based on the timestamp
    def rangebyscore(self, key, start=None, end=None, withscores=True, milliseconds=False, format_date=False, formatter=None, max_items=None):
        if start is None: start = self.module.date.now()-24*3600
        if end is None: end = self.module.date.now()
        if self.query_debug: self.module.log_debug("zrangebyscore "+key+" "+str(start)+" "+str(end))
        data = self.normalize_dataset(self.db.zrangebyscore(key, start, end, withscores=True), withscores, milliseconds, format_date, formatter)
        if max_items is not None and len(data) > max_items: data = data[-max_items:]
        return data
        
    # get a range of values from the db
    def range(self, key,start=-1, end=-1, withscores=True, milliseconds=False, format_date=False, formatter=None, max_items=None):
        if self.query_debug: self.module.log_debug("zrange "+key+" "+str(start)+" "+str(end))
        data = self.normalize_dataset(self.db.zrange(key, start, end, withscores=True), withscores, milliseconds, format_date, formatter)
        if max_items is not None and len(data) > max_items: data = data[-max_items:]
        return data

    # delete a key
    def delete(self, key):
        if self.query_debug: self.module.log_debug("del "+key)
        return self.db.delete(key)

    # rename a key
    def rename(self, key,new_key):
        if self.query_debug: self.module.log_debug("rename "+key+" "+new_key)
        return self.db.rename(key, new_key)

    # delete all elements between a given score
    def deletebyscore(self, key,start,end):
        if self.query_debug: self.module.log_debug("zremrangebyscore "+key+" "+str(start)+" "+str(end))
        return self.db.zremrangebyscore(key, start, end)

    # delete all elements between a given rank
    def deletebyrank(self, key,start,end):
        if self.query_debug: self.module.log_debug("zremrangebyrank "+key+" "+str(start)+" "+str(end))
        return self.db.zremrangebyrank(key, start, end)

    # check if a key exists
    def exists(self, key):
        if self.query_debug: self.module.log_debug("exists "+key)
        return self.db.exists(key)

    # empty the database
    def flushdb(self):
        if self.query_debug: self.module.log_debug("flushdb")
        return self.db.flushdb()
        
    # generate database statistics (key, #items, latest timestamp, earliest timestamp, latest value)
    def stats(self):
        output = []
        keys = self.keys("*")
        for key in sorted(keys):
            if self.db.type(key) != "zset": continue
            data = self.range(key, 1, 1)
            start = data[0][0] if len(data) > 0 else ""
            data = self.range(key, -1, -1)
            end = data[0][0] if len(data) > 0 else ""
            value = data[0][1] if len(data) > 0 else ""
            output.append([key, self.db.zcard(key), start, end, sdk.python.utils.strings.truncate(value, 300)])
            

    # initialize an empty database
    def init_database(self):
        version = None
        if self.exists(self.module.version_key): version = self.get(self.module.version_key)
        # no version found, assuming first installation
        if version is None:
            self.module.log_info("Setting database schema to v"+str(self.db_schema_version))
            self.set_simple(self.module.version_key, self.db_schema_version)
        else:
            version = int(version)
            # already at the latest version
            if version == self.db_schema_version:
                pass
            # database schema needs to be upgraded
            elif version < self.db_schema_version: 
                pass
            # higher version, something strange is happening
            elif version > self.db_schema_version: 
                self.module.log_error("database schema v"+str(version)+" is higher than the supported schema v"+str(self.db_schema_version))
        
    # normalize the output
    def normalize_dataset(self, data, withscores, milliseconds, format_date, formatter):
        output = []
        for entry in data:
            # get the timestamp 
            timestamp = int(entry[1])
            if format_date: timestamp = self.module.date.timestamp2date(timestamp)
            elif milliseconds: timestamp = timestamp*1000
            # normalize the value (entry is timetime:value)
            value_string = entry[0].split(":",1)[1];
            if formatter is None:
                # no formatter provided, guess the type
                value = float(value_string) if sdk.python.utils.numbers.is_number(value_string) else str(value_string)
            else:
                # formatter provided, normalize the value
                value = sdk.python.utils.numbers.normalize(value_string, formatter)
            # normalize "None" in null
            if value == "None": value = None
            # prepare the output
            if (withscores): output.append([timestamp,value])
            else: output.append(value)
        return output