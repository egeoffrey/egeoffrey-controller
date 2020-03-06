# controller/db: database driver for MongoDB

import pymongo
import sys
import re

import sdk.python.utils.exceptions as exception
import sdk.python.utils.numbers
import sdk.python.utils.strings


class Db_mongo():
    def __init__(self, module):
        self.db = None
        self.client = None
        self.connected = False
        self.db_schema_version = 1
        self.query_debug = False
        self.module = module
        self.db_version = None
        self.collections = []
        
     # connect to the database
    def connect(self):
        hostname = self.module.hostname if self.module.hostname is not None else self.module.config["hostname"]
        port = self.module.port if self.module.port is not None else self.module.config["port"]
        database = self.module.database if self.module.database is not None else self.module.config["database"]
        username = self.module.username if self.module.username is not None else self.module.config["username"]
        password = self.module.password if self.module.password is not None else self.module.config["password"]
        while not self.connected:
            try: 
                self.module.log_debug("Connecting to database "+str(database)+" at "+hostname+":"+str(port))
                self.client = pymongo.MongoClient("mongodb://"+username+":"+password+"@"+hostname+":"+str(port)+"/"+str(database))
                self.db = self.client[database]
                self.db_version = str(self.client.server_info()["version"])
                self.module.log_info("Connected to database "+str(database)+" at "+hostname+":"+str(port)+", mongodb version "+self.db_version)
                self.connected = True
            except Exception,e:
                self.module.log_error("Unable to connect to "+hostname+":"+str(port)+" - "+exception.get(e))
                self.module.sleep(5)
                if self.module.stopping: break       

    # disconnect from the database
    def disconnect(self):
        if self.connected: self.client.close()
        
    # show the available keys applying the given filter
    def keys(self, key):
        filter = re.escape(key).replace('\\*', '.*')
        if self.query_debug: self.module.log_debug("list_collection_names() "+filter)
        return self.db.list_collection_names(filter={"name": {"$regex": filter}})

    # save a timeseries value to the db
    def set_series(self, key, value, timestamp, log=True):
        if timestamp is None: 
            if log: self.module.log_warning("no timestamp provided for key "+key)
            return 
        # check if the collection has been hit already (in cache)
        if key not in self.collections:
            # check if the collection is already in the database
            result = self.db.list_collection_names(filter={"name": key})
            # if not, create the collection and the index
            if len(result) == 0:
                self.db.create_collection(key)
                self.db[key].create_index([("timestamp", pymongo.DESCENDING)])
                # add it to the cache
                self.collections.append(key)
        # insert a new document
        document = {
            "timestamp": timestamp,
            "value": str(value),
        }
        if self.query_debug: self.module.log_debug(key+" insert_one() "+str(document))
        self.db[key].insert_one(document)

    # set a single value into the db
    def set_value(self, key, value):
        # delete the collection first
        self.db[key].drop()
        # insert a new document
        document = {
            "value": str(value),
        }
        if self.query_debug: self.module.log_debug(key+" insert_one() "+str(document))
        self.db[key].insert_one(document)

    # get a single value from the db
    def get_value(self, key):
        if self.query_debug: self.module.log_debug("find_one() "+key)
        result = self.db[key].find_one()
        if result != "": return result["value"]
        else: return ""

    # get a range of values from the db based on the timestamp
    def get_by_timeframe(self, key, start=None, end=None, withscores=True, milliseconds=False, format_date=False, formatter=None, max_items=None):
        if start is None: start = self.module.date.now()-24*3600
        if end is None: end = self.module.date.now()
        if start == "-inf": start = 0
        if start == "+inf": start = sys.maxint
        if end == "-inf": end = 0
        if end == "+inf": end = sys.maxint
        filter = {
            "$and": [
                {"timestamp" : {"$gte": start}},
                {"timestamp" : {"$lte": end}}
            ]
        }
        if self.query_debug: self.module.log_debug("find() "+key+" "+str(start)+" "+str(end))
        result = list(self.db[key].find(filter))
        data = self.normalize_dataset(result, withscores, milliseconds, format_date, formatter)
        if max_items is not None and len(data) > max_items: data = data[-max_items:]
        return data
        
    # get a range of values from the db
    def get_by_position(self, key, start=-1, end=-1, withscores=True, milliseconds=False, format_date=False, formatter=None, max_items=None):
        # if requested from the end, sort by timestamp in desending order and use skip and limit to get the values
        if start < 0 and end < 0: 
            # start from the end, including the latest item
            skip = abs(end+1)
            # limit by tje difference between start and end
            limit = abs(start)-abs(end)+1
            if self.query_debug: self.module.log_debug("find() "+key+" DESC skip "+str(skip)+" limit "+str(limit))
            result = list(self.db[key].find().sort("timestamp", pymongo.DESCENDING).skip(skip).limit(limit))
        # otherwise get all the data and use the array positions to get the requested values
        else:
            self.module.log_warning("not optimized query get_by_position("+key+","+str(start)+","+str(end)+")")
            include_last = False
            # include the boundary
            if end >= 0 or (end < 0 and end != -1): 
                end = end+1
            # end is -1 so cannot include it directly (would be 0)
            if end == -1: 
                include_last = True
            if self.query_debug: self.module.log_debug("find() "+key+" start "+str(start)+" end "+str(end))
            all_values = list(self.db[key].find())
            result = all_values[start:end]
            # if include the latest value, add it to the result
            if include_last and len(all_values) > 0: 
                result+[all_values[-1]]
        data = self.normalize_dataset(result, withscores, milliseconds, format_date, formatter)
        if max_items is not None and len(data) > max_items: data = data[-max_items:]
        return data

    # delete a key
    def delete(self, key):
        if self.query_debug: self.module.log_debug("drop() "+key)
        if key in self.collections: self.collections.remove(key)
        return self.db[key].drop()

    # rename a key
    def rename(self, key,new_key):
        if self.query_debug: self.module.log_debug("rename() "+key+" "+new_key)
        return self.db[key].rename(new_key)

    # delete all elements between a given score
    def delete_by_timeframe(self, key, start, end):
        if start == "-inf": start = 0
        if start == "+inf": start = sys.maxint
        if end == "-inf": end = 0
        if end == "+inf": end = sys.maxint
        filter = {
            "$and": [
                {"timestamp" : {"$gte": start}},
                {"timestamp" : {"$lte": end}},
            ]
        }
        if self.query_debug: self.module.log_debug("delete_many() "+key+" "+str(filter))
        result = self.db[key].delete_many(filter)
        return result.deleted_count

    # delete all elements between a given rank
    def delete_by_position(self, key, start, end):
        # if requested from the end, sort by timestamp in desending order and use skip and limit to get the values
        if start < 0 and end < 0: 
            # start from the end, including the latest item
            skip = abs(end+1)
            # limit by tje difference between start and end
            limit = abs(start)-abs(end)+1
            if self.query_debug: self.module.log_debug("find() "+key+" DESC skip "+str(skip)+" limit "+str(limit))
            result = list(self.db[key].find({}, {'_id': 1}).sort("timestamp", pymongo.DESCENDING).skip(skip).limit(limit))
        # otherwise get all the data and use the array positions to get the requested values
        else:
            include_last = False
            # include the boundary
            if end >= 0 or (end < 0 and end != -1): 
                end = end+1
            # end is -1 so cannot include it directly (would be 0)
            if end == -1: 
                include_last = True
            if self.query_debug: self.module.log_debug("find() "+key+" start "+str(start)+" end "+str(end))
            all_values = list(self.db[key].find())
            result = all_values[start:end]
            # if include the latest value, add it to the result
            if include_last and len(all_values) > 0: 
                result+[all_values[-1]]
        # delete each document
        ids = []
        for item in result: ids.append(item["_id"])
        if self.query_debug: self.module.log_debug("remove() "+key+" "+str(ids))
        result = self.db[key].remove({ "_id" : {'$in': ids}})
        return result["n"]

    # check if a key exists
    def exists(self, key):
        if self.query_debug: self.module.log_debug("exists "+key)
        collections = self.db.list_collection_names()
        if key in collections: return True
        return False

    # empty the database
    def flushdb(self):
        if self.query_debug: self.module.log_debug("flushdb")
        collections = self.db.list_collection_names()
        for collection in collections:
            self.db[collection].drop()
        
    # generate database statistics (key, #items, latest timestamp, earliest timestamp, latest value)
    def stats(self):
        output = {}
        output["keys"] = []
        keys = self.keys("*")
        for key in sorted(keys):
            first = self.db[key].find_one({"timestamp": {"$exists": True}}, sort=[("timestamp", pymongo.ASCENDING)])
            last = self.db[key].find_one({"timestamp": {"$exists": True}}, sort=[("timestamp", pymongo.DESCENDING)])
            if first is None or last is None or "timestamp" not in first or "timestamp" not in last: continue
            start = first["timestamp"]
            end = last["timestamp"]
            value = last["value"]
            key_stats = self.db.command("collstats", key)
            output["keys"].append([key, self.db[key].count(), key_stats["size"], start, end, sdk.python.utils.strings.truncate(value, 300)])
        db_stats = self.db.command("dbstats")
        output["database_size"] = db_stats["dataSize"]
        output["database_type"] = self.module.config["type"]
        output["database_version"] = self.db_version
        return output

    # initialize an empty database
    def init_database(self):
        version = None
        if self.exists(self.module.version_key): version = self.get_value(self.module.version_key)
        # no version found, assuming first installation
        if version is None:
            self.module.log_info("Setting database schema to v"+str(self.db_schema_version))
            self.set_value(self.module.version_key, self.db_schema_version)
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
            timestamp = int(entry["timestamp"])
            if format_date: timestamp = self.module.date.timestamp2date(timestamp)
            elif milliseconds: timestamp = timestamp*1000
            # normalize the value
            value_string = str(entry["value"]);
            if formatter is None:
                # no formatter provided, guess the type
                value = float(value_string) if sdk.python.utils.numbers.is_number(value_string) else str(value_string)
            else:
                # formatter provided, normalize the value
                value = sdk.python.utils.numbers.normalize(value_string, formatter)
            # normalize "None" in null
            if value == "None": value = None
            # prepare the output
            if (withscores): output.append([timestamp, value])
            else: output.append(value)
        return output
