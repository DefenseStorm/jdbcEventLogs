#!/usr/bin/env python3

import sys,os,getopt
import traceback
import io
import os
import fcntl
import json
import time
import csv
from random import randrange
from datetime import datetime, timedelta

from six import PY2

if PY2:
    get_unicode_string = unicode
else:
    get_unicode_string = str


sys.path.insert(0, './ds-integration')
from DefenseStorm import DefenseStorm

import jaydebeapi

class integration(object):

    JSON_field_mappings = {
    }


    def jdbc_main(self): 


        # Get JDBC Config info
        try:
            driver = self.ds.config_get('jdbc', 'driver')
            #db_type = self.ds.config_get('jdbc', 'db_type')
            #host = self.ds.config_get('jdbc', 'host')
            #options = self.ds.config_get('jdbc', 'options')
            #db_name = self.ds.config_get('jdbc', 'db_name')
            db_jarfile = self.ds.config_get('jdbc', 'db_jarfile')
            #username = self.ds.config_get('jdbc', 'username')
            #password = self.ds.config_get('jdbc', 'password')
            self.conn_url = self.ds.config_get('jdbc', 'connection_url')
            db_json_file = self.ds.config_get('jdbc', 'db_json_file')
        except Exception as e:
                traceback.print_exc()
                self.ds.log("ERROR", "Failed to get required configurations")
                self.ds.log('ERROR', "Exception {0}".format(str(e)))

        db_tables = None

        try:
            with open(db_json_file) as json_file:
                db_tables = json.load(json_file)
        except Exception as e:
                traceback.print_exc()
                self.ds.log("ERROR", "Failed to load db_json_file + " + db_json_file)
                self.ds.log('ERROR', "Exception {0}".format(str(e)))


        #self.conn_url = "jdbc:" + db_type + "://" + host + (";" if options !="" else "") + options + ("/" if db_name != "" else "") + db_name
        #self.conn_url = "jdbc:" + db_type + "://" + host + ("/" if db_name != "" else "") + db_name + (";" if options !="" else "") + options
        # read files from configured directory to parse

        self.ds.log("INFO", "Connection URL: " + self.conn_url)

        print(self.conn_url)

        conn = None

        try:
            #conn = jaydebeapi.connect(driver, self.conn_url, [username, password], db_jarfile)
            conn = jaydebeapi.connect(driver, self.conn_url, [], db_jarfile)
        except Exception as e:
                traceback.print_exc()
                self.ds.log("ERROR", "Failed to connect to DB")
                self.ds.log('ERROR', "Exception {0}".format(str(e)))

        if conn == None:
            self.ds.log("ERROR", "Error connecting to the DB, no exception")
            return
        else:
            self.ds.log("INFO", "Successfully connected to DB URL")


        for entry in db_tables:
            #query = "select " + ','.join(entry['values']) + " from " + entry['table_name']
            query = "select TOP 10 " + ','.join(entry['values']) + " from " + entry['table_name']
            query = "select " + ','.join(entry['values']) + " from " + entry['table_name'] + " where " + entry['time_field'] + ' > "' + self.mystate.isoformat() + '" AND ' + entry['time_field'] + ' < "' + self.newstate.isoformat() + '"'
            self.ds.log("INFO", "Query: " + query)
            curs = conn.cursor()
            curs.execute(query)
            result = []
            columns = tuple( [d[0] for d in curs.description] )
            for row in curs.fetchall():
                result.append(dict(zip(columns,row)))

            for item in result:
                self.ds.writeJSONEvent(item)


        self.ds.log('INFO', "Done Sending Notifications")


    def run(self):
        try:
            pid_file = self.ds.config_get('jdbc', 'pid_file')
            fp = io.open(pid_file, 'w')
            try:
                fcntl.lockf(fp, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except IOError:
                self.ds.log('ERROR', "An instance of csv integration is already running")
                # another instance is running
                sys.exit(0)
            self.jdbc_main()
        except Exception as e:
            traceback.print_exc()
            self.ds.log('ERROR', "Exception {0}".format(str(e)))
            return
        self.ds.set_state(self.state_dir, self.newstate)
    
    def usage(self):
        print
        print (os.path.basename(__file__))
        print
        print ('  No Options: Run a normal cycle')
        print
        print ('  -t    Testing mode.  Do all the work but do not send events to GRID via ')
        print ('        syslog Local7.  Instead write the events to file \'output.TIMESTAMP\'')
        print ('        in the current directory')
        print
        print ('  -l    Log to stdout instead of syslog Local6')
        print
    
    def __init__(self, argv):

        self.testing = False
        self.send_syslog = True
        self.ds = None
        self.conf_file = None

        self.conn_url = None
    
        try:
            opts, args = getopt.getopt(argv,"htnld:c:",["datedir="])
        except getopt.GetoptError:
            self.usage()
            sys.exit(2)
        for opt, arg in opts:
            if opt == '-h':
                self.usage()
                sys.exit()
            elif opt in ("-t"):
                self.testing = True
            elif opt in ("-l"):
                self.send_syslog = False
            elif opt in ("-c"):
                self.conf_file = arg
    
        try:
            self.ds = DefenseStorm('jdbcEventLogs', testing=self.testing, send_syslog = self.send_syslog, config_file = self.conf_file)
        except Exception as e:
            traceback.print_exc()
            try:
                self.ds.log('ERROR', 'ERROR: ' + str(e))
            except:
                pass

        self.state_dir = os.path.join(self.ds.config_get('jdbc', 'app_path'), 'state')
        self.mystate = self.ds.get_state(self.state_dir)
        self.newstate = datetime.now()
        if self.mystate == None:
            self.mystate = self.newstate - timedelta(0,600)



if __name__ == "__main__":
    i = integration(sys.argv[1:]) 
    i.run()
