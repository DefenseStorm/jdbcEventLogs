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
from datetime import datetime

from six import PY2

if PY2:
    get_unicode_string = unicode
else:
    get_unicode_string = str


sys.path.insert(0, './ds-integration')
from DefenseStorm import DefenseStorm

import jaydebeapi

class integration(object):


    def jdbc_main(self): 


        # Get JDBC Config info
        try:
            driver = self.ds.config_get('jdbc', 'driver')
            db_jarfile = self.ds.config_get('jdbc', 'db_jarfile')
            db_json_file = self.ds.config_get('jdbc', 'db_json_file')
            self.conn_url = self.ds.config_get('jdbc','connection_url')
            self.hostname = self.ds.config_get('jdbc', 'hostname')
            self.state_dir = self.ds.config_get('jdbc', 'state_dir')
            self.last_run = self.ds.get_state(state_dir)
            self.time_format = self.ds.config_get('jdbc', 'time_format')
            current_time = time.time()
            if self.last_run == None:
                self.last_run = (datetime.utcfromtimestamp(60 * ((current_time - 120) // 60))).strftime(self.time_format)
            self.current_run = (datetime.utcfromtimestamp(current_time)).strftime(self.time_format)
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



        self.ds.log("INFO", "Connection URL: " + self.conn_url)

        print(self.conn_url)

        try:
            conn = jaydebeapi.connect(driver, self.conn_url, [username, password], db_jarfile)
        except Exception as e:
                traceback.print_exc()
                self.ds.log("ERROR", "Failed to connect to DB")
                self.ds.log('ERROR', "Exception {0}".format(str(e)))

        if conn == None:
            self.ds.log("ERROR", "Error connecting to the DB, no exception")
        else:
            self.ds.log("INFO", "Successfully connected to DB URL")


        for entry in db_tables:
            #query = "select " + ','.join(entry['values']) + " from " + entry['table_name']
            query = "select " + ','.join(entry['values']) + " from " + entry['table_name'] + " where " + entry['timestamp'] + " > \"" + self.last_run + "\""
            self.ds.log("INFO", "Query: " + query)
            print(query)
            curs = conn.cursor()
            curs.execute(query)
            result = []
            columns = tuple( [d[0] for d in curs.description] )
            for row in curs.fetchall():
                result.append(dict(zip(columns,row)))

            for item in result:
                item['timestamp'] = item['CreatedUTC']
                item['hostname'] = self.hostname
                self.ds.writeJSONEvent(item)

            self.ds.set_state(self.state_dir, self.current_run)


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


if __name__ == "__main__":
    i = integration(sys.argv[1:]) 
    i.run()
