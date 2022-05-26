#! /usr/bin/env python
#---------------------------------------------------------------
# This script generates a SQLite dump of top CRT tables,
# using info from ICARUS hardware database. It is basically a manual
# conversion from PostGreSQL to SQLite.
#
# Author: mvicenzi
# Inspired by: copyPostGresToSQLite.py (by T. Usher)
#
#---------------------------------------------------------------

from __future__ import print_function
import sys, os, datetime
import psycopg2
import sqlite3
import re #regex
from   DataLoader3 import DataLoader, DataQuery

# Set up the columns for the tables
topcrtFEBColumns = ['feb_barcode text PRIMARY KEY NOT NULL',
                    'serialnum integer DEFAULT NULL UNIQUE',
                    'mac_add8b text DEFAULT NULL UNIQUE',
                    'mac_add integer DEFAULT NULL UNIQUE',
                    'voltage float',
                    'feb_index integer',
                    'ch0 integer',
                    'ch1 integer',
                    'ch2 integer',
                    'ch3 integer',
                    'ch4 integer',
                    'ch5 integer',
                    'ch6 integer',
                    'ch7 integer',
                    'ch8 integer',
                    'ch9 integer',
                    'ch10 integer',
                    'ch11 integer',
                    'ch12 integer',
                    'ch13 integer',
                    'ch14 integer',
                    'ch15 integer',
                    'ch16 integer',
                    'ch17 integer',
                    'ch18 integer',
                    'ch19 integer',
                    'ch20 integer',
                    'ch21 integer',
                    'ch22 integer',
                    'ch23 integer',
                    'ch24 integer',
                    'ch25 integer',
                    'ch26 integer',
                    'ch27 integer',
                    'ch28 integer',
                    'ch29 integer',
                    'ch30 integer',
                    'ch31 integer',
                    'create_time text',
                    'update_user text',
                    'update_time text',
                    'create_user text']

topcrtSiPMColumns = ['serial_num integer PRIMARY KEY NOT NULL',
                     'voltage float NOT NULL',
                     'dark_curr float NOT NULL',
                     'comment text',
                     'create_time text',
                     'update_user text',
                     'update_time text',
                     'create_user text']

topcrtSCINTILLATORColumns = ['scin_barcode text PRIMARY KEY NOT NULL',
                             'scin_prod text',
                             'scin_sipm_r integer DEFAULT NULL UNIQUE',
                             'scin_sipm_l integer DEFAULT NULL UNIQUE',
                             'scin_test text',
                             'light_y float',
                             'attenu_t float',
                             'attenu_l float',
                             'create_time text',
                             'update_user text',
                             'update_time text',
                             'create_user text']

topcrtMODULEColumns = ['crt_barcode text PRIMARY KEY NOT NULL',
                       'feb_barcode text DEFAULT NULL UNIQUE',
                       'scin1 text DEFAULT NULL UNIQUE',
                       'scin2 text DEFAULT NULL UNIQUE',
                       'scin3 text DEFAULT NULL UNIQUE',
                       'scin4 text DEFAULT NULL UNIQUE',
                       'scin5 text DEFAULT NULL UNIQUE',
                       'scin6 text DEFAULT NULL UNIQUE',
                       'scin7 text DEFAULT NULL UNIQUE',
                       'scin8 text DEFAULT NULL UNIQUE',
                       'scin9 text DEFAULT NULL UNIQUE',
                       'scin10 text DEFAULT NULL UNIQUE',
                       'scin11 text DEFAULT NULL UNIQUE',
                       'scin12 text DEFAULT NULL UNIQUE',
                       'scin13 text DEFAULT NULL UNIQUE',
                       'scin14 text DEFAULT NULL UNIQUE',
                       'scin15 text DEFAULT NULL UNIQUE',
                       'scin16 text DEFAULT NULL UNIQUE',
                       'test text',
                       'created_by text',
                       'produced text',
                       'test2 text',
                       'test3 text',
                       'test4 text',
                       'effic float',
                       'create_time text',
                       'update_user text',
                       'update_time text',
                       'create_user text']

topcrtCABLEColumns = ['cable_id text PRIMARY KEY NOT NULL',
                      'cable_type text',
                      'cable_function text',
                      'cable_length float',
                      'start_feb text',
                      'end_feb text',
                      'create_time text',
                      'update_user text',
                      'update_time text',
                      'create_user text']

topcrtPOWERColumns = ['cable_id text PRIMARY KEY NOT NULL',
                      'powerline int',
                      'plus_lines text',
                      'sensing text',
                      'feb1 text NULL UNIQUE',
                      'feb2 text NULL UNIQUE',
                      'create_time text',
                      'update_user text',
                      'update_time text',
                      'create_user text']

topcrtPOSITIONColumns = ['crt_barcode text PRIMARY KEY',
                         'wall text',
                         'row_index integer',
                         'column_index integer',
                         'pos_x float',
                         'pos_y float',
                         'pos_z float',
                         'create_time text',
                         'update_user text',
                         'update_time text',
                         'create_user text']

################################################################################

def setForeignKeys(table):

    keysString = ""
    if table == "crtmodule":
        keysString += ", FOREIGN KEY (feb_barcode) REFERENCES crtfeb(feb_barcode), "
        keysString += "FOREIGN KEY (scin1) REFERENCES scintillator(scin_barcode), "
        keysString += "FOREIGN KEY (scin2) REFERENCES scintillator(scin_barcode), "
        keysString += "FOREIGN KEY (scin3) REFERENCES scintillator(scin_barcode), "
        keysString += "FOREIGN KEY (scin4) REFERENCES scintillator(scin_barcode), "
        keysString += "FOREIGN KEY (scin5) REFERENCES scintillator(scin_barcode), "
        keysString += "FOREIGN KEY (scin6) REFERENCES scintillator(scin_barcode), "
        keysString += "FOREIGN KEY (scin7) REFERENCES scintillator(scin_barcode), "
        keysString += "FOREIGN KEY (scin8) REFERENCES scintillator(scin_barcode), "
        keysString += "FOREIGN KEY (scin9) REFERENCES scintillator(scin_barcode), "
        keysString += "FOREIGN KEY (scin10) REFERENCES scintillator(scin_barcode), "
        keysString += "FOREIGN KEY (scin11) REFERENCES scintillator(scin_barcode), "
        keysString += "FOREIGN KEY (scin12) REFERENCES scintillator(scin_barcode), "
        keysString += "FOREIGN KEY (scin13) REFERENCES scintillator(scin_barcode), "
        keysString += "FOREIGN KEY (scin14) REFERENCES scintillator(scin_barcode), "
        keysString += "FOREIGN KEY (scin15) REFERENCES scintillator(scin_barcode), "
        keysString += "FOREIGN KEY (scin16) REFERENCES scintillator(scin_barcode)"

    if table == "scintillator":
        keysString += ", FOREIGN KEY (scin_sipm_r) REFERENCES topcrt_sipm(serial_num), "
        keysString += "FOREIGN KEY (scin_sipm_l) REFERENCES topcrt_sipm(serial_num)"

    if table == "crtcable":
        keysString += ", FOREIGN KEY (start_feb) REFERENCES crtfeb(feb_barcode), "
        keysString += "FOREIGN KEY (end_feb) REFERENCES crtfeb(feb_barcode)"

    if table == "crtpower":
        keysString += ", FOREIGN KEY (feb1) REFERENCES crtfeb(feb_barcode), "
        keysString += "FOREIGN KEY (feb2) REFERENCES crtfeb(feb_barcode)"

    if table == "crtposition":
        keysString += ", FOREIGN KEY (crt_barcode) REFERENCES crtmodule(crt_barcode)"

    return keysString

# Define the function to create and fill each table
def copyTable(postGres, dbCurs, dbName, table, columns):
    createString = "CREATE TABLE " + table + " ("

    for columnIdx in range(len(columns)):
        if columnIdx > 0:
            createString += ", "
        createString += columns[columnIdx]

    createString += setForeignKeys(table)
    createString += ")"

    #print(createString)
    dbCurs.execute(createString)
    query = postGres.query(database=dbName, table=table, columns="*")

    lastIdx = len(query)-1;
    for rowIdx in range(len(query)):

        rowList = query[rowIdx].split(',')
        # exception for column "created_by" in table crtmodule
        # which shouldn't get splitted despite having commas in it
        if table == "crtmodule":
            # regex witchcraft from
            # https://stackoverflow.com/questions/43067373/split-by-comma-and-how-to-exclude-comma-from-quotes-in-split
            rowList = re.split(r",(?=(?:[^\"']*[\"'][^\"']*[\"'])*[^\"']*$)",query[rowIdx])

        if len(rowList) != len(columns):
            if rowIdx != lastIdx:
                print("Length mismatch! Will skip this row (",rowIdx,"/",lastIdx,")")
            continue

        insertString = "INSERT INTO " + table + " VALUES ("
        for idx in range(len(rowList)):
            if idx > 0:
                insertString += ", "
            if "text" in columns[idx]:
                fieldEntry = rowList[idx]
                # changing "None" into NULL is necessary for compliance
                # with UNIQUE statements in certain columns (eg: feb2 in crtpower)
                if fieldEntry == "None":
                    insertString += "NULL"
                else:
                    insertString += "\'" + fieldEntry + "\'"
            else:
                if "none" in rowList[idx]:
                    #print("Found none in column data, field:",columns[idx],", value: ",rowList[idx])
                    insertString += '0'
                else:
                    insertString += "\'" +rowList[idx]+ "\'"
        insertString += ")"
        dbCurs.execute(insertString)

##################################################################################
# Ok get down to extracting the info and copying to SQLite
queryurl = "https://dbdata0vm.fnal.gov:9443/QE/hw/app/SQ/query"
dataQuery = DataQuery(queryurl)

dbName             = "icarus_hardware_prd"
topcrtfebTable     = "crtfeb"
topcrtsipmTable    = "topcrt_sipm"
topcrtmoduleTable  = "crtmodule"
topcrtscintillatorTable = "scintillator"
topcrtcableTable = "crtcable"
topcrtpowerTable = "crtpower"
topcrtpositionTable = "crtposition"

##################################################################################

# Start by creating the new database and setting up the first table
today = str(datetime.date.today())
dump_name = "topCRT_hw_dump_" + today + ".db"

sqliteDB = sqlite3.connect(dump_name)
dbCurs   = sqliteDB.cursor()

###################################################################################

copyTable(dataQuery, dbCurs, dbName, topcrtfebTable, topcrtFEBColumns)
copyTable(dataQuery, dbCurs, dbName, topcrtsipmTable, topcrtSiPMColumns)
copyTable(dataQuery, dbCurs, dbName, topcrtmoduleTable, topcrtMODULEColumns)
copyTable(dataQuery, dbCurs, dbName, topcrtscintillatorTable, topcrtSCINTILLATORColumns)
copyTable(dataQuery, dbCurs, dbName, topcrtcableTable, topcrtCABLEColumns)
copyTable(dataQuery, dbCurs, dbName, topcrtpowerTable, topcrtPOWERColumns)
copyTable(dataQuery, dbCurs, dbName, topcrtpositionTable, topcrtPOSITIONColumns)

###################################################################################

sqliteDB.commit()
sqliteDB.close()
