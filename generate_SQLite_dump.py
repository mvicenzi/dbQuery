#! /usr/bin/env python
#---------------------------------------------------------------
# This script generates a SQLite dump of top CRT tables,
# using info from ICARUS hardware database.
#
# Author: mvicenzi
# Inspired by: copyPostGresToSQLite.py (by T. Usher)
#
#---------------------------------------------------------------

from __future__ import print_function
import sys, os, datetime
import psycopg2
import sqlite3
from   DataLoader3 import DataLoader, DataQuery

# Set up the columns for the tables
topcrtFEBColumns = ['feb_barcode text',
                    'serialnum integer',
                    'mac_add8b text',
                    'mac_add integer',
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

topcrtSiPMColumns = ['serial_num integer',
                     'voltage float',
                     'dark_curr float',
                     'comment text',
                     'create_time text',
                     'update_user text',
                     'update_time text',
                     'create_user text']

topcrtSCINTILLATORColumns = ['scin_barcode text',
                             'scin_prod text',
                             'scin_sipm_r integer',
                             'scin_sipm_l integer',
                             'scin_test text',
                             'light_y float',
                             'attenu_t float',
                             'attenu_l float',
                             'create_time text',
                             'update_user text',
                             'update_time text',
                             'create_user text']

topcrtMODULEColumns = ['crt_barcode text',
                       'feb_barcode text',
                       'scin1 text',
                       'scin2 text',
                       'scin3 text',
                       'scin4 text',
                       'scin5 text',
                       'scin6 text',
                       'scin7 text',
                       'scin8 text',
                       'scin9 text',
                       'scin10 text',
                       'scin11 text',
                       'scin12 text',
                       'scin13 text',
                       'scin14 text',
                       'scin15 text',
                       'scin16 text',
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

topcrtCABLEColumns = ['cable_id text',
                      'cable_type text',
                      'cable_function text',
                      'cable_length float',
                      'start_feb text',
                      'end_feb text',
                      'create_time text',
                      'update_user text',
                      'update_time text',
                      'create_user text']

topcrtPOWERColumns = ['cable_id text',
                      'powerline int',
                      'plus_lines text',
                      'sensing text',
                      'feb1 text',
                      'feb2 text',
                      'create_time text',
                      'update_user text',
                      'update_time text',
                      'create_user text']

topcrtPOSITIONColumns = ['crt_barcode text',
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

# Define the function to create and fill each table
def copyTable(postGres, dbCurs, dbName, table, columns):
    createString = "CREATE TABLE " + table + " ("

    for columnIdx in range(len(columns)):
        if columnIdx > 0:
            createString += ", "
        createString += columns[columnIdx]

    createString += ")"

    print(createString)
    dbCurs.execute(createString)

    query = postGres.query(database=dbName, table=table, columns="*")

    lastIdx = len(query)-1;
    for rowIdx in range(len(query)):

        ### FIX THIS----> for module table, "created_by" colums has strings
        ### with commas in them for multiple people (eg: Francesco,Valerio)
        ### ---> this gets splitted and it shouldn't
        rowList = query[rowIdx].split(',')
        
        if len(rowList) != len(columns):
            print(rowList)
            print("Length mismatch! Will skip this row (",rowIdx,"/",lastIdx,")")
            continue

        insertString = "INSERT INTO " + table + " VALUES ("
        for idx in range(len(rowList)):
            if idx > 0:
                insertString += ", "
            if "text" in columns[idx]:
                fieldEntry = rowList[idx]
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
sqliteDB = sqlite3.connect("test.db")
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
