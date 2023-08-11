#!/usr/bin/env python3

import sqlite3
import csv
import datetime
import sys


def print_table_info(database_path):
    # Connect to the SQLite database
    conn = sqlite3.connect(database_path)
    cursor = conn.cursor()

    # Get the list of table names in the database
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    table_names = cursor.fetchall()

    # Iterate through each table and print its name and columns
    for table in table_names:
        table_name = table[0]
        print(f"Table: {table_name}")
        
        # Get the columns of the current table
        cursor.execute(f"PRAGMA table_info({table_name});")
        columns = cursor.fetchall()
        
        # Print the columns
        for i, column in enumerate(columns):
            print(f"  Column {i}: {column[1]} ({column[2]})")

    # Close the database connection
    conn.close()
    return 0

#### ---------------------------------------
#### ---------------------------------------

def print_table_contents(db_path, table_name):
    # Connect to the SQLite database
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Retrieve all rows from the specified table
    cursor.execute(f"SELECT * FROM {table_name} ORDER BY pmt_id;")
    rows = cursor.fetchall()

    # Print the column headers
    cursor.execute(f"PRAGMA table_info({table_name});")
    columns = cursor.fetchall()
    column_names = [column[1] for column in columns]
    print(', '.join(column_names))

    if len(rows) == 0:
        print("No data.")
        return
    
    padding = [ 0 ] * len(rows[0])
    for row in rows:
        for iCol, value in enumerate(row):
            padding[iCol] = max(padding[iCol], len(str(value)))
    
    uniqueColumns = (
        'pmt_id', 'pmt_sn', 'hv_cable_label', 'signal_cable_label',
        'channel_id',
        )
    uniqueColumnIndices = list(map(column_names.index, uniqueColumns))
    dupCheckValues = [ dict() for i in range(len(uniqueColumns)) ]
    print(f"{uniqueColumnIndices=}")

    nDups = 0
    # Print the table contents; also check for duplicates
    for iRow, row in enumerate(rows):
        print(', '.join(map(lambda p: f"{str(p[1]):>{padding[p[0]]}s}", enumerate(row))))
        for iCheck, iCol in enumerate(uniqueColumnIndices):
            if row[iCol] in dupCheckValues[iCheck]:
                print(f"{column_names[iCol]}='{row[iCol]}' on row {iRow}"
                      f" already found on row {dupCheckValues[iCheck][row[iCol]]}",
                      file=sys.stderr)
                nDups += 1
            else: dupCheckValues[iCheck][row[iCol]] = iRow
        # for values
    # for rows
    
    # Close the connection
    conn.close()
    
    if nDups > 0:
        print(f"Found {nDups} duplicate elements!", file=sys.stderr)
        return 1
    
    return 0

#### --------------------------------------
#### --------------------------------------

nErrors = 0

db_path = 'ChannelMapICARUS_August2023.db'
nErrors += print_table_info(db_path)

old_table_name = 'pmt_placements'
nErrors += print_table_contents(db_path, old_table_name)

new_table_name = 'pmt_placements_Aug2023'
nErrors += print_table_contents(db_path, new_table_name)

if nErrors > 0:
    raise RuntimeError(f"{nErrors} checks failed!!")
