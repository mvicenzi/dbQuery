import sqlite3
import csv
import datetime


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

#### ---------------------------------------
#### ---------------------------------------

def print_table_contents(db_path, table_name):
    # Connect to the SQLite database
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Retrieve all rows from the specified table
    cursor.execute(f"SELECT * FROM {table_name};")
    rows = cursor.fetchall()

    # Print the column headers
    cursor.execute(f"PRAGMA table_info({table_name});")
    columns = cursor.fetchall()
    column_names = [column[1] for column in columns]
    print(', '.join(column_names))

    # Print the table contents
    for row in rows:
        print(', '.join(map(str, row)))

    # Close the connection
    conn.close()

#### --------------------------------------
#### --------------------------------------

db_path = 'ChannelMapICARUS_August2023.db'
print_table_info(db_path)

old_table_name = 'pmt_placements'
print_table_contents(db_path, old_table_name)

new_table_name = 'pmt_placements_Aug2023'
print_table_contents(db_path, new_table_name)
