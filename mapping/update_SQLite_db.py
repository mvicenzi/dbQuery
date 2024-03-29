import sqlite3
import csv
import datetime

### ----------------------------------------------
### CONFIG
user = "mvicenzi"
current_time = datetime.datetime.now()
tz_offset = "-06:00"
time = current_time.strftime('%m/%d/%Y %H:%M:%S.%f') + tz_offset

### ----------------------------------------------

def add_table_and_data_from_csv_to_existing_db(csv_file, db_path, new_table_name, source_table_name):
    # Connect to the existing SQLite database
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Get the data types of columns from the source table
    cursor.execute(f"PRAGMA table_info({source_table_name});")
    column_info = cursor.fetchall()

    # Create a new table in the existing database using the columns name and data types from the source table
    columns_sql_statements = []
    for info in column_info:
        constraint = ""
        if info[1] == "pmt_id":
            constraint = "PRIMARY KEY"
        if info[1] == "channel_id" or info[1] == "hv_cable_label" or info[1] == "signal_cable_label":
            constraint = "UNIQUE"
        columns_sql_statements.append(f'{info[1]} {info[2]} {constraint}')
    create_table_sql = f"CREATE TABLE {new_table_name} ({', '.join([f'{statement}' for statement in columns_sql_statements])});"
    cursor.execute(create_table_sql)

    # Read the CSV file and retrieve the header (column names)
    with open(csv_file, 'r') as csvfile:
        csv_reader = csv.reader(csvfile)
        header = next(csv_reader)
        # Insert data from the CSV file into the new table
        insert_sql = f"INSERT INTO {new_table_name} VALUES ({', '.join(['?']*len(column_info))});"
        for row in csv_reader:
	    #insert missing columns value into row
            row.insert(12,time) # create_time
            row.insert(13,'None') # update_user
            row.insert(14,'None') # update_time
            row.insert(15,user) # create_user
            cursor.execute(insert_sql, row)

    # Commit changes and close the connection
    conn.commit()
    conn.close()

### ---------------------------
### ---------------------------

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
        for column in columns:
            print(f"  Column: {column[1]} ({column[2]})")

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

csv_file = 'PMT_MAPPING_August2023.csv'
new_table_name = 'pmt_placements_Aug2023'
source_table_name = "pmt_placements"

add_table_and_data_from_csv_to_existing_db(csv_file, db_path, new_table_name, source_table_name)

print_table_info(db_path)
print_table_contents(db_path, new_table_name)
