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
### ----------------------------------------------

def table_exists(table_name, cursor):
    cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name=?;", (table_name,))
    return cursor.fetchone()

### ----------------------------------------------
### ----------------------------------------------

def replace_table(db_path, table_name, csv_file):

    # Connect to the existing SQLite database
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    if table_exists(table_name,cursor) is not None:
        delete_table(db_path,table_name)
        print("Deleting previous table...")

    # Read the CSV file and retrieve the header (column names)
    with open(csv_file, 'r') as csvfile:
        csv_reader = csv.reader(csvfile)
        header = next(csv_reader)
        types = ['INTEGER', 'TEXT', 'TEXT', 'INTEGER', 'INTEGER', 'TEXT', 'TEXT','TEXT','TEXT', 'TEXT', 'INTEGER', 'TEXT', 'TEXT', 'TEXT', 'INTEGER', 'TEXT', 'INTEGER', 'INTEGER']
        
        columns_sql_statements = []
        for i, column in enumerate(header):
            # skip column "new_signal_cable_label": we will use this values to fill the "signal_cable_label" column instead
            if column == "new_signal_cable_label":
                continue
        
            constraint = ""
            if column == "pmt_id":
                constraint = "PRIMARY KEY"
            if column == "channel_id" or column == "hv_cable_label" or column == "signal_cable_label":
                constraint = "UNIQUE"
            columns_sql_statements.append(f'{column} {types[i]} {constraint}')
    
        missing_columns= ["create_time TEXT", "update_user TEXT", "update_time TEXT", "create_user TEXT"]
        columns_sql_statements += missing_columns
        create_table_sql = f"CREATE TABLE {table_name} ({', '.join([f'{statement}' for statement in columns_sql_statements])});"
        cursor.execute(create_table_sql)

        # Insert data from the CSV file into the new table
        insert_sql = f"INSERT INTO {table_name} VALUES ({', '.join(['?']*len(columns_sql_statements))});"
	
        # need to skip the "signal_cable_label" column and use the data from "new_signal_cable_label" instead 
        # the two are one after the other, so it's easy to just skip one.
        skip_index = header.index('signal_cable_label')
        
        for row in csv_reader:
            nrow = row[:skip_index] + row[skip_index + 1:]

	    # insert missing columns value into row
            n = len(nrow)
            nrow.insert(n,time) # create_time
            nrow.insert(n+1,'None') # update_user
            nrow.insert(n+2,'None') # update_time
            nrow.insert(n+3,user) # create_user

            #print(row, nrow)
            cursor.execute(insert_sql, nrow)

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

def delete_table(db_path, table_name):
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Execute the DROP TABLE statement
    cursor.execute(f'DROP TABLE {table_name};')

    # Commit the changes and close the connection
    conn.commit()
    conn.close()

#### --------------------------------------
#### --------------------------------------

db_path = 'ChannelMapICARUS_test.db'
print_table_info(db_path)

csv_file = 'PMT_MAPPING-MAPPING_JAN052022.csv'
table_name = 'pmt_placements'
replace_table(db_path, table_name, csv_file)
csv_file = 'PMT_MAPPING-Mapping_August_2023.csv'
table_name = 'pmt_placements_23Aug2023'
replace_table(db_path, table_name, csv_file)
csv_file = 'PMT_MAPPING-MAPPING_29AUG2023.csv'
table_name = 'pmt_placements_29Aug2023'
replace_table(db_path, table_name, csv_file)


print_table_info(db_path)
print_table_contents(db_path, table_name)
