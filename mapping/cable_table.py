import sqlite3
import csv
import datetime

#### ---------------------------------------
#### ---------------------------------------

def print_table_contents(db_path, table_name, csv_file):
    # Connect to the SQLite database
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    f = open(csv_file,"w")

    # Retrieve all rows from the specified table
    column_name = "signal_cable_label"   
    cursor.execute(f"SELECT {column_name} FROM {table_name};")
    cables = cursor.fetchall()

    writer = csv.writer(f, quoting=csv.QUOTE_MINIMAL)
    header = "cable_label,cable_type,cable_length"
    writer.writerow(header.split(","))

    # Print the table contents
    for cable in cables:
        row = [ cable[0], "Signal", "28.0" ]
        writer.writerow(row)

    # Close the connection
    conn.close()
    f.close()

#### --------------------------------------
#### --------------------------------------

db_path = 'ChannelMapICARUS_August2023.db'
csv_file = 'new_cables_August2023.csv'
table_name = 'pmt_placements_Aug2023'

print_table_contents(db_path, table_name, csv_file)
