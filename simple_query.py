from DataLoader3 import DataQuery

queryUrl = "https://dbdata0vm.fnal.gov:9443/QE/hw/app/SQ/query"
 #queryUrl = "https://dbdata0vm.fnal.gov:9090/hw/app/SQ/query"

dBname = "icarus_hardware_prd"
table = "crtmodule"

print("\nQuery URL: %s" % queryUrl)
print("\n\nDataQuery Example:")
print(" select * from crtmodule where crt_barcode = 'CRT00125'")

dataQuery = DataQuery(queryUrl)

"""
     database - The name of the database to be queried.  (This database must
                be in QueryEngine's configuration file.)
     table - The name of the table to query on.
     columns - A comma separated string of the table columns to be returned.
     where - (optional) <column>:<op>:<value> - can be repeated; seperated by ampersand (&)
             op can be: lt, le, eq, ne, ge, gt
     order - (optional) A comma separated string of columns designating row order in the returned list.
             Start the string with a minus (-) for descending order.
     limit - (optional) - A integer designating the maximum number of rows to be returned.
"""
rows = dataQuery.query(database=dBname, table=table, columns="*",where='crt_barcode:eq:CRT00125', echoUrl=True)

print('\n%s returned\n' % len(rows))
for row in rows:
    print(row)
